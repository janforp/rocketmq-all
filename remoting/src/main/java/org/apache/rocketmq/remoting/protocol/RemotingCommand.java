package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyDecoder;
import org.apache.rocketmq.remoting.netty.NettyEncoder;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.ResponseFuture;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 远程命令，远程请求/响应对象
 */
@ToString
public class RemotingCommand {

    public static final String SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type";

    public static final String SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE";

    public static final String REMOTING_VERSION_KEY = "rocketmq.remoting.version";

    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private static final int RPC_TYPE = 0; // 0, REQUEST_COMMAND

    private static final int RPC_ONEWAY = 1; // 0, RPC

    /**
     * @see RemotingCommand#getClazzFields(java.lang.Class)
     */
    private static final Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP = new HashMap<Class<? extends CommandCustomHeader>, Field[]>();

    /**
     * @see RemotingCommand#getCanonicalName(java.lang.Class) key:class,value:名称
     */
    private static final Map<Class<?>, String> CANONICAL_NAME_CACHE = new HashMap<Class<?>, String>();

    /**
     * @see RemotingCommand#isFieldNullable(java.lang.reflect.Field)
     */
    // 1, Oneway
    // 1, RESPONSE_COMMAND
    private static final Map<Field, Boolean> NULLABLE_FIELD_CACHE = new HashMap<Field, Boolean>();

    // java.lang.String
    private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();

    // java.lang.Double
    private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();

    // double
    private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();

    // java.lang.Integer
    private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();

    // int
    private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();

    // java.lang.Long
    private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();

    // long
    private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();

    // java.lang.Boolean
    private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();

    // boolean
    private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();

    private static volatile int configVersion = -1;

    /**
     * @see RemotingCommand#opaque
     */
    private static final AtomicInteger requestId = new AtomicInteger(0);

    private static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;

    static {
        //rocketmq.serialize.type，ROCKETMQ_SERIALIZE_TYPE
        final String protocol = System.getProperty(SERIALIZE_TYPE_PROPERTY/* rocketmq.serialize.type */, System.getenv(SERIALIZE_TYPE_ENV /* ROCKETMQ_SERIALIZE_TYPE */));
        if (!isBlank(protocol)) {
            try {
                serializeTypeConfigInThisServer = SerializeType.valueOf(protocol);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("parser specified protocol error. protocol=" + protocol, e);
            }
        }
    }

    @Setter
    @Getter
    private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;

    /**
     * 该请求的类型
     * request:请求操作码，应答方根据不同的请求码进行不同的业务处理
     * response:应答响应码。0表示成功，非0则表示各种错误
     */
    @Setter
    @Getter
    private int code;

    @Setter
    @Getter
    private LanguageCode language = LanguageCode.JAVA;

    @Setter
    @Getter
    private int version = 0;

    /**
     * request:相当于requestId，在同一个连接上的不同请求标识码，与响应消息中的相对应
     * response:应答不做修改直接返回
     *
     * 同一个 JVM 下是唯一的
     *
     * @see ResponseFuture#opaque 跟这个字段一一对应
     */
    @Setter
    @Getter
    private int opaque = requestId.getAndIncrement();

    /**
     * TODO 标记是响应还是请求？？
     * 区分是普通RPC还是onewayRPC得标志，服务器会根据该标记决定是否返回数据
     *
     * @see NettyRemotingAbstract#invokeOnewayImpl(io.netty.channel.Channel, org.apache.rocketmq.remoting.protocol.RemotingCommand, long)
     */
    @Setter
    @Getter
    private int flag = 0;

    // 传输自定义文本信息，一般失败的时候传失败原因
    @Setter
    @Getter
    private String remark;

    /**
     * 字段的名称跟值的映射表
     *
     * 请求自定义扩展信息 或者 响应自定义扩展信息！！！！！！！
     */
    @Setter
    @Getter
    private HashMap<String/*customHeader 的字段名称*/, String/*customHeader 的字段 值*/> extFields;

    // 不参与序列化，通过extFields传递
    private transient CommandCustomHeader customHeader;

    // 不参与序列化
    @Setter
    @Getter
    private transient byte[] body;

    protected RemotingCommand() {
    }

    public static RemotingCommand createRequestCommand(int code/*业务类型*/, CommandCustomHeader customHeader/*有具体请求参数的对象*/) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        // 请求
        cmd.customHeader = customHeader;
        setCmdVersion(cmd);
        return cmd;
    }

    private static void setCmdVersion(RemotingCommand cmd) {
        if (configVersion >= 0) {
            cmd.setVersion(configVersion);
        } else {
            //rocketmq.remoting.version
            String v = System.getProperty(REMOTING_VERSION_KEY /* rocketmq.remoting.version */);
            if (v != null) {
                int value = Integer.parseInt(v);
                cmd.setVersion(value);
                configVersion = value;
            }
        }
    }

    public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
        return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR /* 1 */, "not set any response code", classHeader);
    }

    public static RemotingCommand createResponseCommand(int code, String remark, Class<? extends CommandCustomHeader> classHeader) {
        RemotingCommand cmd = new RemotingCommand();
        // 标志这是一个响应类型的请求
        cmd.markResponseType();
        // 业务代码
        cmd.setCode(code);
        cmd.setRemark(remark);
        setCmdVersion(cmd);

        if (classHeader != null) {
            try {
                // 反射创建用户自定义 header 对象
                cmd.customHeader = classHeader.newInstance();
            } catch (InstantiationException e) {
                return null;
            } catch (IllegalAccessException e) {
                return null;
            }
        }
        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {
        return createResponseCommand(code, remark, null);
    }

    public static RemotingCommand decode(final byte[] array) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(array);
        return decode(byteBuffer);
    }

    /**
     * 解码
     *
     * @param byteBuffer 收到的数据
     * @return 解码之后的对象
     * @see NettyDecoder#decode(io.netty.channel.ChannelHandlerContext, io.netty.buffer.ByteBuf)
     */
    public static RemotingCommand decode(final ByteBuffer byteBuffer) {
        int length = byteBuffer.limit();
        // 读取头
        int oriHeaderLen = byteBuffer.getInt();
        int headerLength = getHeaderLength(oriHeaderLen);

        byte[] headerData = new byte[headerLength];
        byteBuffer.get(headerData);

        SerializeType protocolType = getProtocolType(oriHeaderLen);
        RemotingCommand cmd = headerDecode(headerData, protocolType);

        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            // 读取 body 的内容
            byteBuffer.get(bodyData);
        }
        assert cmd != null;
        cmd.body = bodyData;

        return cmd;
    }

    public static int getHeaderLength(int length) {
        // TODO ????
        return length & 0xFFFFFF;
    }

    private static RemotingCommand headerDecode(byte[] headerData, SerializeType type) {
        switch (type) {
            case JSON:
                RemotingCommand resultJson = RemotingSerializable.decode(headerData, RemotingCommand.class);
                resultJson.setSerializeTypeCurrentRPC(type);
                return resultJson;
            case ROCKETMQ:
                RemotingCommand resultRMQ = RocketMQSerializable.rocketMQProtocolDecode(headerData);
                resultRMQ.setSerializeTypeCurrentRPC(type);
                return resultRMQ;
            default:
                break;
        }

        return null;
    }

    public static SerializeType getProtocolType(int source) {
        byte byteVal = (byte) ((source >> 24) & 0xFF);
        return SerializeType.valueOf(byteVal);
    }

    public static int createNewRequestId() {
        return requestId.getAndIncrement();
    }

    public static SerializeType getSerializeTypeConfigInThisServer() {
        return serializeTypeConfigInThisServer;
    }

    private static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static byte[] markProtocolType(int source, SerializeType type) {
        byte[] result = new byte[4];

        result[0] = type.getCode();
        result[1] = (byte) ((source >> 16) & 0xFF);
        result[2] = (byte) ((source >> 8) & 0xFF);
        result[3] = (byte) (source & 0xFF);
        return result;
    }

    public void markResponseType() {
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }

    public CommandCustomHeader readCustomHeader() {
        return customHeader;
    }

    @SuppressWarnings("unused")
    public void writeCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;

    }

    /**
     * 把该对象中的 {@link org.apache.rocketmq.remoting.protocol.RemotingCommand#extFields} 数据解码，得到一个 classHeader 类型的对象
     *
     * @param classHeader 解码之后得到的类型
     * @return 把该对象中的数据解码，得到一个 classHeader 类型的对象
     * @see org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor#registerBroker(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)
     */
    public CommandCustomHeader decodeCommandCustomHeader(Class<? extends CommandCustomHeader> classHeader) throws RemotingCommandException {
        CommandCustomHeader objectHeader;
        try {
            // 构造器
            objectHeader = classHeader.newInstance();
        } catch (InstantiationException e) {
            return null;
        } catch (IllegalAccessException e) {
            return null;
        }
        if (this.extFields == null) {
            return objectHeader;
        }

        // 该CommandCustomHeader的字段集合
        Field[] fields = getClazzFields(classHeader);
        for (Field field : fields) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            // 非静态字段
            String fieldName = field.getName();
            if (fieldName.startsWith("this")) {
                // TODO 什么字段会以 this 开头呢？？？？
                System.out.println("this开头的字段" + fieldName);
                continue;
            }
            // 字段名称不以this开头
            try {

                // 拿到该字段的值
                String value = this.extFields.get(fieldName);
                if (null == value) {
                    if (!isFieldNullable(field)) {
                        // 如果非空字段为空则抛异常
                        throw new RemotingCommandException("the custom field <" + fieldName + "> is null");
                    }
                    continue;
                }

                // 校验通过，下面赋值

                field.setAccessible(true);
                // class名称
                String type = getCanonicalName(field.getType());

                // 解析出来的值
                Object valueParsed;

                if (type.equals(STRING_CANONICAL_NAME)) {
                    // string
                    valueParsed = value;
                } else if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
                    // int 或者 integer
                    valueParsed = Integer.parseInt(value);
                } else if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
                    // long 或者 Long
                    valueParsed = Long.parseLong(value);
                } else if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
                    // Boolean 或者 boolean
                    valueParsed = Boolean.parseBoolean(value);
                } else if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
                    // double 或者 Double
                    valueParsed = Double.parseDouble(value);
                } else {
                    throw new RemotingCommandException("the custom field <" + fieldName + "> type is not supported");
                }

                // 设置值
                field.set(objectHeader, valueParsed);
            } catch (Throwable e) {
                log.error("Failed field [{}] decoding", fieldName, e);
            }
        }
        // 校验字段,定义者自己实现
        objectHeader.checkFields();

        return objectHeader;
    }

    private Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
        // 缓存
        Field[] field = CLASS_HASH_MAP.get(classHeader);
        if (field == null) {
            field = classHeader.getDeclaredFields();
            synchronized (CLASS_HASH_MAP) {
                // 同步
                CLASS_HASH_MAP.put(classHeader, field);
            }
        }
        return field;
    }

    /**
     * 该字段是否可以为null
     *
     * @param field 字段
     * @return 该字段是否可以为null
     */
    private boolean isFieldNullable(Field field) {
        if (!NULLABLE_FIELD_CACHE.containsKey(field)) {
            Annotation annotation = field.getAnnotation(CFNotNull.class);
            synchronized (NULLABLE_FIELD_CACHE) {
                NULLABLE_FIELD_CACHE.put(field, annotation == null);
            }
        }
        return NULLABLE_FIELD_CACHE.get(field);
    }

    private String getCanonicalName(Class<?> clazz) {
        String name = CANONICAL_NAME_CACHE.get(clazz);
        if (name == null) {
            name = clazz.getCanonicalName();
            synchronized (CANONICAL_NAME_CACHE) {
                CANONICAL_NAME_CACHE.put(clazz, name);
            }
        }
        return name;
    }

    public ByteBuffer encode() {
        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData = this.headerEncode();
        length = length + headerData.length;

        // 3> body data length
        if (this.body != null) {
            length += body.length;
        }

        ByteBuffer result = ByteBuffer.allocate(4 + length);

        // length
        result.putInt(length);

        // header length
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        result.put(headerData);

        // body data;
        if (this.body != null) {
            result.put(this.body);
        }

        result.flip();

        return result;
    }

    private byte[] headerEncode() {
        this.makeCustomHeaderToNet();
        if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
            return RocketMQSerializable.rocketMQProtocolEncode(this);
        } else {

            // 一般是这个
            return RemotingSerializable.encode(this);
        }
    }

    /**
     * 自定义头到网络
     * 把  {@link RemotingCommand#customHeader} 转换到 {@link RemotingCommand#extFields}
     */
    public void makeCustomHeaderToNet() {
        if (this.customHeader == null) {
            return;
        }
        Field[] fields = getClazzFields(customHeader.getClass());
        if (null == this.extFields) {
            this.extFields = new HashMap<String, String>();
        }

        // 遍历所有字段
        for (Field field : fields) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            String name = field.getName();
            if (name.startsWith("this")) {
                continue;
            }

            // 正常逻辑在下面

            Object value = null;
            try {
                field.setAccessible(true);
                // 从该对象中拿到该字段的值
                value = field.get(this.customHeader);
            } catch (Exception e) {
                log.error("Failed to access field [{}]", name, e);
            }

            if (value != null) {
                this.extFields.put(name, value.toString());
            }
        }
    }

    /**
     * @return 编码之后的字节数组
     * @see NettyEncoder#encode(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand, io.netty.buffer.ByteBuf)
     */
    public ByteBuffer encodeHeader() {

        int bodyLen = this.body != null ? this.body.length : 0;

        return encodeHeader(bodyLen);
    }

    public ByteBuffer encodeHeader(final int bodyLength) {
        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData;
        headerData = this.headerEncode();

        length += headerData.length;

        // 3> body data length
        length += bodyLength;

        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        // length
        result.putInt(length);

        // header length
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        result.put(headerData);

        result.flip();

        return result;
    }

    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }

    @JSONField(serialize = false)
    @SuppressWarnings("all")
    public boolean isOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        return (this.flag & bits) == bits;
    }

    @JSONField(serialize = false)
    public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }

        return RemotingCommandType.REQUEST_COMMAND;
    }

    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }

    public void addExtField(String key, String value) {
        if (null == extFields) {
            extFields = new HashMap<String, String>();
        }
        extFields.put(key, value);
    }
}