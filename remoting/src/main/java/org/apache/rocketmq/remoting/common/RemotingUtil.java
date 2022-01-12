package org.apache.rocketmq.remoting.common;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Enumeration;

public class RemotingUtil {

    public static final String OS_NAME = System.getProperty("os.name");

    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private static boolean isLinuxPlatform = false;

    private static boolean isWindowsPlatform = false;

    static {
        if (OS_NAME != null && OS_NAME.toLowerCase().contains("linux")) {
            isLinuxPlatform = true;
        }

        if (OS_NAME != null && OS_NAME.toLowerCase().contains("windows")) {
            isWindowsPlatform = true;
        }
    }

    public static boolean isWindowsPlatform() {
        return isWindowsPlatform;
    }

    public static Selector openSelector() throws IOException {
        Selector result = null;

        if (isLinuxPlatform()) {
            try {
                final Class<?> providerClazz = Class.forName("sun.nio.ch.EPollSelectorProvider");
                if (providerClazz != null) {
                    try {
                        final Method method = providerClazz.getMethod("provider");
                        if (method != null) {
                            final SelectorProvider selectorProvider = (SelectorProvider) method.invoke(null);
                            if (selectorProvider != null) {
                                result = selectorProvider.openSelector();
                            }
                        }
                    } catch (final Exception e) {
                        log.warn("Open ePoll Selector for linux platform exception", e);
                    }
                }
            } catch (final Exception e) {
                // ignore
            }
        }

        if (result == null) {
            result = Selector.open();
        }

        return result;
    }

    public static boolean isLinuxPlatform() {
        return isLinuxPlatform;
    }

    public static String getLocalAddress() {
        try {
            // Traversal Network interface to get the first non-loopback and non-private address
            Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
            ArrayList<String> ipv4Result = new ArrayList<String>();
            ArrayList<String> ipv6Result = new ArrayList<String>();
            while (enumeration.hasMoreElements()) {
                final NetworkInterface networkInterface = enumeration.nextElement();
                final Enumeration<InetAddress> en = networkInterface.getInetAddresses();
                while (en.hasMoreElements()) {
                    final InetAddress address = en.nextElement();

                    boolean loopbackAddress = address.isLoopbackAddress();
                    if (!loopbackAddress) {

                        String hostAddress = normalizeHostAddress(address);

                        if (address instanceof Inet6Address) {
                            ipv6Result.add(hostAddress);
                        } else {
                            ipv4Result.add(hostAddress);
                        }
                    }
                }
            }

            // prefer ipv4，ipv4优先
            if (!ipv4Result.isEmpty()) {
                for (String ip : ipv4Result) {
                    //以这些开头的一般都是局域网之内的IP地址，区别于外网IP
                    if (ip.startsWith("127.0") || ip.startsWith("192.168")) {
                        continue;
                    }

                    return ip;
                }

                //TODO 直接返回最后一个？
                return ipv4Result.get(ipv4Result.size() - 1);

            } else if (!ipv6Result.isEmpty()) {
                //如果 ipv4 为空，ipv6有，则取第一个 ipv6
                return ipv6Result.get(0);
            }
            //If failed to find,fall back to localhost
            //如果 ipv4,ipv6 都么有，则使用 localhost
            final InetAddress localHost = InetAddress.getLocalHost();
            return normalizeHostAddress(localHost);
        } catch (Exception e) {
            log.error("Failed to obtain local address", e);
        }

        return null;
    }

    public static String normalizeHostAddress(final InetAddress localHost) {
        if (localHost instanceof Inet6Address) {
            return "[" + localHost.getHostAddress() + "]";
        } else {
            return localHost.getHostAddress();
        }
    }

    public static SocketAddress string2SocketAddress(final String addr) {
        int split = addr.lastIndexOf(":");
        String host = addr.substring(0, split);
        String port = addr.substring(split + 1);
        return new InetSocketAddress(host, Integer.parseInt(port));
    }

    public static String socketAddress2String(final SocketAddress addr) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) addr;
        return inetSocketAddress.getAddress().getHostAddress()
                + ":"
                + inetSocketAddress.getPort();
    }

    public static SocketChannel connect(SocketAddress remote) {
        return connect(remote, 1000 * 5);
    }

    public static SocketChannel connect(SocketAddress remote, final int timeoutMillis) {
        SocketChannel sc = null;
        try {
            sc = SocketChannel.open();
            sc.configureBlocking(true);

            Socket socket = sc.socket();

            socket.setSoLinger(false, -1);
            socket.setTcpNoDelay(true);
            socket.setReceiveBufferSize(1024 * 64);
            socket.setSendBufferSize(1024 * 64);
            socket.connect(remote, timeoutMillis);
            // 非阻塞
            sc.configureBlocking(false);
            return sc;
        } catch (Exception e) {
            if (sc != null) {
                try {
                    sc.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

        return null;
    }

    public static void closeChannel(Channel channel) {
        final String addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
        //channel.close() = ChannelFuture
        //just log
        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                boolean futureSuccess = future.isSuccess();
                log.info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote, futureSuccess);
            }
        });
    }

}
