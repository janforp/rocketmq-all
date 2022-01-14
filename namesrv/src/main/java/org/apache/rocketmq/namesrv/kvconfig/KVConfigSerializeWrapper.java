package org.apache.rocketmq.namesrv.kvconfig;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;

@Getter
@Setter
public class KVConfigSerializeWrapper extends RemotingSerializable {

    private HashMap<String/* Namespace */, HashMap<String/* Key */, String/* Value */>> configTable;
}