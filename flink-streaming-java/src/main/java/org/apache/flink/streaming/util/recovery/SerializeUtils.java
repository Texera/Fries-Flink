package org.apache.flink.streaming.util.recovery;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.chill.IKryoRegistrar;
import com.twitter.chill.KryoInstantiator;
import com.twitter.chill.KryoPool;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.controller.ControlMessage;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.AbstractID;

import java.util.HashMap;
import java.util.HashSet;

public class SerializeUtils {
    private static KryoPool serializer;

    public static KryoPool getSerializer() {
        if(serializer == null) {
            KryoInstantiator init = new KryoInstantiator().withRegistrar(new IKryoRegistrar() {
                @Override
                public void apply(Kryo kryo) {
                    kryo.register(ControlMessage.class);
                    kryo.register(HashMap.class);
                    kryo.register(InputChannelInfo.class);
                    kryo.register(ExecutionAttemptID.class);
                    kryo.register(IntermediateResultPartitionID.class);
                    kryo.register(IntermediateDataSetID.class);
                    kryo.register(AbstractID.class);
                }
            });
            serializer = KryoPool.withByteArrayOutputStream(4* Runtime.getRuntime().availableProcessors(), init);
        }
        return serializer;
    }

}
