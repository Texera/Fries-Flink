package org.apache.flink.streaming.util.recovery;

import com.esotericsoftware.kryo.Kryo;
import com.twitter.chill.IKryoRegistrar;
import com.twitter.chill.KryoInstantiator;
import com.twitter.chill.KryoPool;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.recovery.AbstractLogStorage;
import org.apache.flink.util.AbstractID;

public class SerializeUtils {
    private static KryoPool serializer;

    public static KryoPool getSerializer() {
        if(serializer == null) {
            KryoInstantiator init = new KryoInstantiator().withRegistrar(new IKryoRegistrar() {
                @Override
                public void apply(Kryo kryo) {
                    kryo.register(AbstractLogStorage.ControlRecord.class);
                    kryo.register(AbstractLogStorage.DPCursor.class);
                    kryo.register(AbstractLogStorage.UpdateStepCursor.class);
                    kryo.register(AbstractLogStorage.ChannelOrder.class);
                    kryo.register(InputChannelInfo.class);
                    kryo.register(ExecutionAttemptID.class);
                    kryo.register(IntermediateResultPartitionID.class);
                    kryo.register(IntermediateDataSetID.class);
                    kryo.register(AbstractID.class);
                    kryo.register(AbstractLogStorage.TimerStart.class);
                    kryo.register(AbstractLogStorage.WindowStart.class);
                }
            });
            serializer = KryoPool.withByteArrayOutputStream(4* Runtime.getRuntime().availableProcessors(), init);
        }
        return serializer;
    }

}
