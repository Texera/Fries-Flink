package org.apache.flink.streaming.util.recovery;

import com.twitter.chill.akka.AkkaSerializer;

public class SerializeUtils {
    private static AkkaSerializer serializer;

    public static AkkaSerializer getSerializer() {
        if(serializer == null) {
            serializer = new AkkaSerializer(null);
        }
        return serializer;
    }

}
