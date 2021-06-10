package org.apache.flink.streaming.util.recovery;

import com.twitter.chill.akka.AkkaSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

public class RecoveryUtils {
    // static variable single_instance of type Singleton
    private static RecoveryUtils single_instance = null;

    public FileSystem hdfs;
    public AkkaSerializer serializer;

    // private constructor restricted to this class itself
    private RecoveryUtils() {
        String hostAddress = "hdfs://10.128.0.2:8020/";
        Configuration hdfsConf = new Configuration();
        hdfsConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "false");
        try {
            hdfs = FileSystem.get(new URI(hostAddress), hdfsConf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        serializer = new AkkaSerializer(null);
    }

    // static method to create instance of Singleton class
    public static RecoveryUtils getInstance() {
        if (single_instance == null)
            single_instance = new RecoveryUtils();
        return single_instance;
    }
}
