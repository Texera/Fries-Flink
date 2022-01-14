package org.apache.flink.streaming.tests;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */




import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.function.Consumer;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;

public class JoinWithStaticExample {


    public static void busySleep(long nanos)
    {
        long elapsed;
        final long startTime = System.nanoTime();
        do {
            elapsed = System.nanoTime() - startTime;
        } while (elapsed < nanos);
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(new String[] {
                "--classloader.check-leaked-classloader","false",
                "--state_backend.checkpoint_directory", "file:///home/shengqun97/",
                "--test.simulate_failure", "false",
                "--test.simulate_failure.max_failures", String.valueOf(1),
                "--test.simulate_failure.num_records", "100",
                "--print-level", "0",
//                "--hdfs-log-storage","hdfs://10.128.0.5:8020/",
                "--enable-logging","false",
        });

//        Controller.controlInitialDelay_$eq(60000); //60s
//        Controller.controlNonstop_$eq(false); // do it once
//        Controller.controlMode_$eq("dcm"); // use dcm
//        ControlMessage.consumer_$eq(new Consumer<Object[]>() {
//            @Override
//            public void accept(Object[] objects) {
//                System.setProperty("control received","true");
//            }
//        });
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int ingestionFactor = 1; //1, 2, 5, 10, 15, 20, 25
        int costFactor = 25;
        int sourceTupleCount = 24386900; // 24386900 max
        int workerNum = 1;
        int sourceParallelism = 1;
        int parallelism = 4*workerNum;
        int sinkParallelism = 1;
        int sourceDelay = 0; // 50000/ingestionFactor;
        int fraudDetectorProcessingDelay = 200000*costFactor;  //sleep x ns
        setupEnvironment(env, pt);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://10.128.0.10:8020"),conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new org.apache.hadoop.fs.Path("/IBM-transaction-dataset.csv"))));
        reader.readLine();
        String strLine;
        HashMap<String, HashSet<String>> untrusted = new HashMap<>();
        for(int i=0;i<100 && (strLine = reader.readLine())!= null;++i)   {
            String[] arr = strLine.split(",");
            if(!untrusted.containsKey(arr[9])){
                untrusted.put(arr[9], new HashSet<>());
            }
            untrusted.get(arr[9]).add(arr[8]);
        }
        reader.close();

        DataStream<Row> dynamicSource = env.addSource(new ParallelSourceFunction<Row>() {
            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                Configuration conf2 = new Configuration();
                FileSystem fs2 = FileSystem.get(new URI("hdfs://10.128.0.10:8020"),conf2);
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs2.open(new org.apache.hadoop.fs.Path("/IBM-transaction-dataset.csv"))));
                reader.readLine();
                String strLine2;
                int current = 0;
                int limit = sourceTupleCount/sourceParallelism;
                while ((strLine2 = reader.readLine()) != null && current < limit)   {
                    busySleep(sourceDelay);
                    String[] arr = strLine2.split(",");
                    Row r = new Row(14);
                    r.setField(0, Integer.parseInt(arr[0])); //user
                    r.setField(1, Integer.parseInt(arr[1])); //card
                    r.setField(2, Integer.parseInt(arr[2])); //year
                    r.setField(3, Integer.parseInt(arr[3])); //month
                    r.setField(4, Integer.parseInt(arr[4])); //date
                    r.setField(5, arr[5]); //time
                    r.setField(6, arr[6]); //amount
                    r.setField(7, arr[7]); //use chip
                    r.setField(8, arr[8]); //merchant name
                    r.setField(9, arr[9]); //merchant city
                    r.setField(10, arr[10]); //merchant state
                    r.setField(11, arr[11].isEmpty()? null :Double.valueOf(arr[11]).intValue()); //zip code
                    r.setField(12, Integer.parseInt(arr[12])); //MCC
                    r.setField(13, arr[0]); //errors
                    ctx.collect(r);
                    current++;
                }
                reader.close();
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(sourceParallelism);
        dynamicSource.keyBy((KeySelector<Row, Integer>) value -> {
            return value.getField(10).hashCode(); //use merchant state
        }).process(new KeyedProcessFunction<Integer, Row, Row>() {
            HashMap<String, HashSet<String>> merchants = untrusted;
            @Override
            public void processElement(
                    Row value,
                    Context ctx,
                    Collector<Row> out) throws Exception {
//                Row enrichment = new Row(2);
//                HashSet<String> merchants = untrusted.get((String)value.getFieldAs(9));
//                if(merchants != null && merchants.contains((String)value.getFieldAs(8))){
//                    enrichment.setField(0, true);
//                }else{
//                    enrichment.setField(0, false);
//                }
//                enrichment.setField(1, value.getField(2).toString()+"/"+value.getField(3)+"/"+value.getField(4)+" "+value.getField(5));
//                out.collect(Row.join(value, enrichment));
                out.collect(value);

        }}).setParallelism(parallelism).process(new ProcessFunction<Row, Boolean>() {

            @Override
            public void processElement(
                    Row value,
                    ProcessFunction<Row, Boolean>.Context ctx,
                    Collector<Boolean> out) throws Exception {
                if(System.getProperty("control received")==null){
                    busySleep(fraudDetectorProcessingDelay);
                }
                out.collect(true);
            }
        }).setParallelism(parallelism).addSink(new SinkFunction<Boolean>() {
            ArrayList<Boolean> result = new ArrayList<>();
            @Override
            public void invoke(Boolean value, Context context) throws Exception {
                result.add(value);
            }
        }).setParallelism(sinkParallelism);

        // execute program
          env.execute("Fraud detection");
//        Thread.sleep(5000);
//        jobClient.pause();
//        Thread.sleep(20000);
//        jobClient.resume();
//        jobClient.getJobExecutionResult().get();
    }


//    static class MyWatermarkStrategy implements WatermarkStrategy<Long> {
//        @Override
//        public WatermarkGenerator<Long> createWatermarkGenerator(
//                WatermarkGeneratorSupplier.Context context) {
//            return new WatermarkGenerator<Long>(){
//                long current = 0;
//                @Override
//                public void onEvent(
//                        Long event,
//                        long eventTimestamp,
//                        WatermarkOutput output) {
//                    current++;
//                }
//
//                @Override
//                public void onPeriodicEmit(WatermarkOutput output) {
//                    output.emitWatermark(new Watermark(current));
//                }
//            };
//        }
//    }


    public static class StreamDataSource extends RichParallelSourceFunction<Tuple2<Long, Long>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws InterruptedException {
            long count = 0;
            while (running && count < 10000) {
                ctx.collect(new Tuple2<>(1L, 1L));
                for(int i=0;i<10;++i){
                    ctx.collect(new Tuple2<>(2L, 1L));
                }
                ctx.collect(new Tuple2<>(3L, 1L));
                ctx.collect(new Tuple2<>(4L, 1L));
                count++;
            }
            ctx.close();
        }

        @Override
        public void cancel() {
            running = false;
        }
    }


    public static class StreamDataSource1 extends RichParallelSourceFunction<Long> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Long> ctx) throws InterruptedException {

            int count = 0;
            while (running && count < 30) {
                ctx.collect((long)count%3);
                count++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
