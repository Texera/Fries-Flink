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




import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
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
                "--state_backend.checkpoint_directory", "file:///home/12198/checkpoints",
                "--environment.checkpoint_interval","10000000",
                "--test.simulate_failure", "false",
                "--test.simulate_failure.max_failures", String.valueOf(1),
                "--test.simulate_failure.num_records", "100",
                "--environment.restart_strategy","no_restart",
                "--print-level", "0",
                // "--state.backend.rocksdb.memory.managed","false",
//                "--hdfs-log-storage","hdfs://10.128.0.5:8020/",
                "--enable-logging","false",
                "--clear-old-log","true",
                "--storage-type","local"
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
        int workerNum = 10;
        int ingestionFactor = 1; //1, 2, 5, 10, 15, 20, 25
        int costFactor = 25;
        int sourceTupleCount = 24386900; // 24386900 max
        int failureTupleIdx =  -18000000;
        int sourceParallelism = 1;
        int parallelism = 4*workerNum;
        int sinkParallelism = 1;
        int sourceDelay = 0; //50000/ingestionFactor;
        int fraudDetectorProcessingDelay = 200000*costFactor;  //sleep x ns
        setupEnvironment(env, pt);

        abstract class MySource implements CheckpointedFunction, ParallelSourceFunction<Row> {

        }

        abstract class MyParser extends KeyedProcessFunction<Integer, Row, Row> implements CheckpointedFunction{

        }

        DataStream<Row> dynamicSource = env.addSource(new MySource() {
            FSDataInputStream stream = null;
            ListState<Long> state = null;
            boolean recovered = false;
            long current = 0;

            @Override
            public void snapshotState(FunctionSnapshotContext context) throws Exception {
                long pos = stream.getPos();
                state.clear();
                state.add(pos);
                state.add(current);
                System.out.println("Saved the current pos = "+pos+" current count = "+current);

            }

            @Override
            public void initializeState(FunctionInitializationContext context) throws Exception {
                Configuration conf2 = new Configuration();
                FileSystem fs2 = FileSystem.get(new URI("hdfs://10.128.0.10:8020"),conf2);
                stream = fs2.open(new org.apache.hadoop.fs.Path("/IBM-transaction-dataset.csv"));
                state = context.getOperatorStateStore().getListState(new ListStateDescriptor(
                        "state", Long.class));
                Iterator<Long> iter = state.get().iterator();
                if(iter.hasNext()){
                    stream.seek(iter.next());
                    current = iter.next();
                    recovered = true;
                }
                System.out.println("recovered the current pos = "+stream.getPos()+" current count = "+current);
            }

            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
                reader.readLine();
                String strLine2;
                int limit = sourceTupleCount/sourceParallelism;
                while ((strLine2 = reader.readLine()) != null && current < limit)   {
                    busySleep(sourceDelay);
                    String[] arr = strLine2.split(",");
                    Row r = new Row(14);
                    r.setField(0, Integer.parseInt(arr[0])); //user
                    r.setField(1, Integer.parseInt(arr[1])); //card
                    if(current == failureTupleIdx){
                        r.setField(2, -2022); //year
                    }else{
                        r.setField(2, Integer.parseInt(arr[2])); //year
                    }
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
            return value.getField(0).hashCode();
        }).process(new MyParser() {
            //MapState<String, HashSet<String>> untrusted = null;
            HashMap<Integer, ArrayList<Row>> prev_transaction = new HashMap<>();
            MapState<Integer, ArrayList<Row>> prev_transaction_state= null;

            @Override
            public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                super.open(parameters);
//                untrusted = getRuntimeContext().getMapState(new MapStateDescriptor<String, HashSet<String>>("untrusted",
//                        Types.STRING,
//                        TypeInformation.of(new TypeHint<HashSet<String>>(){})));
//                prev_transaction_state = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, ArrayList<Row>>("prev_transactions",
//                        Types.INT,
//                        TypeInformation.of(new TypeHint<ArrayList<Row>>(){})));
//                Configuration conf = new Configuration();
//                FileSystem fs = FileSystem.get(new URI("hdfs://10.128.0.10:8020"),conf);
//                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path("/IBM-transaction-dataset.csv"))));
//                reader.readLine();
//                String strLine;
//                for(int i=0;i<10000 && (strLine = reader.readLine())!= null;++i)   {
//                    String[] arr = strLine.split(",");
//                    if(!untrusted.contains(arr[9])){
//                        untrusted.put(arr[9], new HashSet<>());
//                    }
//                    untrusted.get(arr[9]).add(arr[8]);
//                }
//                reader.close();
            }

            @Override
            public void snapshotState(FunctionSnapshotContext context) throws Exception {
                prev_transaction_state.clear();
                prev_transaction_state.putAll(prev_transaction);
//                ByteArrayOutputStream bos = new ByteArrayOutputStream();
//                ObjectOutputStream os = new ObjectOutputStream(bos);
//                os.writeObject(untrusted);
//                os.writeObject(previous_transaction);
//                String serialized_untrusted = bos.toString();
//                os.close();
//                //busySleep(10000000000L);
//                stringListState.add(serialized_untrusted);
            }

            @Override
            public void initializeState(FunctionInitializationContext context) throws Exception {
            }

            @Override
            public void processElement(
                    Row value,
                    Context ctx,
                    Collector<Row> out) throws Exception {
                Row enrichment = new Row(2);
                //HashSet<String> merchants = untrusted.get((String)value.getFieldAs(9));
//                Integer user = (Integer) value.getField(0);
//                if(!prev_transaction.containsKey(user)) {
//                    prev_transaction.put(user, new ArrayList<Row>());
//                }
//                prev_transaction.get(user).add(value);
//                if(prev_transaction.get(user).size() > 20000){
//                    prev_transaction.get(user).remove(0);
//                }
//                if(merchants != null && merchants.contains((String)value.getFieldAs(8))){
//                    enrichment.setField(0, true);
//                }else{
//                    enrichment.setField(0, false);
//                }
                enrichment.setField(1, value.getField(2).toString()+"/"+value.getField(3)+"/"+value.getField(4)+" "+value.getField(5));
                out.collect(Row.join(value, enrichment));
                //out.collect(value);

        }}).setParallelism(parallelism).process(new ProcessFunction<Row, Boolean>() {
            String myID = "";

            @Override
            public void setRuntimeContext(RuntimeContext t) {
                super.setRuntimeContext(t);
                myID = t.getTaskName()+"-"+t.getIndexOfThisSubtask();
                System.out.println("get name of the task = "+myID);
                System.out.println(myID+" start time="+System.currentTimeMillis());
            }

            @Override
            public void processElement(
                    Row value,
                    ProcessFunction<Row, Boolean>.Context ctx,
                    Collector<Boolean> out) throws Exception {

                if((int)value.getField(2) < 0){
                    System.out.println(myID+" error occur time="+System.currentTimeMillis());
                    //throw new RuntimeException("error occurred");
                }
                // busySleep(fraudDetectorProcessingDelay);

                if(System.getProperty(myID)==null){
                    busySleep(fraudDetectorProcessingDelay);
                }else{
                    busySleep(40000);
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
