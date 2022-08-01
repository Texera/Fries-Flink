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


import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;

public class JoinWithStaticExample3 {

    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(new String[] {
                "--classloader.check-leaked-classloader","false",
                "--state_backend.checkpoint_directory", "hdfs:///10.128.0.11.8020/flink-unaligned-checkpoints",
                "--environment.checkpoint_interval","10000000",
                "--test.simulate_failure", "false",
                "--test.simulate_failure.max_failures", String.valueOf(1),
                "--test.simulate_failure.num_records", "100",
                "--environment.restart_strategy","no_restart",
                "--print-level", "0",
                "--enable-logging","false",
                "--clear-old-log","true",
                "--storage-type","local"
        });
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int workerNum = 10;
        int numInputEvents = 10;
        int numInputEvents2 = 50;
        int modelScaleFactor = 4;
        int modelScaleFactor2 = 4;
        boolean modelUpdating = false;
        int sourceParallelism = 1;
        int parallelism = 4*workerNum;
        int sinkParallelism = 1;
        int sourceTPS = 3000; //3K baseline -> 6K actual
        int changeTPSDelay = -1;//100 secs
        int changedTPS = 10000;
        int sourceTimeLimit = 300000;
        Set<String> whitelist = new HashSet<>(Arrays.asList("Walmart","Applebees","Apple","Kelly Auto Repair"));
        setupEnvironment(env, pt);

        abstract class MySource implements CheckpointedFunction, ParallelSourceFunction<Row> {}

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
                System.out.println("source commit time = "+System.currentTimeMillis());
                System.out.println("Saved the current pos = "+pos+" current count = "+current);

            }

            @Override
            public void initializeState(FunctionInitializationContext context) throws Exception {
                Configuration conf2 = new Configuration();
                FileSystem fs2 = FileSystem.get(new URI("hdfs://10.128.0.11:8020"),conf2);
                stream = fs2.open(new Path("/card_transaction.v1.csv"));
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
                long id = 0;
                long startTime = System.currentTimeMillis();
                long prevTime = startTime;
                long currentTPS = sourceTPS;
                while ((strLine2 = reader.readLine()) != null)   {
                    long currentTime = System.currentTimeMillis();
                    if (currentTime >= startTime+sourceTimeLimit)break;
                    if(changeTPSDelay!= -1 && currentTime >=startTime+changeTPSDelay && currentTPS != changedTPS){
                        currentTPS = changedTPS;
                    }

                    String[] arr = strLine2.split(",");
                    Row r = new Row(4);
                    id++;
                    r.setField(0, Integer.parseInt(arr[0])); //user
//                    r.setField(1, Integer.parseInt(arr[1])); //card
//                    r.setField(2, Integer.parseInt(arr[2])); //year
//                    r.setField(3, Integer.parseInt(arr[3])); //month
//                    r.setField(4, Integer.parseInt(arr[4])); //date
//                    r.setField(5, arr[5]); //time
                    r.setField(1, arr[6]); //amount
//                    r.setField(7, arr[7]); //use chip
                    r.setField(2, arr[8]); //merchant name
//                    r.setField(9, arr[9]); //merchant city
//                    r.setField(10, arr[10]); //merchant state
//                    r.setField(11, arr[11].isEmpty()? null :Double.valueOf(arr[11]).intValue()); //zip code
//                    r.setField(12, Integer.parseInt(arr[12])); //MCC
//                    r.setField(13, arr[13]); //errors
                    r.setField(3, id); //id
                    ctx.collect(r);
                    current++;
                    if(current == currentTPS){
                        long now = System.currentTimeMillis();
                        long duration = now-prevTime;
                        if(duration < 1000){
                            Thread.sleep(1000-duration);
                        }
                        current = 0;
                        prevTime = now;
                    }
                }
                reader.close();
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(sourceParallelism).process(new ProcessFunction<Row, Row>() {

            @Override
            public void processElement(
                    Row value,
                    ProcessFunction<Row, Row>.Context ctx,
                    Collector<Row> out) throws Exception {
                out.collect(value);
            }
        }).name("replicate").setParallelism(parallelism);


        DataStream<Row> merchant = dynamicSource.filter((row) -> {
            return !whitelist.contains((String)row.getFieldAs(2));
        }).name("f1").setParallelism(workerNum).keyBy((KeySelector<Row, String>) value -> {
            return (String) value.getField(2);
        }).process(new MyInferenceOp<String, Double>(1, numInputEvents2, modelScaleFactor2, modelUpdating) {

            @Override
            public void initializeState(FunctionInitializationContext context) throws Exception {
                prev_transaction_state = context.getKeyedStateStore().getMapState(new MapStateDescriptor<String,LinkedList<Double>>("map"+myID,
                        TypeInformation.of(new TypeHint<String>() {}), TypeInformation.of(
                        new TypeHint<LinkedList<Double>>() {})));
            }

            @Override
            public String getKey(Row row) {
                return row.getFieldAs(2);
            }

            @Override
            public Double getValue(Row row) {
                return Double.valueOf(((String) row.getField(1)).substring(1));
            }

            @Override
            public INDArray mkInput(List<Double> prev_trans) {
                int len = prev_trans.size();
                double[] user_trans = ArrayUtils.toPrimitive(prev_trans
                        .subList(Math.max(0, len - currentInputNum), len)
                        .toArray(new Double[0]));
                if (user_trans.length < currentInputNum) {
                    user_trans = ArrayUtils.addAll(new double[currentInputNum
                            - user_trans.length], user_trans);
                }
                return Nd4j.create(user_trans, new int[]{1, 1, currentInputNum});
            }
        }).name("merchant").setParallelism(parallelism);


        DataStream<Row> user = dynamicSource.keyBy((KeySelector<Row, Integer>) value -> {
            return (Integer) value.getField(0);
        }).process(new MyInferenceOp<Integer, Double>(1, numInputEvents, modelScaleFactor, modelUpdating) {

                    @Override
                    public void initializeState(FunctionInitializationContext context) throws Exception {
                        prev_transaction_state = context.getKeyedStateStore().getMapState(new MapStateDescriptor<Integer,LinkedList<Double>>("map"+myID,
                                TypeInformation.of(new TypeHint<Integer>() {}), TypeInformation.of(
                                new TypeHint<LinkedList<Double>>() {})));
                    }

                    @Override
                    public Integer getKey(Row row) {
                        return row.getFieldAs(0);
                    }

                    @Override
                    public Double getValue(Row row) {
                        return Double.valueOf(((String) row.getField(1)).substring(1));
                    }

                    @Override
                    public INDArray mkInput(List<Double> prev_trans) {
                            int len = prev_trans.size();
                            double[] user_trans = ArrayUtils.toPrimitive(prev_trans
                                    .subList(Math.max(0, len - currentInputNum), len)
                                    .toArray(new Double[0]));
                            if (user_trans.length < currentInputNum) {
                                user_trans = ArrayUtils.addAll(new double[currentInputNum
                                        - user_trans.length], user_trans);
                            }
                            return Nd4j.create(user_trans, new int[]{1, 1, currentInputNum});
                    }
                }).name("user").setParallelism(parallelism).process(new ProcessFunction<Row, Row>() {
            @Override
            public void processElement(
                    Row value,
                    ProcessFunction<Row, Row>.Context ctx,
                    Collector<Row> out) throws Exception {
                out.collect(Row.join(Row.project(value, new int[]{0,1,2,3}), Row.of((double)value.getFieldAs(4)*0.8)));
            }
        }).name("weight").filter(r -> (double)r.getFieldAs(4)> 0.3).name("f2").setParallelism(workerNum);
        user.connect(merchant).keyBy(r-> r.getField(3), r-> r.getField(3)).process(new KeyedCoProcessFunction<Long, Row, Row, Row>() {
                    HashMap<Long, Row> pairMap = new HashMap<>();
                    @Override
                    public void processElement1(
                            Row value,
                            KeyedCoProcessFunction<Long, Row, Row, Row>.Context ctx,
                            Collector<Row> out) throws Exception {
                        Long key = value.getFieldAs(3);
                        if(pairMap.containsKey(key)){
                            Row pair = pairMap.get(key);
                            out.collect(Row.join(Row.project(pair, new int[]{0,1,2}),Row.of(value.getField(4), pair.getField(4))));
                            pairMap.remove(key);
                        }else{
                            pairMap.put(key, value);
                        }
                    }

                    @Override
                    public void processElement2(
                            Row value,
                            KeyedCoProcessFunction<Long, Row, Row, Row>.Context ctx,
                            Collector<Row> out) throws Exception {
                        Long key = value.getFieldAs(3);
                        if(pairMap.containsKey(key)){
                            Row pair = pairMap.get(key);
                            out.collect(Row.join(Row.project(pair, new int[]{0,1,2}),Row.of(value.getField(4), pair.getField(4))));
                            pairMap.remove(key);
                        }else{
                            pairMap.put(key, value);
                        }
                    }
                }).name("selfjoin").setParallelism(parallelism).process(new ProcessFunction<Row, Row>(){
                    @Override
                    public void processElement(
                            Row value,
                            ProcessFunction<Row, Row>.Context ctx,
                            Collector<Row> out) throws Exception {
                        value.setField(1, Base64.getEncoder().encode(((String)value.getFieldAs(1)).getBytes(
                                StandardCharsets.UTF_8)));
                        value.setField(2, Base64.getEncoder().encode(((String)value.getFieldAs(2)).getBytes(
                                StandardCharsets.UTF_8)));
                        out.collect(value);
                    }
                }).name("encrypt").setParallelism(workerNum)
        .addSink(new SinkFunction<Row>() {
                    @Override
                    public void invoke(Row value, Context context) throws Exception {
                        SinkFunction.super.invoke(value, context);
                    }
                })
                .setParallelism(sinkParallelism);

        // execute program
          env.execute("Fraud detection");
    }
}
