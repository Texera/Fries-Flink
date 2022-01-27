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




import org.apache.commons.lang3.ArrayUtils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
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
import org.deeplearning4j.nn.conf.GradientNormalization;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.LSTM;
import org.deeplearning4j.nn.conf.layers.RnnOutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Nadam;
import org.nd4j.linalg.lossfunctions.LossFunctions;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;

public class JoinWithStaticExample2 {


    public static void busySleep(long nanos)
    {
        if(nanos == 0){
            return;
        }
        long elapsed;
        final long startTime = System.nanoTime();
        do {
            elapsed = System.nanoTime() - startTime;
        } while (elapsed < nanos);
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(new String[] {
                "--classloader.check-leaked-classloader","false",
                "--state_backend.checkpoint_directory", "hdfs:///10.128.0.10.8020/flink-unaligned-checkpoints",
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
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int workerNum = 1;
        int numInputEvents = 10;
        int numInputEvents2 = 500;
        int modelScaleFactor = 16;
        int modelScaleFactor2 = 64;
        int failureTupleIdx =  -18000000;
        boolean modelUpdating = true;
        int sourceParallelism = 1;
        int parallelism = 4*workerNum;
        int sinkParallelism = 1;
        int sourceTPS = 3000; //3K baseline -> 6K actual
        int changeTPSDelay = -1;//100 secs
        int changedTPS = 10000;
        int sourceTimeLimit = 300000;
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
                    Row r = new Row(14);
                    r.setField(0, (int)current); //user
                    r.setField(1, Integer.parseInt(arr[1])); //card
                    r.setField(2, Integer.parseInt(arr[2])); //year
                    r.setField(3, Integer.parseInt(arr[3])); //month
                    r.setField(4, Integer.parseInt(arr[4])); //date
                    r.setField(5, arr[5]); //time
                    if(current == failureTupleIdx){
                        r.setField(6, "$-999999999"); // invalid amount
                    }else{
                        r.setField(6, arr[6]); //amount
                    }
                    r.setField(7, arr[7]); //use chip
                    r.setField(8, arr[8]); //merchant name
                    r.setField(9, arr[9]); //merchant city
                    r.setField(10, arr[10]); //merchant state
                    r.setField(11, arr[11].isEmpty()? null :Double.valueOf(arr[11]).intValue()); //zip code
                    r.setField(12, Integer.parseInt(arr[12])); //MCC
                    r.setField(13, arr[13]); //errors
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
        }).setParallelism(sourceParallelism);
        dynamicSource.keyBy((KeySelector<Row, Integer>) value -> {
                    return (Integer) value.getField(0);
                }).process(new ProcessFunction<Row, Row>() {
            @Override
            public void processElement(
                    Row value,
                    ProcessFunction<Row, Row>.Context ctx,
                    Collector<Row> out) throws Exception {
                out.collect(Row.project(value, new int[]{0,6,8}));
            }
        }).setParallelism(parallelism).keyBy((KeySelector<Row, Integer>) value -> {
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
                        if (modelUpdated) {
                            return Nd4j.create(
                                    new double[]{prev_trans.get(prev_trans.size() - 1)},
                                    new int[]{1, 1});
                        } else {
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
                    }
                }).setParallelism(parallelism).keyBy(new KeySelector<Row, Integer>() {

                    @Override
                    public Integer getKey(Row value) throws Exception {
                        return value.getFieldAs(2).hashCode();
                    }
                }).process(new MyInferenceOp<String, Tuple2<Double, Double>>(2, numInputEvents2, modelScaleFactor2, modelUpdating) {

                    @Override
                    public void initializeState(FunctionInitializationContext context) throws Exception {
                        prev_transaction_state = context.getKeyedStateStore().getMapState(new MapStateDescriptor<String,LinkedList<Tuple2<Double, Double>>>("map"+myID,
                                TypeInformation.of(new TypeHint<String>() {}), TypeInformation.of(
                                new TypeHint<LinkedList<Tuple2<Double, Double>>>() {})));
                    }

                    @Override
                    public String getKey(Row row) {
                        return row.getFieldAs(2);
                    }

                    @Override
                    public Tuple2<Double, Double> getValue(Row row) {
                        double d = Double.valueOf(((String) row.getField(1)).substring(1));
                        double d2 = (Boolean) row.getFieldAs(3) ? 1: 0 ;
                        return Tuple2.of(d, d2);
                    }

                    @Override
                    public INDArray mkInput(List<Tuple2<Double, Double>> prev_trans) {
                        if (modelUpdated) {
                            Tuple2<Double, Double> t = prev_trans.get(prev_trans.size() - 1);
                            return Nd4j.create(
                                    new double[]{t.f0,t.f1},
                                    new int[]{1, 2});
                        } else {
                            int len = prev_trans.size();
                            INDArray arr = Nd4j.zeros(1,2, currentInputNum);
                            for(int i=0;i<currentInputNum;++i){
                                if(len-1-i>=0){
                                    Tuple2<Double, Double> t = prev_trans.get(len-1-i);
                                    arr.putScalar(new int[]{0,0,currentInputNum-i-1},t.f0);
                                    arr.putScalar(new int[]{0,1,currentInputNum-i-1},t.f1);
                                }else{
                                    arr.putScalar(new int[]{0,0,currentInputNum-i-1},0);
                                    arr.putScalar(new int[]{0,1,currentInputNum-i-1},0);
                                }
                            }
                            return arr;
                        }
                    }
                }).setParallelism(parallelism).addSink(new SinkFunction<Row>() {
                    @Override
                    public void invoke(Row value, Context context) throws Exception {
                        SinkFunction.super.invoke(value, context);
                    }
                })
//                .addSink(new TwoPhaseCommitSinkFunction<Boolean, Object,Object>(TypeInformation.of(Object.class).createSerializer(
//                env.getConfig()),TypeInformation.of(Object.class).createSerializer(
//                env.getConfig())) {
//
//            ArrayList<Boolean> results = new ArrayList<Boolean>();
//
//            @Override
//            protected void invoke(
//                    Object transaction,
//                    Boolean value,
//                    Context context) throws Exception {
//                results.add(value);
//            }
//
//            @Override
//            protected Object beginTransaction() throws Exception {
//                return null;
//            }
//
//            @Override
//            protected void preCommit(Object transaction) throws Exception {
//
//            }
//
//            @Override
//            protected void commit(Object transaction) {
//                System.out.println("sink committed time= "+System.currentTimeMillis());
//                results.clear();
//            }
//
//            @Override
//            protected void abort(Object transaction) {
//
//            }
//        })
                .setParallelism(sinkParallelism);

        // execute program
          env.execute("Fraud detection");
    }



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
