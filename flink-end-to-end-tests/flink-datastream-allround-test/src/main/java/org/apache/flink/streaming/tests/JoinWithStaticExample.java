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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.bytedeco.opencv.opencv_dnn.RNNLayer;
import org.deeplearning4j.nn.conf.GradientNormalization;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.LSTM;
import org.deeplearning4j.nn.conf.layers.RnnOutputLayer;
import org.deeplearning4j.nn.conf.layers.recurrent.SimpleRnn;
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
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;

public class JoinWithStaticExample {


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
                "--environment.checkpoint_interval","10000",
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
        //ClassPathClassLoader globalLoader = new ClassPathClassLoader("/home/12198/libs", null);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int workerNum = 10;
        int numInputEvents = 10;
        int modelScaleFactor = 8;
        int numLabelClasses = 2;
        int sourceTupleCount = 100000; // 24386900 max
        boolean modelUpdating = true;
        int sourceParallelism = 1;
        int parallelism = 4*workerNum;
        int sinkParallelism = 1;
        int sourceTPS = 1000; //3K baseline -> 6K actual
        int changeTPSDelay = 50000;//50 secs
        int sourceTimeLimit = 800000;
        setupEnvironment(env, pt);
        env.setBufferTimeout(0);

        abstract class MySource implements ParallelSourceFunction<Row> {

        }

        abstract class MyMLInferenceOp extends KeyedProcessFunction<Integer, Row, Row> implements CheckpointedFunction{

        }

        DataStream<Row> dynamicSource = env.addSource(new MySource() {
            FSDataInputStream stream = null;
            ListState<Long> state = null;
            long beginTime = System.currentTimeMillis();
            boolean recovered = false;
            long current = 0;
            long id = 0;
            long sourceVersion = 0;

//            @Override
//            public void snapshotState(FunctionSnapshotContext context) throws Exception {
//                long pos = stream.getPos();
//                state.clear();
//                state.add(pos);
//                state.add(current);
//                System.out.println("source commit time = "+System.currentTimeMillis());
//                System.out.println("Saved the current pos = "+pos+" current count = "+current);
//
//            }

//            @Override
//            public void initializeState(FunctionInitializationContext context) throws Exception {
//                Configuration conf2 = new Configuration();
//                FileSystem fs2 = FileSystem.get(new URI("hdfs://10.128.0.10:8020"),conf2);
//                stream = fs2.open(new org.apache.hadoop.fs.Path("/IBM-transaction-dataset.csv"));
//                state = context.getOperatorStateStore().getListState(new ListStateDescriptor(
//                        "state", Long.class));
//                Iterator<Long> iter = state.get().iterator();
//                if(iter.hasNext()){
//                    stream.seek(iter.next());
//                    current = iter.next();
//                    recovered = true;
//                }
//                System.out.println("recovered the current pos = "+stream.getPos()+" current count = "+current);
//            }

            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                //BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
                //reader.readLine();
                String strLine2;
                long startTime = System.currentTimeMillis();
                long prevTime = startTime;
                long currentTPS = sourceTPS;
               // while ((strLine2 = reader.readLine()) != null)   {
                while(true){
                    long currentTime = System.currentTimeMillis();
                    if (currentTime >= beginTime+sourceTimeLimit)break;
                    if(changeTPSDelay!= -1){
                        boolean needChange = false;
                        needChange = currentTime >=startTime+changeTPSDelay;
                        if(needChange && sourceVersion<2){
                            currentTPS += sourceTPS*(1+sourceVersion*6);
                            System.out.println("Source change to "+currentTPS+" at "+(currentTime-beginTime)/1000);
                            startTime = currentTime;
                            sourceVersion++;
                        }
                    }

                    //String[] arr = strLine2.split(",");
                    Row r = new Row(14);
                    r.setField(0, (int)current); //user
                    r.setField(1, System.currentTimeMillis()); //card
//                    r.setField(2, Integer.parseInt(arr[2])); //year
//                    r.setField(3, Integer.parseInt(arr[3])); //month
//                    r.setField(4, Integer.parseInt(arr[4])); //date
//                    r.setField(5, arr[5]); //time
                    r.setField(6, "$10000"); //amount
//                    r.setField(7, arr[7]); //use chip
//                    r.setField(8, arr[8]); //merchant name
//                    r.setField(9, arr[9]); //merchant city
//                    r.setField(10, arr[10]); //merchant state
//                    r.setField(11, arr[11].isEmpty()? null :Double.valueOf(arr[11]).intValue()); //zip code
                    r.setField(12, id); //MCC
                    r.setField(13, sourceVersion); //errors
                    //System.out.println("S: "+id+" "+sourceVersion+" "+System.currentTimeMillis());
                    ctx.collect(r);
                    current++;
                    id++;
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
                //reader.close();
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
                out.collect(Row.project(value, new int[]{0,6,12,13,1}));
            }
        }).setParallelism(parallelism).keyBy((KeySelector<Row, Integer>) value -> {
            return (Integer) value.getField(0);
        }).process(new MyMLInferenceOp() {

            MapState<Integer, LinkedList<Double>> prev_transaction_state= null;
            @Override
            public void snapshotState(FunctionSnapshotContext context) throws Exception {
                prev_transaction_state.clear();
                prev_transaction_state.putAll(prev_transaction);
            }

            @Override
            public void initializeState(FunctionInitializationContext context) throws Exception {
                prev_transaction_state = context.getKeyedStateStore().getMapState(new MapStateDescriptor<Integer, LinkedList<Double>>("map",TypeInformation.of(Integer.class), TypeInformation.of(
                        new TypeHint<LinkedList<Double>>() {})));
            }
            String myID = "";
            HashMap<Integer, LinkedList<Double>> prev_transaction = new HashMap<>();
            long modelVersion = 0;
            MultiLayerNetwork net = null;
            MultiLayerNetwork cheapNet=null;
            int currentInputNum = numInputEvents;
            long startTime = 0;
            long processed = 0;
            int modelScaleFactor1= modelScaleFactor;
            long numSample = 0;

            @Override
            public void setRuntimeContext(RuntimeContext t) {
                super.setRuntimeContext(t);
                myID = t.getTaskName()+"-"+t.getIndexOfThisSubtask();
                System.out.println("get name of the task = "+myID);
                System.out.println(myID+" start time="+System.currentTimeMillis());
            }

            private void buildRNN(int numInputEvts){
                currentInputNum = numInputEvts;
                NeuralNetConfiguration.ListBuilder confBuilder = new NeuralNetConfiguration.Builder()
                        .seed(256)
                        .weightInit(WeightInit.XAVIER)
                        .updater(new Nadam())
                        .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue)  //Not always required, but helps with this data set
                        .gradientNormalizationThreshold(0.5)
                        .list().layer(new LSTM.Builder().activation(Activation.TANH).nIn(numInputEvts).nOut(16).build());
                for(int i=1;i<modelScaleFactor1;++i){
                     confBuilder=confBuilder.layer(new LSTM.Builder().activation(Activation.TANH).nIn(16).nOut(16).build());
                }
                confBuilder=confBuilder.layer(new LSTM.Builder().activation(Activation.TANH).nIn(16).nOut(8).build())
                        .layer(new LSTM.Builder().activation(Activation.TANH).nIn(8).nOut(16).build());
                for(int i=1;i<modelScaleFactor1;++i){
                    confBuilder=confBuilder.layer(new LSTM.Builder().activation(Activation.TANH).nIn(16).nOut(16).build());
                }
                confBuilder = confBuilder.layer(new LSTM.Builder().activation(Activation.TANH).nIn(16).nOut(numInputEvts).build());
                MultiLayerConfiguration conf = confBuilder.layer(new RnnOutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
                                .activation(Activation.SOFTMAX).nIn(numInputEvts).nOut(numLabelClasses).build())
                        .build();
                if(net !=null){
                    net.clear();
                    net.close();
                }
                net = new MultiLayerNetwork(conf);
                net.init();
            }


            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                buildRNN(numInputEvents);
                MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                        .seed(256)
                        .weightInit(WeightInit.XAVIER)
                        .updater(new Nadam())
                        .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue)  //Not always required, but helps with this data set
                        .gradientNormalizationThreshold(0.5)
                        .list().layer(new DenseLayer.Builder().activation(Activation.SOFTMAX).nIn(1).nOut(2).build()).build();
                cheapNet = new MultiLayerNetwork(conf);
                cheapNet.init();
                System.out.println(myID+" finished building rnn");
                startTime = System.currentTimeMillis();
            }

            @Override
            public void close() throws Exception {
                System.out.println(myID+" average processing time = "+processed/(double)numSample);
                super.close();
            }

                    @Override
            public void processElement(
                    Row value,
                    KeyedProcessFunction<Integer, Row, Row>.Context ctx,
                    Collector<Row> out) throws Exception {
                long begin = System.currentTimeMillis();
                int user = value.getFieldAs(0);
                long id = value.getFieldAs(2);
                long sourceVersion = value.getFieldAs(3);
                long sentTime = value.getFieldAs(4);
                double amount = Double.valueOf(((String) value.getField(1)).substring(1));
                    if (!prev_transaction.containsKey(user)) {
                        prev_transaction.put(user, new LinkedList<>());
                    }
                    LinkedList<Double> user_prev_transactions = prev_transaction.get(user);
                    user_prev_transactions.add(amount);
                    while (user_prev_transactions.size() > currentInputNum) {
                        user_prev_transactions.remove(0);
                    }
                    if (System.getProperty(myID) != null && Integer.parseInt(System.getProperty(myID)) == modelVersion) {
                        modelVersion++;
                        currentInputNum=1;
                        modelScaleFactor1=1;
                        buildRNN(currentInputNum);
                        System.out.println(myID+"switched to new model at "+(System.currentTimeMillis()-startTime)/1000f);
                    }
                    if(modelVersion>=2){
                        INDArray input = Nd4j.create(new double[]{amount}, new int[]{1, 1});
                        double[] output = cheapNet.output(input).reshape(numLabelClasses).toDoubleVector();
                        out.collect(Row.of(id, sourceVersion, modelVersion, sentTime));
                    }else{
                        int len = user_prev_transactions.size();
                        double[] user_trans = ArrayUtils.toPrimitive(user_prev_transactions
                                .subList(Math.max(0, len - currentInputNum), len)
                                .toArray(new Double[0]));
                        if (user_trans.length < currentInputNum) {
                            user_trans = ArrayUtils.addAll(new double[currentInputNum
                                    - user_trans.length], user_trans);
                        }
                        INDArray input = Nd4j.create(user_trans, new int[]{1, currentInputNum,1});
                        double[] output = net.output(input).reshape(numLabelClasses).toDoubleVector();
                        out.collect(Row.of(id, sourceVersion,modelVersion, sentTime));
                    }
                    processed += System.currentTimeMillis()-begin;
                    numSample++;
                    //System.out.println("processing = "+(System.currentTimeMillis()-begin+" ms"));
                }
        }).setParallelism(parallelism).addSink(new SinkFunction<Row>() {
                    ArrayList<Long> delays = new ArrayList<>();
                    LinkedList<Double> window = new LinkedList<>();
                    long wrong_count = 0;
                    long last_sent_wrong_count = 0;
                    long start = 0;
                    private double calculateAverage1(List<Long> marks) {
                        if (marks == null || marks.isEmpty()) {
                            return 0;
                        }

                        double sum = 0;
                        for (Long mark : marks) {
                            sum += mark;
                        }

                        return sum / marks.size();
                    }
                    private double calculateAverage2(List<Double> marks) {
                        if (marks == null || marks.isEmpty()) {
                            return 0;
                        }

                        double sum = 0;
                        for (Double mark : marks) {
                            sum += mark;
                        }

                        return sum / marks.size();
                    }
                    @Override
                    public void invoke(Row value, Context context) throws Exception {
                        long current = System.currentTimeMillis();
                        if((long)value.getFieldAs(1)!=(long)value.getFieldAs(2)){
                            wrong_count++;
                        }
                        long sentTime = value.getFieldAs(3);
                        if(start == 0){
                            start = sentTime;
                        }
                        delays.add(current-sentTime);
                        if(sentTime - start> 1000){
                            System.out.println("current delay: "+(current-sentTime));
                            window.add(calculateAverage1(delays));
                            delays.clear();
                            start = sentTime;
                            if(window.size() == 10){
                                System.out.println("R: "+calculateAverage2(window));
                                window.remove(0);
                            }
                        }
                        if(current-last_sent_wrong_count>1000){
                            System.out.println("C: "+wrong_count);
                            last_sent_wrong_count = current;
                        }
                        //System.out.println("R: "+value.getField(0)+" "+value.getField(1)+" "+value.getField(2)+" "+System.currentTimeMillis());
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
