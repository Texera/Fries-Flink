package org.apache.flink.streaming.tests;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;

import org.apache.flink.util.Collector;

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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

abstract class MyInferenceOp<T, U> extends KeyedProcessFunction<Integer, Row, Row> implements CheckpointedFunction {
    public String myID = "";
    public long startTime = 0;
    public long processed = 0;
    public long numSample = 0;
    public MultiLayerNetwork net = null;
    public int currentInputNum;
    public int modelScaleFactor;
    public int featureSize;
    public MapState<T, LinkedList<U>> prev_transaction_state= null;
    HashMap<T, LinkedList<U>> prev_transaction = new HashMap<>();
    boolean modelUpdated = false;
    boolean updateModel = false;

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        prev_transaction_state.clear();
        prev_transaction_state.putAll(prev_transaction);
    }

    public MyInferenceOp(int featureSize, int numInputEvents, int scaleFactor, boolean updateModel){
        currentInputNum = numInputEvents;
        this.featureSize = featureSize;
        modelScaleFactor = scaleFactor;
        this.updateModel = updateModel;
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        super.setRuntimeContext(t);
        myID = t.getTaskName()+"-"+t.getIndexOfThisSubtask();
        System.out.println("get name of the task = "+myID);
        System.out.println(myID+" start time="+System.currentTimeMillis());
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        buildRNN(currentInputNum);
        System.out.println(myID+" finished building rnn");
        startTime = System.currentTimeMillis();
    }

    @Override
    public void close() throws Exception {
        System.out.println(myID+" average processing time = "+processed/(double)numSample);
        super.close();
    }

    void buildRNN(int numInputEvts){
        currentInputNum = numInputEvts;
        NeuralNetConfiguration.ListBuilder confBuilder = new NeuralNetConfiguration.Builder()
                .seed(256)
                .weightInit(WeightInit.XAVIER)
                .updater(new Nadam())
                .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue)  //Not always required, but helps with this data set
                .gradientNormalizationThreshold(0.5)
                .list().layer(new LSTM.Builder().activation(Activation.TANH).nIn(featureSize).nOut(modelScaleFactor).build());
        int originalFactor = modelScaleFactor;
        while(modelScaleFactor>8){
            confBuilder=confBuilder.layer(new LSTM.Builder().activation(Activation.TANH).nIn(modelScaleFactor).nOut(modelScaleFactor/2).build());
            modelScaleFactor/=2;
        }
        while(modelScaleFactor< originalFactor){
            confBuilder=confBuilder.layer(new LSTM.Builder().activation(Activation.TANH).nIn(modelScaleFactor).nOut(modelScaleFactor*2).build());
            modelScaleFactor*=2;
        }
        confBuilder = confBuilder.layer(new LSTM.Builder().activation(Activation.TANH).nIn(modelScaleFactor).nOut(16).build());
        MultiLayerConfiguration conf = confBuilder.layer(new RnnOutputLayer.Builder(
                        LossFunctions.LossFunction.MCXENT)
                        .activation(Activation.SOFTMAX).nIn(16).nOut(2).build())
                .build();
        if(net !=null){
            net.clear();
            net.close();
        }
        net = new MultiLayerNetwork(conf);
        net.init();
    }

    public abstract T getKey(Row row);

    public abstract U getValue(Row row);

    public abstract INDArray mkInput(List<U> prev_trans);

    @Override
    public void processElement(
            Row value,
            KeyedProcessFunction<Integer, Row, Row>.Context ctx,
            Collector<Row> out) throws Exception {
        long begin = System.currentTimeMillis();
        T k = getKey(value);
        U v = getValue(value);
        if (!prev_transaction.containsKey(k)) {
            prev_transaction.put(k, new LinkedList<U>());
        }
        LinkedList<U> user_prev_transactions = prev_transaction.get(k);
        user_prev_transactions.add(v);
        while (user_prev_transactions.size() > currentInputNum) {
            user_prev_transactions.remove(0);
        }
        if (System.getProperty(myID) != null && !modelUpdated && updateModel) {
            System.out.println(myID+"starts building new model at "+(System.currentTimeMillis()-startTime)/1000f);
            modelUpdated = true;
            MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                    .seed(256)
                    .weightInit(WeightInit.XAVIER)
                    .updater(new Nadam())
                    .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue)  //Not always required, but helps with this data set
                    .gradientNormalizationThreshold(0.5)
                    .list().layer(new DenseLayer.Builder().activation(Activation.SOFTMAX).nIn(1).nOut(2).build()).build();
            net = new MultiLayerNetwork(conf);
            net.init();
            System.out.println(myID+"finishes building new model at "+(System.currentTimeMillis()-startTime)/1000f);
        }
        INDArray output = net.output(mkInput(user_prev_transactions));
        double fraudProb = output.getDouble(0,0,currentInputNum-1);
        double nonFraudProb = output.getDouble(0,1,currentInputNum-1);
        out.collect(Row.join(value, Row.of(nonFraudProb < fraudProb)));
        processed += System.currentTimeMillis()-begin;
        numSample++;
    }


}
