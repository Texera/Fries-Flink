/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import org.deeplearning4j.nn.conf.GradientNormalization;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.LSTM;
import org.deeplearning4j.nn.conf.layers.RnnOutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Nadam;
import org.nd4j.linalg.lossfunctions.LossFunctions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createEventSource;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;
import static org.apache.flink.streaming.tests.TestOperatorEnum.EVENT_SOURCE;

/**
 * A general purpose test job for Flink's DataStream API operators and primitives.
 *
 * <p>The job is constructed of generic components from {@link DataStreamAllroundTestJobFactory}. It
 * currently covers the following aspects that are frequently present in Flink DataStream jobs:
 *
 * <ul>
 *   <li>A generic Kryo input type.
 *   <li>A state type for which we register a {@link KryoSerializer}.
 *   <li>Operators with {@link ValueState}.
 *   <li>Operators with union state.
 *   <li>Operators with broadcast state.
 * </ul>
 *
 * <p>The cli job configuration options are described in {@link DataStreamAllroundTestJobFactory}.
 */
public class DataStreamAllroundTestProgram {

    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(new String[] {
                "--classloader.check-leaked-classloader","false",
                "--metrics.fetcher.update-interval", "1000"});

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        setupEnvironment(env, pt);

//        ControlMessage.consumer_$eq(new Consumer<Object[]>() {
//            @Override
//            public void accept(Object[] objects) {
//                System.out.println(123123);
//                System.setProperty("delay", "false");
//            }
//        });

        // add a keyed stateful map operator, which uses Kryo for state serialization
        DataStream<Event> eventStream =
                env.addSource(createEventSource(pt))
                        .name(EVENT_SOURCE.getName())
                        .uid(EVENT_SOURCE.getUid()).setParallelism(1);
        eventStream.process(new ProcessFunction<Event, Object>() {

            String myID = "";

            @Override
            public void setRuntimeContext(RuntimeContext t) {
                super.setRuntimeContext(t);
                myID = t.getTaskName()+"-"+t.getIndexOfThisSubtask();
                System.out.println("get name of the task = "+myID);
//                MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
//                        .seed(256)
//                        .weightInit(WeightInit.XAVIER)
//                        .updater(new Nadam())
//                        .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue)  //Not always required, but helps with this data set
//                        .gradientNormalizationThreshold(0.5)
//                        .list()
//                        .layer(new LSTM.Builder().activation(Activation.TANH).nIn(10).nOut(10).build())
//                        .layer(new RnnOutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
//                                .activation(Activation.SOFTMAX).nIn(10).nOut(2).build())
//                        .build();
//                MultiLayerNetwork net = new MultiLayerNetwork(conf);
//                net.init();
//
//                List<Integer> user_prev_trans = Arrays.asList(1, 2, 3, 4, 5, 7, 8, 9, 8, 8, 8, 8, 8, 8, 8, 8,8,8,8,9);
//                int len = user_prev_trans.size();
//                int [] user_trans = ArrayUtils.toPrimitive(user_prev_trans.subList(Math.max(0,len-10),len).toArray(
//                        new Integer[0]));
//                if(user_trans.length > 10){
//                    user_trans = Arrays.copyOf(user_trans, 10);
//                }else if(user_trans.length < 10){
//                    user_trans = ArrayUtils.addAll(new int[10 - user_trans.length], user_trans);
//                }
//                INDArray input = Nd4j.create(user_trans, new int[]{1,10,1});
//                double[] output = net.output(input).reshape(2).toDoubleVector();
//                System.out.println(output);
            }

            @Override
            public void processElement(
                    Event value,
                    ProcessFunction<Event, Object>.Context ctx,
                    Collector<Object> out) throws Exception {
                System.out.println(value);
                //if(System.getProperty(myID) == null)
                    Thread.sleep(1000);
                out.collect("123");
            }

        }).setParallelism(40).addSink(new SinkFunction<Object>() {

        }).setParallelism(1);

        env.execute("General purpose test job");
    }
}
