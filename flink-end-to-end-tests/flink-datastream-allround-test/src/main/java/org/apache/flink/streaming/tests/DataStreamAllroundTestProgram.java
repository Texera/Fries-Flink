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

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
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
                "--state_backend.checkpoint_directory", "file:///chkpts",
                "--environment.checkpoint_interval","10000",
                "--test.simulate_failure", "false",
                "--test.simulate_failure.max_failures", String.valueOf(1),
                "--test.simulate_failure.num_records", "100",
                "--environment.restart_strategy","no_restart",
                "--print-level", "1",
                // "--state.backend.rocksdb.memory.managed","false",
//                "--hdfs-log-storage","hdfs://10.128.0.5:8020/",
                "--enable-logging","false",
                "--clear-old-log","true",
                "--storage-type","local"
        });

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        setupEnvironment(env, pt);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

//        ControlMessage.consumer_$eq(new Consumer<Object[]>() {
//            @Override
//            public void accept(Object[] objects) {
//                System.out.println(123123);
//                System.setProperty("delay", "false");
//            }
//        });

        // add a keyed stateful map operator, which uses Kryo for state serialization
        DataStream<String> eventStream =
                env.addSource(new SourceFunction<String>() {
                            @Override
                            public void run(SourceContext<String> ctx) throws Exception {
                                while(true){
                                    Thread.sleep(1000);
                                    ctx.collect("123");
                                }
                            }

                            @Override
                            public void cancel() {

                            }
                        })
                        .name("source")
                        .uid(EVENT_SOURCE.getUid()).setParallelism(1);
        eventStream.shuffle().process(new ProcessFunction<String, Object>() {

            String myID = "";

            @Override
            public void setRuntimeContext(RuntimeContext t) {
                super.setRuntimeContext(t);
                myID = t.getTaskName()+"-"+t.getIndexOfThisSubtask();
                System.out.println("get name of the task = "+myID);
            }

            @Override
            public void processElement(
                    String value,
                    ProcessFunction<String, Object>.Context ctx,
                    Collector<Object> out) throws Exception {
                //Thread.sleep(100);
                out.collect("123");
            }

        }).name("process1").setParallelism(4).shuffle().process(new ProcessFunction<Object, Object>() {
            String myID = "";
            int stage = 1;
            long discarded = 0;
            @Override
            public void setRuntimeContext(RuntimeContext t) {
                super.setRuntimeContext(t);
                myID = t.getTaskName()+"-"+t.getIndexOfThisSubtask();
                System.out.println("get name of the task = "+myID);
            }

            @Override
            public void processElement(
                    Object value,
                    ProcessFunction<Object, Object>.Context ctx,
                    Collector<Object> out) throws Exception {
                if(System.getProperty(myID+"-epoch")!=null && stage < 3){
                    System.out.println("received epoch at "+System.currentTimeMillis()+" discarded = "+discarded);
                    stage = 3;
                }
                if(System.getProperty(myID+"-dcm")!=null && stage < 2){
                    System.out.println("received dcm at "+System.currentTimeMillis());
                    stage = 2;
                }
                Thread.sleep(3);
                switch (stage){
                    case 1:
                        //System.out.println("processing");
                        break;
                    case 2:
                        discarded++;
                        //System.out.println("discarding");
                        break;
                    case 3:
                        //System.out.println("updated!!!!!!!!");
                        break;
                }
            }
        }).name("process2").setParallelism(8).shuffle().addSink(new SinkFunction<Object>() {

        }).setParallelism(1);

        env.execute("General purpose test job");
    }
}
