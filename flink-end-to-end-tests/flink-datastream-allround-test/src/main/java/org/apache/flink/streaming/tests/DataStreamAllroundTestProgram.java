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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

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

        }).setParallelism(1).addSink(new SinkFunction<Object>() {

        }).setParallelism(1);

        env.execute("General purpose test job");
    }
}
