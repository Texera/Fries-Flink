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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.tests.avro.ComplexPayloadAvro;
import org.apache.flink.streaming.tests.avro.InnerPayLoadAvro;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.applyTumblingWindows;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createArtificialKeyedStateMapper;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createArtificialOperatorStateMapper;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createEventSource;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createFailureMapper;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createSemanticsCheckMapper;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createSlidingWindow;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createSlidingWindowCheckMapper;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.isSimulateFailures;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;
import static org.apache.flink.streaming.tests.TestOperatorEnum.EVENT_SOURCE;
import static org.apache.flink.streaming.tests.TestOperatorEnum.FAILURE_MAPPER_NAME;
import static org.apache.flink.streaming.tests.TestOperatorEnum.KEYED_STATE_OPER_WITH_AVRO_SER;
import static org.apache.flink.streaming.tests.TestOperatorEnum.OPERATOR_STATE_OPER;
import static org.apache.flink.streaming.tests.TestOperatorEnum.SEMANTICS_CHECK_MAPPER;
import static org.apache.flink.streaming.tests.TestOperatorEnum.SEMANTICS_CHECK_PRINT_SINK;
import static org.apache.flink.streaming.tests.TestOperatorEnum.SLIDING_WINDOW_AGG;
import static org.apache.flink.streaming.tests.TestOperatorEnum.SLIDING_WINDOW_CHECK_MAPPER;
import static org.apache.flink.streaming.tests.TestOperatorEnum.SLIDING_WINDOW_CHECK_PRINT_SINK;
import static org.apache.flink.streaming.tests.TestOperatorEnum.TIME_WINDOW_OPER;

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
        final ParameterTool pt = ParameterTool.fromArgs(new String[]{
                "--state_backend.checkpoint_directory", "file:///home/shengqun97/",
                "--environment.checkpoint_interval","100",
        });

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        setupEnvironment(env, pt);

        // add a keyed stateful map operator, which uses Kryo for state serialization
        DataStream<Event> eventStream =
                env.addSource(createEventSource(pt))
                        .name(EVENT_SOURCE.getName())
                        .uid(EVENT_SOURCE.getUid()).setParallelism(4);

        eventStream.shuffle().print();

        env.execute("General purpose test job");
    }
}
