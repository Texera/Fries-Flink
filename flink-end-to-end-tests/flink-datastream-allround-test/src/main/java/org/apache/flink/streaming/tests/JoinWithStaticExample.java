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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;

public class JoinWithStaticExample {

    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        setupEnvironment(env, pt);

        // a streaming source that keeps running indefinitely
        DataStream<Long> dynamicSource = env.addSource(new SourceFunction<Long>() {
            @Override
            public void run(SourceContext<Long> ctx) throws Exception {
                int count = 0;
                while (count< 1000) {
                    count++;
                    Thread.sleep(10);
                    ctx.collect((long)count%3);
                }
            }

            @Override
            public void cancel() {

            }
        });

        // a finite source that eventually stops, this will emit a Watermark(Long.MAX_VALUE) when
        // finishing
        DataStream<Tuple2<Long, String>> staticSource = env.addSource(new SourceFunction<Tuple2<Long, String>>() {
            @Override
            public void run(SourceContext<Tuple2<Long, String>> ctx) throws Exception {
                // delay a bit so that the join operator actually has to buffer elements from
                // the first input
                Thread.sleep(5000);
                ctx.collect(new Tuple2<>(0L, "a"));
                ctx.collect(new Tuple2<>(1L, "b"));
                ctx.collect(new Tuple2<>(2L, "c"));
            }

            @Override
            public void cancel() {}
        });

        KeyedStream<Long, Long> keyedDynamic = dynamicSource.keyBy(new KeySelector<Long, Long>() {
            @Override
            public Long getKey(Long value) throws Exception {
                return value;
            }
        });

        KeyedStream<Tuple2<Long, String>, Long> keyedStatic = staticSource.keyBy(new KeySelector<Tuple2<Long,String>, Long>() {
            @Override
            public Long getKey(Tuple2<Long, String> value) throws Exception {
                return value.f0;
            }
        });

        keyedDynamic.connect(keyedStatic)
                .transform("custom join",
                        new TypeHint<Tuple3<Long, Long, Tuple2<Long, String>>>() {}.getTypeInfo(),
                        new JoinOperator<Long, Long, Tuple2<Long, String>>(BasicTypeInfo.LONG_TYPE_INFO, new TypeHint<Tuple2<Long, String>>() {}.getTypeInfo()))
                .print();

        // execute program
        env.execute("Join Example");
    }


    /**
     * Assume that the second input is the static input. We wait on a Long.MAX_VALUE watermark
     * from the second input and buffer the elements from the first input until that happens. Then
     * we just continue streaming by elements from the first input.
     *
     * <p>This assumes that both inputs are keyed on the same key K.
     */
    public static class JoinOperator<K, I1, I2>
            extends AbstractStreamOperator<Tuple3<K, I1, I2>>
            implements TwoInputStreamOperator<I1, I2, Tuple3<K, I1, I2>> {

        private boolean waitingForStaticInput;

        private ListStateDescriptor<I1> dynamicInputBuffer;
        private ListStateDescriptor<I2> staticInputBuffer;

        // this part is a bit of a hack, we manually keep track of the keys for which we
        // have buffered elements. This can change once the state allows iterating over all keys
        // we need this to iterate over the buffered input elements once we receive the watermark
        // from the second input
        private Set<K> inputKeys;

        public JoinOperator(TypeInformation<I1> dynamicType, TypeInformation<I2> staticType) {
            dynamicInputBuffer = new ListStateDescriptor<>("dyn-elements", dynamicType);
            staticInputBuffer = new ListStateDescriptor<>("build-elements", staticType);
        }

        @Override
        public void open() throws Exception {
            super.open();
            waitingForStaticInput = true;
            inputKeys = new HashSet<>();
        }

        @Override
        public void processElement1(StreamRecord<I1> element) throws Exception {

            if (waitingForStaticInput) {
                // store the element for when the static input is available
                getRuntimeContext().getListState(dynamicInputBuffer).add(element.getValue());
                inputKeys.add((K) getKeyedStateBackend().getCurrentKey());
                //System.out.println("STORING INPUT ELEMENT FOR LATER: " + element.getValue());
            } else {
                // perform nested-loop join

                // the elements we get here are scoped to the same key as the input element
                ListState<I2> joinElements = getRuntimeContext().getListState(staticInputBuffer);
                for (I2 joinElement : joinElements.get()) {
                    output.collect(new StreamRecord<>(new Tuple3<>((K) getKeyedStateBackend().getCurrentKey(), element.getValue(), joinElement)));
                }
            }
        }

        @Override
        public void processElement2(StreamRecord<I2> element) throws Exception {
            // store for joining with elements from primary input
            getRuntimeContext().getListState(staticInputBuffer).add(element.getValue());
        }

        @Override
        public void processWatermark1(Watermark mark) throws Exception {
            // we are not interrested in those
        }

        @Override
        public void processWatermark2(Watermark mark) throws Exception {
            if (mark.getTimestamp() == Long.MAX_VALUE) {
                waitingForStaticInput = false;

                // perform nested loop join for the buffered elements from input 1
                for (K key: inputKeys) {
                    getKeyedStateBackend().setCurrentKey(key);
                    ListState<I1> storedElements = getRuntimeContext().getListState(dynamicInputBuffer);

                    for (I1 storedElement: storedElements.get()) {
                        // the elements we get here are scoped to the same key as the input element
                        ListState<I2> joinElements = getRuntimeContext().getListState(staticInputBuffer);
                        for (I2 joinElement : joinElements.get()) {
                            //System.out.println("JOINING FOR STORED ELEMENT: " + joinElement);
                            output.collect(new StreamRecord<>(new Tuple3<>((K)getKeyedStateBackend().getCurrentKey(),
                                    storedElement,
                                    joinElement)));
                        }
                    }

                    // clean out the stored elements
                    storedElements.clear();
                }
                inputKeys = null;
            }
        }
    }
}
