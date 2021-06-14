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



import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;

public class JoinWithStaticExample {


    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(new String[] {
                "--classloader.check-leaked-classloader","false",
                "--environment.parallelism", "3",
                "--state_backend.checkpoint_directory", "file:///home/shengqun97/",
                "--test.simulate_failure", "false",
                "--test.simulate_failure.max_failures", String.valueOf(1),
                "--test.simulate_failure.num_records", "100",
                "--print-level", "0",
                "--hdfs-log-storage","hdfs://10.128.0.5:8020/",
                "--enable-logging","true",
        });

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        setupEnvironment(env, pt);
        DataStream<Long> leftSource = env.addSource(new StreamDataSource()).name("Demo Source1");
        DataStream<Long> rightSource = env.addSource(new StreamDataSource1()).name("Demo Source2");

//        leftSource.join(rightSource)
//                .where(x -> x%3)
//                .equalTo(x -> x)
//                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1000)))
//                .apply(new JoinFunction<Long, Long, Long>() {
//                    @Override
//                    public Long join(Long first, Long second) {
//                        return first+second;
//                    }
//                }).countWindowAll(10).sum(0).setParallelism(1).print();



//        leftSource.filter((x) -> x%20==0).forward().union(rightSource).keyBy(x -> x).sum(0).print();

        leftSource.connect(rightSource).process(new CoProcessFunction<Long, Long, Tuple2<Long, Long>>() {

            private final ArrayList<Long> list1 = new ArrayList<>();
            private final ArrayList<Long> list2 = new ArrayList<>();

            @Override
            public void processElement1(
                    Long value,
                    Context ctx,
                    Collector<Tuple2<Long, Long>> out) throws Exception {
                list1.add(value);
                for (Long aLong : list2) {
                    out.collect(new Tuple2<Long, Long>(value, aLong));
                }
            }

            @Override
            public void processElement2(
                    Long value,
                    Context ctx,
                    Collector<Tuple2<Long, Long>> out) throws Exception {
                list2.add(value);
                for (Long aLong : list1) {
                    out.collect(new Tuple2<Long, Long>(value, aLong));
                }
            }
        }).map(x -> x.f0*x.f1).windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(1000))).sum(0).print();

        // execute program
        env.execute("Join Example");
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


    public static class StreamDataSource extends RichParallelSourceFunction<Long> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Long> ctx) throws InterruptedException {
            long count = 0;
            while (running && count < 20000) {
                ctx.collect(count);
                count++;
            }
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
            while (running && count < 600) {
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
