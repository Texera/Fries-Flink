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

package org.apache.flink.tests.util;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.tests.util.activation.OperatingSystemRestriction;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Tests for {@link TestUtils}. */
public class TestUtilsTest extends TestLogger {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

//    @BeforeClass
//    public static void setupClass() {
//        OperatingSystemRestriction.forbid(
//                "Symbolic links usually require special permissions on Windows.",
//                OperatingSystem.WINDOWS);
//    }

    public Tuple2<Long,String> createSampleTuple(Long l, String s){
        return new Tuple2<>(l,s);
    }

    @Test
    public void copyDirectory() throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setExecutionMode(ExecutionMode.PIPELINED_FORCED);
        env.setParallelism(1);

        List<Tuple2<Long,String>> dataset1 = new ArrayList<>();
        dataset1.add(createSampleTuple(0L,"apple"));
        dataset1.add(createSampleTuple(1L,"banana"));
        dataset1.add(createSampleTuple(2L,"orange"));
        dataset1.add(createSampleTuple(3L,"watermelon"));
        dataset1.add(createSampleTuple(4L,"grape"));


        List<Tuple2<Long,String>> dataset2 = new ArrayList<>();
        dataset2.add(createSampleTuple(0L,"cat"));
        dataset2.add(createSampleTuple(1L,"dog"));
        dataset2.add(createSampleTuple(2L,"bird"));
        dataset2.add(createSampleTuple(3L,"mouse"));
        dataset2.add(createSampleTuple(4L,"turtle"));




        DataStream<Tuple2<Long,String>> ds = env.fromCollection(dataset1).shuffle().map(t ->{Thread.sleep(Math.abs(new Random().nextLong())%300); return t;}).returns(
                Types.TUPLE(Types.LONG, Types.STRING));
        DataStream<Tuple2<Long,String>> ds2 = env.fromCollection(dataset2).shuffle();
        //ds.join(ds2, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0).equalTo(0).print();
        DataStream<Tuple2<Long,String>> union = ds.union(ds2);
        union.print();
        System.out.println(env.getExecutionPlan());
        env.execute();

//        Path[] files = {
//            Paths.get("file1"), Paths.get("dir1", "file2"),
//        };
//
//        Path source = temporaryFolder.newFolder("source").toPath();
//        for (Path file : files) {
//            Files.createDirectories(source.resolve(file).getParent());
//            Files.createFile(source.resolve(file));
//        }
//
//        Path symbolicLink = source.getParent().resolve("link");
//        Files.createSymbolicLink(symbolicLink, source);
//
//        Path target = source.getParent().resolve("target");
//        TestUtils.copyDirectory(symbolicLink, target);
//
//        for (Path file : files) {
//            Assert.assertTrue(Files.exists(target.resolve(file)));
//        }
    }
}
