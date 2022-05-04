/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/** Group of consumer {@link ExecutionVertexID}s. */
public class ConsumerVertexGroup implements Iterable<ExecutionVertexID> {
    private final List<ExecutionVertexID> vertices;
    private final List<String> workerNames;

    private ConsumerVertexGroup(List<ExecutionVertexID> vertices, List<String> workerNames) {
        this.vertices = vertices;
        this.workerNames = workerNames;
    }

    public static ConsumerVertexGroup fromMultipleVertices(ExecutionVertex[] vertices) {
        return new ConsumerVertexGroup(Arrays.stream(vertices)
                .map(ExecutionVertex::getID)
                .collect(Collectors.toList()), Arrays.stream(vertices)
                .map(v ->v.getTaskName()+"-"+v.getSubTaskIndex())
                .collect(Collectors.toList()));
    }

    public static ConsumerVertexGroup fromSingleVertex(ExecutionVertex vertex) {
        return new ConsumerVertexGroup(Collections.singletonList(vertex.getID()),
                Collections.singletonList(vertex.getTaskName()+"-"+vertex.getSubTaskIndex()));
    }

    public Iterator<String> getAllWorkerNames(){
        return this.workerNames.iterator();
    }

    @Override
    public Iterator<ExecutionVertexID> iterator() {
        return vertices.iterator();
    }

    public int size() {
        return vertices.size();
    }

    public boolean isEmpty() {
        return vertices.isEmpty();
    }

    public ExecutionVertexID getFirst() {
        return iterator().next();
    }
}
