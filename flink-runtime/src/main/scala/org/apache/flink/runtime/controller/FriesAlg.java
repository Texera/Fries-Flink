package org.apache.flink.runtime.controller;

import com.google.common.collect.Sets;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import java.util.*;
import java.util.stream.Collectors;

public class FriesAlg {

    public static HashMap<String, HashSet<String>> computeMCS(
            HashMap<String, HashSet<String>> workerDAG, String[] reconfigWorkers, String[] oneToManyWorkers) {

        DirectedAcyclicGraph<String, DefaultEdge> dag = toJgraphtDag(workerDAG);

        // add all reconfiguration operators to M
        Set<String> reconfigWorkerSet = new HashSet<>(Arrays.asList(reconfigWorkers));
        Set<String> M = new HashSet<>(reconfigWorkerSet);

        // for each one-to-many operator, add it to M if its downstream has a reconfiguration operator
        for (String oneToManyWorker : oneToManyWorkers) {
            Set<String> intersection = Sets.intersection(dag.getDescendants(oneToManyWorker), reconfigWorkerSet);
            if (! intersection.isEmpty()) {
                M.add(oneToManyWorker);
            }
        }

        List<String> topoNodes = new ArrayList<>();
        dag.iterator().forEachRemaining(v -> topoNodes.add(v));

        List<String> reverseTopoNodes = new ArrayList<>();
        dag.iterator().forEachRemaining(v -> reverseTopoNodes.add(0, v));

        Set<String> forwardVertices = new HashSet<>();
        Set<String> backwardVertices = new HashSet<>();

        for (String node : topoNodes) {
            Set<DefaultEdge> parentEdges = dag.incomingEdgesOf(node);
            Set<String> parents = parentEdges.stream().map(e -> dag.getEdgeSource(e)).collect(Collectors.toSet());
            boolean fromParent = parents.stream().anyMatch(e -> forwardVertices.contains(e));
            if (M.contains(node) || fromParent) {
                forwardVertices.add(node);
            }
        }
        for (String node : reverseTopoNodes) {
            Set<DefaultEdge> childEdges = dag.outgoingEdgesOf(node);
            Set<String> children = childEdges.stream().map(e -> dag.getEdgeTarget(e)).collect(Collectors.toSet());
            boolean fromChildren = children.stream().anyMatch(e -> backwardVertices.contains(e));
            if (M.contains(node) || fromChildren) {
                backwardVertices.add(node);
            }
        }

        Set<String> resultVertices = Sets.intersection(forwardVertices, backwardVertices);

        DirectedAcyclicGraph<String, DefaultEdge> resultDag = new DirectedAcyclicGraph<>(DefaultEdge.class);
        M.forEach(m -> resultDag.addVertex(m));
        resultVertices.forEach(v -> resultDag.addVertex(v));

        dag.edgeSet().forEach(e -> {
            String source = dag.getEdgeSource(e);
            String target = dag.getEdgeTarget(e);
            if (resultVertices.contains(source) && resultVertices.contains(target)) {
                resultDag.addEdge(source, target);
            }
        });

        return fromJgraphtDag(resultDag);
    }

    public static String[] getSources(HashMap<String, HashSet<String>> graph){
        DirectedAcyclicGraph<String, DefaultEdge> dag = toJgraphtDag(graph);
        List<String> results = new ArrayList<>();
        dag.forEach(vertex ->{
            if(dag.inDegreeOf(vertex) == 0){
                results.add(vertex);
            }
        });
        return results.toArray(new String[0]);
    }


    public static DirectedAcyclicGraph<String, DefaultEdge> toJgraphtDag(HashMap<String, HashSet<String>> dag) {
        DirectedAcyclicGraph<String, DefaultEdge> result = new DirectedAcyclicGraph<>(DefaultEdge.class);
        dag.forEach((vertex, connected) -> {
            result.addVertex(vertex);
            connected.forEach(c -> {
                result.addVertex(c);
                result.addEdge(vertex, c);
            });
        });
        return result;
    }

    public static HashMap<String, HashSet<String>> fromJgraphtDag(DirectedAcyclicGraph<String, DefaultEdge> dag) {
        HashMap<String, HashSet<String>> result = new HashMap<>();
        dag.vertexSet().forEach(v -> result.put(v, new HashSet<>()));
        dag.edgeSet().forEach(e -> {
            String from = dag.getEdgeSource(e);
            String to = dag.getEdgeTarget(e);
            result.get(from).add(to);
        });
        return result;
    }

}
