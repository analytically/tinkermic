package com.tinkermic.benchmark.util;

import com.tinkermic.benchmark.jmh.AbstractBenchmarkBase;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.openjdk.jmh.annotations.*;

@State(Scope.Benchmark)
public abstract class AbstractNeo4jGraphMutationBenchmark extends AbstractBenchmarkBase {
    protected Graph graph;
    protected GraphTraversalSource g;

    @Setup
    public void prepare() {
        System.out.println("NEO STARTUP");
        graph = Neo4jGraph.open(TestHelper.getRootOfBuildDirectory(getClass()).getAbsolutePath() + "/neo4j-test/");
        g = graph.traversal();
    }

    @TearDown
    public void tearDown() throws Exception {
        graph.close();
    }
}