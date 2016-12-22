package com.tinkermic.benchmark.util;

import com.tinkermic.benchmark.jmh.AbstractBenchmarkBase;
import com.tinkermic.gremlin.structure.AbstractTinkermicGraphBenchmark;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.io.InputStream;

/**
 * Read-only graph benchmarks extend {@code AbstractNeo4jGraphBenchmark}. Annotating your benchmark class
 * with {@link LoadGraphWith} will load the {@link Neo4jGraph} instance with the desired data set.
 */
@State(Scope.Thread)
public class AbstractNeo4jGraphBenchmark extends AbstractBenchmarkBase {

    private final String PATH = "/org/apache/tinkerpop/gremlin/structure/io/gryo/";

    protected Neo4jGraph graph;
    protected GraphTraversalSource g;

    /**
     * Opens a new {@link Neo4jGraph} instance and optionally preloads it with one of the test data sets enumerated
     * in {@link LoadGraphWith}.
     *
     * @throws IOException on failure to load graph
     */
    @Setup
    public void prepare() throws IOException {
        graph = Neo4jGraph.open(TestHelper.getRootOfBuildDirectory(getClass()).getAbsolutePath() + "/neo4j-test/");
        g = graph.traversal();

        LoadGraphWith[] loadGraphWiths = this.getClass().getAnnotationsByType(LoadGraphWith.class);
        LoadGraphWith loadGraphWith = loadGraphWiths.length == 0 ? null : loadGraphWiths[0];
        LoadGraphWith.GraphData loadGraphWithData = null == loadGraphWith ? null : loadGraphWith.value();

        String graphFile;
        if (loadGraphWithData != null) {
            if (loadGraphWithData.equals(LoadGraphWith.GraphData.GRATEFUL)) {
                graphFile = "grateful-dead.kryo";
            } else if (loadGraphWithData.equals(LoadGraphWith.GraphData.MODERN)) {
                graphFile = "tinkerpop-modern.kryo";
            } else if (loadGraphWithData.equals(LoadGraphWith.GraphData.CLASSIC)) {
                graphFile = "tinkerpop-classic.kryo";
            } else if (loadGraphWithData.equals(LoadGraphWith.GraphData.CREW)) {
                graphFile = "tinkerpop-crew.kryo";
            } else {
                throw new RuntimeException("Could not load graph with " + loadGraphWithData);
            }

            GraphReader reader = GryoReader.build().create();
            try (InputStream stream = AbstractTinkermicGraphBenchmark.class.getResourceAsStream(PATH + graphFile)) {
                reader.readGraph(stream, graph);
            }
        }
    }
}