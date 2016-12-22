package com.tinkermic.gremlin.structure;

import com.tinkermic.benchmark.jmh.AbstractBenchmarkBase;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.UUID;

/**
 * Read-only graph benchmarks extend {@code AbstractTinkermicGraphBenchmark}. Annotating your benchmark class
 * with {@link LoadGraphWith} will load the {@link TinkermicGraph} instance with the desired data set.
 */
@State(Scope.Thread)
public class AbstractTinkermicGraphBenchmark extends AbstractBenchmarkBase {

    private final String PATH = "/org/apache/tinkerpop/gremlin/structure/io/gryo/";

    protected TinkermicGraph graph;
    protected GraphTraversalSource g;

    /**
     * Opens a new {@link TinkermicGraph} instance and optionally preloads it with one of the test data sets enumerated
     * in {@link LoadGraphWith}.
     *
     * @throws IOException on failure to load graph
     */
    @Setup
    public void prepare() throws IOException {
        graph = TinkermicGraph.open("datomic:mem://temp-" + UUID.randomUUID());
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

            // call requestIndex after the import to reduce the connection time for a transactor and peers
            // configured for ongoing transactional load
            graph.connection().requestIndex();

            // call gcStorage after indexing job requested above is completed, to recover underlying storage space
            graph.connection().gcStorage(new Date());
        }
    }
}