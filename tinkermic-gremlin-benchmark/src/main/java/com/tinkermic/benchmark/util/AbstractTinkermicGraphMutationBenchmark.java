package com.tinkermic.benchmark.util;

import com.tinkermic.benchmark.jmh.AbstractBenchmarkBase;
import com.tinkermic.gremlin.structure.TinkermicGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.UUID;

/**
 * Graph write and update benchmarks extend {@code AbstractTinkermicGraphMutationBenchmark}.
 * {@code AbstractTinkermicGraphMutationBenchmark} runs setup once per invocation so that benchmark measurements
 * are made on an empty {@link com.tinkermic.gremlin.structure.TinkermicGraph}.  This approach was taken to isolate the tested method from the
 * performance side effects of unbounded graph growth.
 */
@State(Scope.Thread)
public abstract class AbstractTinkermicGraphMutationBenchmark extends AbstractBenchmarkBase {
    protected Graph graph;
    protected GraphTraversalSource g;

    @Setup(Level.Invocation)
    public void prepare() {
        graph = TinkermicGraph.open("datomic:mem://temp-" + UUID.randomUUID());
        g = graph.traversal();
    }
}