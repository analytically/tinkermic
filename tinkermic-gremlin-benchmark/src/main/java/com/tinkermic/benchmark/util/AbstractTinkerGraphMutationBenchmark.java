package com.tinkermic.benchmark.util;

import com.tinkermic.benchmark.jmh.AbstractBenchmarkBase;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Graph write and update benchmarks extend {@code AbstractGraphMutateBenchmark}.  {@code AbstractGraphMutateBenchmark}
 * runs setup once per invocation so that benchmark measurements are made on an empty {@link TinkerGraph}.  This approach
 * was taken to isolate the tested method from the performance side effects of unbounded graph growth.
 *
 * @author Ted Wilmes (http://twilmes.org)
 */
@State(Scope.Thread)
public abstract class AbstractTinkerGraphMutationBenchmark extends AbstractBenchmarkBase {

    protected Graph graph;
    protected GraphTraversalSource g;

    @Setup(Level.Invocation)
    public void prepare() {
        graph = TinkerGraph.open();
        g = graph.traversal();
    }
}