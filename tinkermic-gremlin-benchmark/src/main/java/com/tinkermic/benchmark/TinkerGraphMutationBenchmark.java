package com.tinkermic.benchmark;

import com.tinkermic.benchmark.util.AbstractTinkerGraphMutationBenchmark;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;

/**
 * Runs mutation benchmarks against a {@link org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph}.
 */
public class TinkerGraphMutationBenchmark extends AbstractTinkerGraphMutationBenchmark {
    private Vertex a;
    private Vertex b;
    private Vertex c;
    private Edge e;

    @Setup
    @Override
    public void prepare() {
        super.prepare();
        a = g.addV().next();
        b = g.addV().next();
        c = g.addV().next();
        e = b.addEdge("knows", c);
    }

    @Benchmark
    public Vertex testAddVertex() {
        return graph.addVertex("test");
    }

    @Benchmark
    public VertexProperty testVertexProperty() {
        return a.property("name", "Susan");
    }

    @Benchmark
    public Edge testAddEdge() {
        return a.addEdge("knows", b);
    }

    @Benchmark
    public Property testEdgeProperty() {
        return e.property("met", 1967);
    }

    @Benchmark
    public Vertex testAddV() {
        return g.addV("test").next();
    }

    @Benchmark
    public Vertex testVertexPropertyStep() {
        return g.V(a).property("name", "Susan").next();
    }

    @Benchmark
    public Edge testAddE() {
        return g.V(a).as("a").V(b).as("b").addE("knows").from("a").to("b").next();
    }

    @Benchmark
    public Edge testEdgePropertyStep() {
        return g.E(e).property("met", 1967).next();
    }
}
