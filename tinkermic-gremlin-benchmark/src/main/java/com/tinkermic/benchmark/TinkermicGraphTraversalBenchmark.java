package com.tinkermic.benchmark;

import com.tinkermic.gremlin.structure.AbstractTinkermicGraphBenchmark;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import org.openjdk.jmh.annotations.Benchmark;

import java.util.List;

/**
 * Runs a traversal benchmarks against a {@link com.tinkermic.gremlin.structure.TinkermicGraph} loaded
 * with the Grateful Dead data set.
 */
@LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
public class TinkermicGraphTraversalBenchmark extends AbstractTinkermicGraphBenchmark {

    @Benchmark
    public List<Vertex> g_V_out_out_out() throws Exception {
        return g.V().out().toList();
    }

}