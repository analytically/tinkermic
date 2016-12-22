package com.tinkermic.benchmark;

import com.tinkermic.benchmark.util.AbstractTinkerGraphBenchmark;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import org.openjdk.jmh.annotations.Benchmark;

import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

/**
 * Runs a traversal benchmarks against a {@link org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph} loaded
 * with the Grateful Dead data set.
 */
@LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
public class TinkerGraphTraversalBenchmark extends AbstractTinkerGraphBenchmark {

    @Benchmark
    public List<Vertex> g_V_outE_inV_outE_inV_outE_inV() throws Exception {
        return g.V().outE().inV().outE().inV().outE().inV().toList();
    }

    @Benchmark
    public List<Vertex> g_V_out_out_out() throws Exception {
        return g.V().out().out().out().toList();
    }

    @Benchmark
    public List<Path> g_V_out_out_out_path() throws Exception {
        return g.V().out().out().out().path().toList();
    }

    @Benchmark
    public List<Vertex> g_V_repeatXoutX_timesX2X() throws Exception {
        return g.V().repeat(out()).times(2).toList();
    }

    @Benchmark
    public List<Vertex> g_V_repeatXoutX_timesX3X() throws Exception {
        return g.V().repeat(out()).times(3).toList();
    }

    @Benchmark
    public List<List<Object>> g_V_localXout_out_valuesXnameX_foldX() throws Exception {
        return g.V().local(out().out().values("name").fold()).toList();
    }

    @Benchmark
    public List<List<Object>> g_V_out_localXout_out_valuesXnameX_foldX() throws Exception {
        return g.V().out().local(out().out().values("name").fold()).toList();
    }

    @Benchmark
    public List<List<Object>> g_V_out_mapXout_out_valuesXnameX_toListX() throws Exception {
        return g.V().out().map(v -> g.V(v.get()).out().out().values("name").toList()).toList();
    }

    @Benchmark
    public List<Map<Object, Long>> g_V_label_groupCount() throws Exception {
        return g.V().label().groupCount().toList();
    }

    @Benchmark
    public List<Object> g_V_match_selectXbX_valuesXnameX() throws Exception {
        return g.V().match(
                __.as("a").has("name", "Garcia"),
                __.as("a").in("writtenBy").as("b"),
                __.as("a").in("sungBy").as("b")).select("b").values("name").toList();
    }

    @Benchmark
    public List<Edge> g_E_hasLabelXwrittenByX_whereXinV_inEXsungByX_count_isX0XX_subgraphXsgX() throws Exception {
        return g.E().hasLabel("writtenBy").where(__.inV().inE("sungBy").count().is(0)).subgraph("sg").toList();
    }
}