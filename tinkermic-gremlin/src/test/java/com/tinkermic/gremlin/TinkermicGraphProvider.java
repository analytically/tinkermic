package com.tinkermic.gremlin;

import com.tinkermic.gremlin.structure.*;
import datomic.Peer;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.*;

import static org.junit.Assert.assertTrue;

public class TinkermicGraphProvider extends AbstractGraphProvider {
    private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
        add(TinkermicEdge.class);
        add(TinkermicElement.class);
        add(TinkermicGraph.class);
        add(TinkermicProperty.class);
        add(TinkermicVertex.class);
        add(TinkermicVertexProperty.class);
    }};

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName, final LoadGraphWith.GraphData graphData) {
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, TinkermicGraph.class.getName());
            put(TinkermicGraph.DATOMIC_DB_URI, "datomic:mem://tinkermic-gremlin-" + graphName);
        }};
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        if (null != graph) {
            if (graph.tx().isOpen()) graph.tx().rollback();
        }
        Peer.deleteDatabase(configuration.getString(TinkermicGraph.DATOMIC_DB_URI));
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }

    @Override
    public Object convertId(Object id, Class<? extends Element> c) {
        try {
            return UUID.fromString(id.toString());
        }
        catch(Exception e) {
            return UUID.fromString(id.toString() + "-44cf-4dfe-a102-ceca8bec569f");
        }
    }

    @Override
    public String convertLabel(String label) {
        return label.replaceAll("-", "");
    }
}
