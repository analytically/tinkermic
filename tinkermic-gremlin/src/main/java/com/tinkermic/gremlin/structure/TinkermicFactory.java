package com.tinkermic.gremlin.structure;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public final class TinkermicFactory {
    private TinkermicFactory() {
    }

    public static TinkermicGraph createClassic() {
        final TinkermicGraph g = TinkermicGraph.open("datomic:mem://tinkermic-gremlin-classic");
        generateClassic(g);
        return g;
    }

    public static void generateClassic(final TinkermicGraph g) {
        final Vertex marko = g.addVertex("name", "marko", "age", 29);
        final Vertex vadas = g.addVertex("name", "vadas", "age", 27);
        final Vertex lop = g.addVertex("name", "lop", "lang", "java");
        final Vertex josh = g.addVertex("name", "josh", "age", 32);
        final Vertex ripple = g.addVertex("name", "ripple", "lang", "java");
        final Vertex peter = g.addVertex("name", "peter", "age", 35);
        marko.addEdge("knows", vadas, "weight", 0.5f);
        marko.addEdge("knows", josh, "weight", 1.0f);
        marko.addEdge("created", lop, "weight", 0.4f);
        josh.addEdge("created", ripple, "weight", 1.0f);
        josh.addEdge("created", lop, "weight", 0.4f);
        peter.addEdge("created", lop, "weight", 0.2f);
    }

    public static TinkermicGraph createModern() {
        final TinkermicGraph g = TinkermicGraph.open("datomic:mem://tinkermic-gremlin-modern");
        generateModern(g);
        return g;
    }

    public static void generateModern(final TinkermicGraph g) {
        final Vertex marko = g.addVertex(T.label, "person", "name", "marko", "age", 29);
        final Vertex vadas = g.addVertex(T.label, "person", "name", "vadas", "age", 27);
        final Vertex lop = g.addVertex(T.label, "software", "name", "lop", "lang", "java");
        final Vertex josh = g.addVertex(T.label, "person", "name", "josh", "age", 32);
        final Vertex ripple = g.addVertex(T.label, "software", "name", "ripple", "lang", "java");
        final Vertex peter = g.addVertex(T.label, "person", "name", "peter", "age", 35);
        marko.addEdge("knows", vadas, "weight", 0.5d);
        marko.addEdge("knows", josh, "weight", 1.0d);
        marko.addEdge("created", lop, "weight", 0.4d);
        josh.addEdge("created", ripple, "weight", 1.0d);
        josh.addEdge("created", lop, "weight", 0.4d);
        peter.addEdge("created", lop, "weight", 0.2d);
    }
}
