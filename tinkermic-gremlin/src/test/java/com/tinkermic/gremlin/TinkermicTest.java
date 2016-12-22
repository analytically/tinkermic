package com.tinkermic.gremlin;

import com.google.common.collect.Iterators;
import com.tinkermic.gremlin.structure.TinkermicGraph;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.copyOf;
import static org.apache.tinkerpop.gremlin.structure.Direction.IN;
import static org.apache.tinkerpop.gremlin.structure.Direction.OUT;
import static org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils.count;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Simple sanity check to see if the graph functions as required.
 */
public class TinkermicTest {
    private TinkermicGraph graph;

    @Before
    public void setUp() throws Exception {
        graph = TinkermicGraph.open("datomic:mem://tinkermic-gremlin-" + UUID.randomUUID());
    }

    @After
    public void tearDown() throws Exception {
        graph.close();
    }

    @Test
    public void testAddVertex() throws Exception {
        Vertex v = graph.addVertex("foo", "bar");
        graph.tx().commit();

        assertEquals(graph.traversal().V().next(), v);

        Vertex v2 = graph.vertices(v.id()).next();
        assertEquals(v.id(), v2.id());
        assertEquals(v.property("foo"), v2.property("foo"));
    }

    @Test
    public void testAddEdge() throws Exception {
        Vertex v1 = graph.addVertex();
        Vertex v2 = graph.addVertex();
        Edge edge = v1.addEdge("test", v2);

        assertEquals(v1.id(), edge.vertices(OUT).next().id());
        assertEquals(v2.id(), edge.vertices(IN).next().id());
    }

    @Test
    public void testGetEdgesInTx() throws Exception {
        Vertex v1 = graph.addVertex();
        Vertex v2 = graph.addVertex();
        Edge edge = v1.addEdge("test", v2);

        assertEquals(edge.id(), v1.edges(OUT).next().id());
        assertEquals(edge.id(), v2.edges(IN).next().id());
    }

    @Test
    public void testRemoveEdge() throws Exception {
        Vertex v1 = graph.addVertex();
        Vertex v2 = graph.addVertex();
        Edge edge = v1.addEdge("test", v2);
        assertEquals(1L, Iterators.size(graph.edges()));
        assertEquals(v1.id(), edge.vertices(OUT).next().id());
        assertEquals(v2.id(), edge.vertices(IN).next().id());
        graph.tx().commit();
        edge.remove();
        assertEquals(0L, Iterators.size(graph.edges()));
    }

    @Test
    public void testAddProperties() throws Exception {
        Vertex v1 = graph.addVertex("oid", 1, "foo", "bar");
        assertEquals(1, v1.property("oid").value());
        assertEquals(1, v1.properties("oid").next().value());
        assertEquals("bar", v1.property("foo").value());
        assertEquals("bar", v1.properties("foo").next().value());
        graph.tx().commit();
        assertEquals(1, v1.property("oid").value());
        assertEquals(1, v1.properties("oid").next().value());
        assertEquals("bar", v1.property("foo").value());
        assertEquals("bar", v1.properties("foo").next().value());
    }

    @Test
    public void testAddVerticesInMultipleTx() throws Exception {
        Vertex v1 = graph.addVertex();
        v1.property("foo", "bar");
        graph.tx().commit();
        Vertex v2 = graph.addVertex();
        v2.property("foo", "bar");
        graph.tx().commit();
        Vertex v3 = graph.addVertex();
        v3.property("foo", "bar");
        graph.tx().commit();
        v1.addEdge("knows", v2);
        v2.addEdge("knows", v3);
        graph.tx().commit();

        assertEquals(3L, Iterators.size(graph.vertices()));
        Vertex v4 = graph.addVertex();
        assertEquals(4L, Iterators.size(graph.vertices()));
        graph.tx().rollback();
        assertEquals(3L, Iterators.size(graph.vertices()));
        assertFalse(graph.vertices(v4.id()).hasNext());

        assertEquals(v3, v1.vertices(OUT, "knows").next().vertices(OUT, "knows").next());
    }

    private int treeBranchSize = Integer.parseInt(System.getProperty("tinkermic-gremlin.smoketest.treeBranchSize", "3"));

    @Test
    public void testTreeConnectivity() {
        Vertex start = graph.addVertex();
        setupTree(treeBranchSize, start);

        graph.tx().commit();
        testTreeIteration(treeBranchSize, start);
    }

    @Test
    public void testTreeConnectivityInTransaction() {
        Vertex start = graph.addVertex();
        setupTree(treeBranchSize, start);
        testTreeIteration(treeBranchSize, start);
    }

    private void testTreeIteration(int branchSize, Vertex start) {
        assertEquals(0, count(start.edges(IN)));
        assertEquals(branchSize, count(start.edges(OUT)));

        for (Edge e : copyOf(start.edges(OUT))) {
            assertEquals("test1", e.label());
            assertEquals(branchSize, count(e.vertices(IN).next().edges(OUT)));
            assertEquals(1, count(e.vertices(IN).next().edges(IN)));
            for (Edge f : copyOf(e.vertices(IN).next().edges(OUT))) {
                assertEquals("test2", f.label());
                assertEquals(branchSize, count(f.vertices(IN).next().edges(OUT)));
                assertEquals(1, count(f.vertices(IN).next().edges(IN)));
                for (Edge g : copyOf(f.vertices(IN).next().edges(OUT))) {
                    assertEquals("test3", g.label());
                    assertEquals(0, count(g.vertices(IN).next().edges(OUT)));
                    assertEquals(1, count(g.vertices(IN).next().edges(IN)));
                }
            }
        }

        int totalVertices = 0;
        for (int i = 0; i < 4; i++) {
            totalVertices = totalVertices + (int) Math.pow(branchSize, i);
        }

        Set<Vertex> vertices = new HashSet<>();
        vertices.addAll(copyOf(graph.vertices()));
        assertEquals(totalVertices, vertices.size());

        Set<Edge> edges = new HashSet<>();
        edges.addAll(copyOf(graph.edges()));
        assertEquals(totalVertices - 1, edges.size());
    }

    private void setupTree(int branchSize, Vertex start) {
        for (int i = 0; i < branchSize; i++) {
            Vertex a = graph.addVertex();
            start.addEdge("test1", a);
            for (int j = 0; j < branchSize; j++) {
                Vertex b = graph.addVertex();
                a.addEdge("test2", b);
                for (int k = 0; k < branchSize; k++) {
                    Vertex c = graph.addVertex();
                    b.addEdge("test3", c);
                }
            }
        }
    }
}

