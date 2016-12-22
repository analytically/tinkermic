package com.tinkermic.gremlin;

import com.tinkermic.gremlin.structure.TinkermicGraph;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TinkermicOptimisticLockingTest {
    private TinkermicGraph graph;

    @Before
    public void setUp() throws Exception {
        graph = TinkermicGraph.open("datomic:mem://tinkermic-gremlin-locking-" + UUID.randomUUID());
    }

    @After
    public void tearDown() throws Exception {
        graph.close();
    }

    @Test
    public void testVertexVersionProperty() {
        Vertex v1 = graph.addVertex("_version", 1);
        graph.tx().commit();

        assertTrue(v1.property("_version").isPresent());
        assertEquals(1, v1.property("_version").value());

        v1.property("_version", 1234);
        graph.tx().commit();
        assertEquals(1234, v1.property("_version").value());

        v1.property("_version").remove();
        graph.tx().commit();

        assertFalse(v1.property("_version").isPresent());
    }

    @Test
    public void testVertexVersionPropertyCompareWithNil() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch commitLatch = new CountDownLatch(1);
        AtomicBoolean commitFailed = new AtomicBoolean(false);

        Vertex v1 = graph.addVertex("oid", 1);
        graph.tx().commit();

        Thread t1 = new Thread(() -> {
            v1.property("_version", 1);
            try {
                latch.await();
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
            graph.tx().commit();
            commitLatch.countDown();
        });

        Thread t2 = new Thread(() -> {
            v1.property("_version", 1);
            latch.countDown();
            try {
                commitLatch.await();
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
            try {
                graph.tx().commit();
            } catch (RuntimeException ex) {
                if (ex.getCause() instanceof AbstractTransaction.TransactionException) commitFailed.set(true);
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        assertTrue("commit should fail since we're using cas for the _version property", commitFailed.get());
    }

    @Test
    public void testEdgeVersionPropertyCompareWithNil() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch commitLatch = new CountDownLatch(1);
        AtomicBoolean commitFailed = new AtomicBoolean(false);

        Vertex v1 = graph.addVertex("oid", 1);
        Vertex v2 = graph.addVertex("oid", 1);
        Edge e = v1.addEdge("has", v2);
        graph.tx().commit();

        Thread t1 = new Thread(() -> {
            e.property("_version", 1);
            try {
                latch.await();
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
            graph.tx().commit();
            commitLatch.countDown();
        });

        Thread t2 = new Thread(() -> {
            e.property("_version", 1);
            latch.countDown();
            try {
                commitLatch.await();
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
            try {
                graph.tx().commit();
            } catch (RuntimeException ex) {
                if (ex.getCause() instanceof AbstractTransaction.TransactionException) commitFailed.set(true);
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        assertTrue("commit should fail since we're using cas for the _version property", commitFailed.get());
    }

    @Test
    public void testVertexVersionPropertyCompareWithValue() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch commitLatch = new CountDownLatch(1);
        AtomicBoolean commitFailed = new AtomicBoolean(false);

        Vertex v1 = graph.addVertex("oid", 1);
        v1.property("_version", 1);
        graph.tx().commit();

        Thread t1 = new Thread(() -> {
            v1.property("_version", 2);
            try {
                latch.await();
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
            graph.tx().commit();
            commitLatch.countDown();
        });

        Thread t2 = new Thread(() -> {
            v1.property("_version", 2);
            latch.countDown();
            try {
                commitLatch.await();
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
            try {
                graph.tx().commit();
            } catch (RuntimeException ex) {
                if (ex.getCause() instanceof AbstractTransaction.TransactionException) commitFailed.set(true);
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        assertTrue("commit should fail since we're using cas for the _version property", commitFailed.get());
    }

    @Test
    public void testEdgeVersionPropertyCompareWithValue() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch commitLatch = new CountDownLatch(1);
        AtomicBoolean commitFailed = new AtomicBoolean(false);

        Vertex v1 = graph.addVertex("oid", 1);
        Vertex v2 = graph.addVertex("oid", 1);
        Edge e = v1.addEdge("has", v2);
        e.property("_version", 1);
        graph.tx().commit();

        Thread t1 = new Thread(() -> {
            e.property("_version", 2);
            try {
                latch.await();
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
            graph.tx().commit();
            commitLatch.countDown();
        });

        Thread t2 = new Thread(() -> {
            e.property("_version", 2);
            latch.countDown();
            try {
                commitLatch.await();
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
            try {
                graph.tx().commit();
            } catch (RuntimeException ex) {
                if (ex.getCause() instanceof AbstractTransaction.TransactionException) commitFailed.set(true);
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        assertTrue("commit should fail since we're using cas for the _version property", commitFailed.get());
    }
}