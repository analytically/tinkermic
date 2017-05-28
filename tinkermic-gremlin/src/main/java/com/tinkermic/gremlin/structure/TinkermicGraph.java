package com.tinkermic.gremlin.structure;

import com.google.common.base.CharMatcher;
import com.tinkermic.gremlin.process.traversal.strategy.optimization.DatomicGraphStepStrategy;
import datomic.*;
import org.apache.commons.configuration.*;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.stream.Stream;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.VertexTest$BasicVertexTest",
        method = "shouldNotGetConcurrentModificationException",
        reason = "Fails occasionally on Travis CI due to threading issues on low-powered VM's"
)
@Graph.OptIn("com.tinkermic.gremlin.DatomicStrategySuite")
public class TinkermicGraph implements Graph {
    static {
        TraversalStrategies.GlobalCache.registerStrategies(
                TinkermicGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(DatomicGraphStepStrategy.instance()));
    }

    public static final String DATOMIC_DB_URI = "tinkermic.datomic.uri";
    static final String DATOMIC_EXCEPTION_MESSAGE = "An error occured within the Datomic datastore.";

    private static final CharMatcher LABEL_MATCHER = CharMatcher.inRange('0', '9')
            .or(CharMatcher.inRange('a', 'z'))
            .or(CharMatcher.inRange('A', 'Z'))
            .or(CharMatcher.inRange('0', '9'))
            .precomputed();

    private final BaseConfiguration configuration = new BaseConfiguration();
    private final Connection connection;
    private final Graph.Features features = new TinkermicFeatures();
    private final TinkermicHelper helper = new TinkermicHelper();

    private final ThreadLocal<Long> asOfTransaction = new ThreadLocal<Long>() {
        protected Long initialValue() {
            return null;
        }
    };

    private final TinkermicTransaction transaction;

    public TinkermicGraph(Configuration configuration) {
        this.configuration.copy(configuration);

        String dbUri = configuration.getString(DATOMIC_DB_URI);

        // it is not strictly necessary to call create-database if the database already exists, but it is safe
        // to do so—create-database is idempotent and will return false if one already exists
        Peer.createDatabase(dbUri);
        connection = Peer.connect(dbUri);
        transaction = new TinkermicTransaction(this, connection);

        // Setup the meta model for the graph
        if (requiresMetaModel(connection.db())) {
            try {
                helper().loadMetaModel(connection);
                connection.transact(Util.list(Util.map(":db/id", Peer.tempid(":db.part/tx"), ":db/txInstant", new Date(0))));
            } catch (Exception e) {
                throw new RuntimeException(DATOMIC_EXCEPTION_MESSAGE, e);
            }
        }
    }

    /**
     * This method is the one use by the {@link GraphFactory} to instantiate {@link Graph} instances. This method must
     * be implemented for the Structure Test Suite to pass.
     */
    public static TinkermicGraph open(Configuration configuration) {
        if (null == configuration) throw Graph.Exceptions.argumentCanNotBeNull("configuration");
        if (!configuration.containsKey(DATOMIC_DB_URI))
            throw new IllegalArgumentException(String.format("The tinkermic-gremlin configuration requires that the %s be set", DATOMIC_DB_URI));
        return new TinkermicGraph(configuration);
    }

    public static TinkermicGraph open(String dbUri) {
        if (null == dbUri) throw Graph.Exceptions.argumentCanNotBeNull("dbUri");
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(DATOMIC_DB_URI, dbUri);
        return open(config);
    }

    public static TinkermicGraph open() {
        return open("datomic:mem://tutorial");
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public Graph.Features features() {
        return features;
    }

    @Override
    public TinkermicTransaction tx() {
        return transaction;
    }

    @Override
    public Graph.Variables variables() {
        throw Exceptions.variablesNotSupported();
    }

    public TinkermicHelper helper() {
        return helper;
    }

    /**
     * This implementation of {@code close} will also close the current transaction on the the thread, but it
     * is up to the caller to deal with dangling transactions in other threads prior to calling this method.
     */
    @Override
    public void close() throws Exception {
        if (tx().isOpen()) tx().commit();
    }

    /**
     * Shutdown all Datomic peer resources. This method should be called as part of clean shutdown
     * of a JVM process. Will release all Connections, and will release Clojure resources.
     */
    public void shutdown() throws Exception {
        close();
        Peer.shutdown(true);
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();
        String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        if (!LABEL_MATCHER.matchesAllOf(label)) throw labelIllegalSymbol(label);

        tx().readWrite();

        // Create the new vertex
        UUID uuid = Peer.squuid();
        TinkermicHelper.Addition addition = helper().vertexAddition(uuid, label);
        TinkermicVertex vertex = new TinkermicVertex(this, Optional.empty(), uuid, addition.tempId, label);
        tx().add(vertex, addition.statements.get(0));

        ElementHelper.attachProperties(vertex, keyValues);
        return vertex;
    }

    protected Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Edge.Exceptions.userSuppliedIdsNotSupported();
        if (!LABEL_MATCHER.matchesAllOf(label)) throw labelIllegalSymbol(label);

        tx().readWrite();

        // Create the new edge
        UUID uuid = Peer.squuid();
        TinkermicVertex out = (TinkermicVertex) outVertex;
        TinkermicVertex in = (TinkermicVertex) inVertex;
        TinkermicHelper.Addition addition = helper().edgeAddition(uuid, label, out.graphId, in.graphId);
        TinkermicEdge edge = new TinkermicEdge(this, Optional.empty(), uuid, addition.tempId, label);
        tx().add(edge, addition.statements.get(0), out, in);

        ElementHelper.attachProperties(edge, keyValues);
        return edge;
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        if (0 == vertexIds.length) {
            tx().readWrite();

            Iterable<List<Object>> vertices = helper().listVertices(database());
            return IteratorUtils.stream(vertices)
                    .map(v -> (Vertex) new TinkermicVertex(this, Optional.of(database()), (UUID) v.get(1), v.get(0), (String) v.get(2))).iterator();
        } else {
            ElementHelper.validateMixedElementIds(Vertex.class, vertexIds);

            tx().readWrite();
            return Stream.of(vertexIds)
                    .map(TinkermicUtil::externalIdToUuid)
                    .flatMap(uuid -> {
                        try {
                            List<Object> v = helper().getVertex(database(), uuid);
                            return Stream.of(new TinkermicVertex(this, Optional.of(database()), uuid, v.get(0), (String) v.get(2)));
                        } catch (NoSuchElementException e) {
                            return Stream.empty();
                        }
                    })
                    .map(vertex -> (Vertex) vertex).iterator();
        }
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        if (0 == edgeIds.length) {
            tx().readWrite();

            Iterable<List<Object>> edges = helper().listEdges(database());
            return IteratorUtils.stream(edges)
                    .map(e -> (Edge) new TinkermicEdge(this, Optional.of(database()), (UUID) e.get(1), e.get(0), (String) e.get(2))).iterator();
        } else {
            ElementHelper.validateMixedElementIds(Edge.class, edgeIds);

            tx().readWrite();
            return Stream.of(edgeIds)
                    .map(TinkermicUtil::externalIdToUuid)
                    .flatMap(uuid -> {
                        try {
                            List<Object> e = helper().getEdge(database(), uuid);
                            return Stream.of(new TinkermicEdge(this, Optional.of(database()), uuid, e.get(0), (String) e.get(2)));
                        } catch (NoSuchElementException e) {
                            return Stream.empty();
                        }
                    })
                    .map(edge -> (Edge) edge).iterator();
        }
    }

    final Database databaseAsOf(Object transaction) {
        Database database = tx().getDatabase();
        if (transaction == null) {
            return database;
        }
        return database.asOf(transaction);
    }

    public final Database database() {
        if (asOfTransaction.get() != null) {
            return databaseAsOf(asOfTransaction.get());
        }
        return databaseAsOf(null);
    }

    Connection connection() {
        return connection;
    }

    protected void removeEdge(Edge e) {
        tx().readWrite();

        TinkermicEdge edge = (TinkermicEdge) e;
        if (tx().newInThisTx(edge)) {
            tx().remove(edge);
        } else {
            tx().del(edge, Util.list(":db.fn/retractEntity", edge.graphId));
        }
    }

    protected void removeVertex(Vertex v) {
        tx().readWrite();

        TinkermicVertex vertex = (TinkermicVertex) v;
        if (tx().newInThisTx(vertex)) {
            tx().remove(vertex);
        } else {
            // Retrieve all edges associated with this vertex and remove them one by one
            Iterator<Edge> edgesIt = vertex.edges(Direction.BOTH);
            while (edgesIt.hasNext()) {
                Edge edge = edgesIt.next();
                removeEdge(edge);
            }
            tx().del(vertex, Util.list(":db.fn/retractEntity", vertex.graphId));
        }
    }

    // Helper method to check whether the meta model of the graph still needs to be setup
    private boolean requiresMetaModel(Database database) {
        return Peer.q("[:find ?entity :in $ :where [?entity :db/ident :graph.element/type]]", database).isEmpty();
    }

    private boolean inMemoryDatomicDatabase() {
        return configuration.getString(DATOMIC_DB_URI).startsWith("datomic:mem:");
    }

    @Override
    public String toString() {
        if (inMemoryDatomicDatabase()) {
            return StringFactory.graphString(this,
                    configuration.getString(DATOMIC_DB_URI)
                            + ", vertices:" + IteratorUtils.count(vertices())
                            + ", edges:" + IteratorUtils.count(edges()));
        }
        return StringFactory.graphString(this, configuration.getString(DATOMIC_DB_URI));
    }

    public static class TinkermicFeatures implements Features {
        private final GraphFeatures graphFeatures = new TinkermicGraphFeatures();
        private final VertexFeatures vertexFeatures = new TinkermicVertexFeatures();
        private final EdgeFeatures edgeFeatures = new TinkermicEdgeFeatures();

        @Override
        public GraphFeatures graph() {
            return graphFeatures;
        }

        @Override
        public VertexFeatures vertex() {
            return vertexFeatures;
        }

        @Override
        public EdgeFeatures edge() {
            return edgeFeatures;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }

        public class TinkermicGraphFeatures implements GraphFeatures {
            private final VariableFeatures variableFeatures = new TinkermicVariableFeatures();

            TinkermicGraphFeatures() {
            }

            @Override
            public boolean supportsConcurrentAccess() {
                return false;
            }

            @Override
            public boolean supportsComputer() {
                return false;
            }

            @Override
            public VariableFeatures variables() {
                return variableFeatures;
            }

            @Override
            public boolean supportsThreadedTransactions() {
                return false;
            }
        }

        public static class TinkermicVariableFeatures implements Graph.Features.VariableFeatures {
            @Override
            public boolean supportsBooleanValues() {
                return false;
            }

            @Override
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            public boolean supportsDoubleValues() {
                return false;
            }

            @Override
            public boolean supportsFloatValues() {
                return false;
            }

            @Override
            public boolean supportsIntegerValues() {
                return false;
            }

            @Override
            public boolean supportsLongValues() {
                return false;
            }

            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return false;
            }

            @Override
            public boolean supportsByteArrayValues() {
                return false;
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return false;
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return false;
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return false;
            }

            @Override
            public boolean supportsStringArrayValues() {
                return false;
            }

            @Override
            public boolean supportsLongArrayValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsStringValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }
        }

        public class TinkermicVertexFeatures extends TinkermicElementFeatures implements VertexFeatures {
            private final VertexPropertyFeatures vertexPropertyFeatures = new TinkermicVertexPropertyFeatures();

            protected TinkermicVertexFeatures() {
            }

            @Override
            public VertexPropertyFeatures properties() {
                return vertexPropertyFeatures;
            }

            @Override
            public boolean supportsMetaProperties() {
                return false;
            }

            @Override
            public boolean supportsMultiProperties() {
                return false;
            }

            @Override
            public VertexProperty.Cardinality getCardinality(String key) {
                return VertexProperty.Cardinality.single;
            }
        }

        public class TinkermicEdgeFeatures extends TinkermicElementFeatures implements EdgeFeatures {
            private final EdgePropertyFeatures edgePropertyFeatures = new TinkermicEdgePropertyFeatures();

            TinkermicEdgeFeatures() {
            }

            @Override
            public EdgePropertyFeatures properties() {
                return edgePropertyFeatures;
            }
        }

        public class TinkermicElementFeatures implements ElementFeatures {
            TinkermicElementFeatures() {
            }

            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public boolean supportsNumericIds() {
                return false;
            }

            @Override
            public boolean supportsStringIds() {
                return false;
            }

            @Override
            public boolean supportsCustomIds() {
                return false;
            }

            @Override
            public boolean supportsAnyIds() {
                return false;
            }
        }

        public class TinkermicVertexPropertyFeatures implements VertexPropertyFeatures {
            TinkermicVertexPropertyFeatures() {
            }

            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }

            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public boolean supportsAnyIds() {
                return false;
            }

            @Override
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return false;
            }

            @Override
            public boolean supportsByteArrayValues() {
                return false;
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return false;
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return false;
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return false;
            }

            @Override
            public boolean supportsStringArrayValues() {
                return false;
            }

            @Override
            public boolean supportsLongArrayValues() {
                return false;
            }
        }

        public class TinkermicEdgePropertyFeatures implements EdgePropertyFeatures {
            TinkermicEdgePropertyFeatures() {
            }

            @Override
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return false;
            }

            @Override
            public boolean supportsByteArrayValues() {
                return false;
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return false;
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return false;
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return false;
            }

            @Override
            public boolean supportsStringArrayValues() {
                return false;
            }

            @Override
            public boolean supportsLongArrayValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }
        }
    }

    private static IllegalArgumentException labelIllegalSymbol(String label) {
        return new IllegalArgumentException("Label contains illegal symbol: " + label);
    }
}