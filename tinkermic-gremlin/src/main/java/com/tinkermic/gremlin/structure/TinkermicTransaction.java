package com.tinkermic.gremlin.structure;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import datomic.Connection;
import datomic.Database;
import datomic.Peer;
import datomic.Util;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static datomic.Connection.DB_AFTER;
import static datomic.Connection.TEMPIDS;

/**
 * Transaction class that manages the mismatch between TinkerPop and Datomic transactional semantics. Basically this holds
 * a list of pending additions and retractions to the database in the form of Datomic statements. However, statements
 * in a pending Datomic transaction can not be "contradictory", that is, you can't add and retract a fact in the
 * same operation.
 * <p>
 * Tinkerpop, however, does allow adding and removing edges, vertices, and properties in the same transaction, so we
 * need to manage the set of pending statements in such a way that the pending queue does not contain a contradiction.
 */
public final class TinkermicTransaction extends AbstractThreadLocalTransaction {
    private enum OpType {
        add,
        del,
        mod
    }

    private static class Op {
        final OpType opType;
        Object statement;
        final TinkermicElement[] touched;

        public Op(OpType opType, Object statement, TinkermicElement... touched) {
            this.opType = opType;
            this.statement = statement;
            this.touched = touched;
        }

        public boolean concerns(TinkermicElement element) {
            for (TinkermicElement t : touched) {
                if (t.equals(element)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class TxContext {
        private Instant validTime = null;

        // List of pending graph operations
        private LinkedHashMap<UUID, Op> operations = Maps.newLinkedHashMap();

        // Map of vertex attributes to create the schema definition for
        private Map<String, Class> vertexAttributes = Maps.newHashMap();

        // Map of edge attributes to create the schema definition for
        private Map<String, Class> edgeAttributes = Maps.newHashMap();

        // List of dirty elements that need their temp IDs resolved
        // to permanent graph IDs following a commit.
        private final Map<Object, TinkermicElement> dirty = Maps.newHashMap();

        // Reverse lookup of dirty IDs
        private final Map<TinkermicElement, Object> revMap = Maps.newHashMap();

        private Database database;
    }

    private final ThreadLocal<TxContext> context = ThreadLocal.withInitial(() -> null);
    private final Connection connection;

    public TinkermicTransaction(Graph graph, Connection connection) {
        super(graph);
        this.connection = connection;
    }

    public Database getDatabase() {
        if (context.get().database == null) {
            List<Object> ops = ops(); // creates attributes
            context.get().database = (Database) connection.db().with(ops).get(Connection.DB_AFTER);
            return context.get().database;
        } else {
            return context.get().database;
        }
    }

    private void createAttributeDefinitions() {
        connection.transact(context.get().vertexAttributes.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(TinkermicUtil.createKey(entry.getKey(), entry.getValue(), Vertex.class), entry.getValue()))
                .filter(entry -> !TinkermicUtil.attributeDefinitionExists(entry.getKey(), connection))
                .map(entry -> {
                    String valueType = TinkermicUtil.javaTypeToDatomicType(entry.getValue());
                    return Util.map(":db/id", Peer.tempid(":db.part/db"),
                            ":db/ident", entry.getKey(),
                            ":db/valueType", valueType,
                            ":db/cardinality", ":db.cardinality/one",
                            ":db/index", true,
                            ":db.install/_attribute", ":db.part/db");
                }).collect(Collectors.toList()));

        context.get().vertexAttributes.clear();

        connection.transact(context.get().edgeAttributes.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(TinkermicUtil.createKey(entry.getKey(), entry.getValue(), Edge.class), entry.getValue()))
                .filter(entry -> !TinkermicUtil.attributeDefinitionExists(entry.getKey(), connection))
                .map(entry -> {
                    String valueType = TinkermicUtil.javaTypeToDatomicType(entry.getValue());
                    return Util.map(":db/id", Peer.tempid(":db.part/db"),
                            ":db/ident", entry.getKey(),
                            ":db/valueType", valueType,
                            ":db/cardinality", ":db.cardinality/one",
                            ":db/index", true,
                            ":db.install/_attribute", ":db.part/db");
                }).collect(Collectors.toList()));

        context.get().edgeAttributes.clear();
    }

    private void setDirty() {
        context.get().database = null;
    }

    public List<Object> ops() {
        createAttributeDefinitions();

        return Lists.newArrayList(
                Sets.newLinkedHashSet(context.get().operations.values().stream().map(op -> op.statement).collect(Collectors.toList())));
    }

    public void addVertexAttribute(String key, Class valueClass) {
        context.get().vertexAttributes.put(key, valueClass);
    }

    public void addEdgeAttribute(String key, Class valueClass) {
        context.get().edgeAttributes.put(key, valueClass);
    }

    public boolean newInThisTx(TinkermicElement element) {
        Op op = context.get().operations.get(element.id());
        return op != null && op.opType == OpType.add;
    }

    public boolean modInThisTx(TinkermicElement element) {
        Op op = context.get().operations.get(element.id());
        return op != null && op.opType == OpType.mod;
    }

    public void add(TinkermicElement element, Object statement, TinkermicElement... touched) {
        context.get().operations.put(element.id(), new Op(OpType.add, statement, touched));
        context.get().dirty.put(element.graphId, element);
        context.get().revMap.put(element, element.graphId);
        setDirty();
    }

    public void mod(TinkermicElement element, Object statement) {
        context.get().operations.put(element.id(), new Op(OpType.mod, statement));
        setDirty();
    }

    public void del(TinkermicElement element, Object statement) {
        if (newInThisTx(element)) {
            remove(element);
        } else {
            context.get().operations.put(element.id(), new Op(OpType.del, statement));
            setDirty();
        }
    }

    public void remove(TinkermicElement element) {
        context.get().operations.remove(element.id());
        Object o = context.get().revMap.get(element);
        if (o != null) {
            context.get().dirty.remove(o);
        }

        LinkedHashMap<UUID, Op> newMap = Maps.newLinkedHashMap();
        context.get().operations.entrySet().stream().filter(entry -> !entry.getValue().concerns(element)).forEach(entry -> {
            newMap.put(entry.getKey(), entry.getValue());
        });
        context.get().operations = newMap;
        setDirty();
    }

    void setProperty(TinkermicElement element, String key, Object value) {
        insertIntoStatement(context.get().operations.get(element.id()), key, value);
        setDirty();
    }

    void removeProperty(TinkermicElement element, String key) {
        removeFromStatementMap(context.get().operations.get(element.id()), key);
        setDirty();
    }

    Set<String> getPropertyKeys(TinkermicElement element) {
        if (newInThisTx(element)) {
            Set<String> keys = Sets.newHashSet();
            for (Object item : getStatementMap(context.get().operations.get(element.id())).keySet()) {
                if (item instanceof String) {
                    keys.add((String) item);
                } else {
                    keys.add(item.toString());
                }
            }
            return keys;
        }
        throw new IllegalArgumentException("Item is not added in current TX: " + element);
    }

    Map getStatements(TinkermicElement element) {
        if (newInThisTx(element)) {
            return getStatementMap(context.get().operations.get(element.id()));
        }
        throw new IllegalArgumentException("Item is not added in current TX: " + element);
    }

    @Override
    protected void doOpen() {
        context.set(new TxContext());
    }

    @Override
    protected void doCommit() throws TransactionException {
        try {
            Map transactResult = connection.transact(ops()).get();
            resolveIds((Database) transactResult.get(DB_AFTER), (Map) transactResult.get(TEMPIDS));
        } catch (InterruptedException | ExecutionException e) {
            throw new TransactionException(TinkermicGraph.DATOMIC_EXCEPTION_MESSAGE, e);
        } finally {
            context.remove();
        }
    }

    @Override
    protected void doRollback() throws TransactionException {
        context.remove();
    }

    @Override
    public boolean isOpen() {
        return context.get() != null;
    }

    private void insertIntoStatement(Op op, String key, Object value) {
        Map newMap = Maps.newHashMap(getStatementMap(op));
        newMap.put(key, value);
        op.statement = newMap;
    }

    private void removeFromStatementMap(Op op, String key) {
        Map newMap = Maps.newHashMap(getStatementMap(op));
        newMap.remove(key);
        op.statement = newMap;
    }

    private Map getStatementMap(Op op) {
        if (op.statement instanceof Map) {
            return (Map) op.statement;
        }
        throw new IllegalArgumentException("Statement was not a map: " + op.statement);
    }

    private void resolveIds(Database database, Map tempIds) {
        for (Map.Entry<Object, TinkermicElement> entry : context.get().dirty.entrySet()) {
            entry.getValue().graphId = Peer.resolveTempid(database, tempIds, entry.getKey());
        }
    }
}
