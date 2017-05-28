package com.tinkermic.gremlin.structure;

import clojure.lang.Keyword;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import datomic.*;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.structure.Direction.IN;
import static org.apache.tinkerpop.gremlin.structure.Direction.OUT;

public final class TinkermicHelper {
    public static final Keyword ELEMENT_ID = Keyword.intern("graph.element/id");

    public static final Keyword EDGE_LABEL = Keyword.intern("graph.edge/label");
    public static final Keyword VERTEX_LABEL = Keyword.intern("graph.vertex/label");

    public static final Keyword IN_VERTEX = Keyword.intern("graph.edge/inVertex");
    public static final Keyword OUT_VERTEX = Keyword.intern("graph.edge/outVertex");

    public static class Addition {
        public final Object tempId;
        public final List statements;

        public Addition(Object tempId, List statements) {
            this.tempId = tempId;
            this.statements = statements;
        }

        public Addition withStatements(List statements) {
            return new Addition(tempId, Lists.newArrayList(
                    IteratorUtils.concat(this.statements.iterator(), statements.iterator())));
        }
    }

    public TinkermicHelper() {
    }

    /**
     * Load the graph's meta model, specified in the datomic-graph-schema.edn resource.
     *
     * @return The connection's transaction data
     * @throws Exception
     */
    public Map loadMetaModel(Connection connection) throws Exception {
        String statements = Resources.toString(Resources.getResource("datomic-graph-schema.edn"), Charsets.UTF_8);
        return loadStatements(connection, statements);
    }

    /**
     * Load a file containing a set of datoms into the database.
     *
     * @param statements A statements string
     * @return The connection's transaction data
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public Map loadStatements(Connection connection, String statements) throws IOException, ExecutionException, InterruptedException {
        List statementList = (List) Util.readAll(new StringReader(statements)).get(0);
        return connection.transact(statementList).get();
    }

    /**
     * Install a set of property definitions in the graph
     *
     * @param properties  A mapping from property name to the a Java
     *                    class representing the property's type
     * @param elementType A class assignable to either Vertex or Edge
     * @return The connection's transaction data
     */
    public Map installElementProperties(Connection connection, Map<String, Class> properties,
            Class<?> elementType) throws ExecutionException, InterruptedException {
        ArrayList<Object> statements = Lists.newArrayList();
        for (Map.Entry<String, Class> prop : properties.entrySet()) {
            statements.add(Util.map(":db/id", Peer.tempid(":db.part/db"),
                    ":db/ident", TinkermicUtil.createKey(prop.getKey(), prop.getValue(), elementType),
                    ":db/valueType", TinkermicUtil.javaTypeToDatomicType(prop.getValue()),
                    ":db/cardinality", ":db.cardinality/one"));
        }
        return connection.transact(statements).get();
    }

    /**
     * Obtain an iterable of vertex data, comprising a pair of the internal
     * graph ID and the element's UUID.
     *
     * @return An iterable of ID-UUID pairs
     */
    public Iterable<List<Object>> listVertices(Database db) {
        return Peer.q("[:find ?v ?uuid ?label :in $ :where " +
                "[?v :graph.element/id ?uuid] " +
                "[?v :graph.vertex/label ?label]]",
                db);
    }

    public List<Object> getVertex(Database db, UUID id) {
        return Peer.q("[:find ?v ?uuid ?label :in $ ?uuid :where " +
                "[?v :graph.element/id ?uuid] " +
                "[?v :graph.vertex/label ?label]]",
                db, id).iterator().next();
    }

    public List<Object> getEdge(Database db, UUID id) {
        return Peer.q("[:find ?v ?uuid ?label :in $ ?uuid :where " +
                "[?v :graph.element/id ?uuid] " +
                "[?v :graph.edge/label ?label]]",
                db, id).iterator().next();
    }

    public Iterable<List<Object>> listEdges(Database db) {
        return Peer.q("[:find ?v ?uuid ?label :in $ :where " +
                "[?v :graph.element/id ?uuid] " +
                "[?v :graph.edge/label ?label]]",
                db);
    }

    /**
     * Fetch the internal ID for an element given its UUID.
     *
     * @param uuid The external UUID of the element
     * @return The entity's interal ID
     * @throws NoSuchElementException
     */
    public Object idFromUuid(Database db, UUID uuid) throws NoSuchElementException {
        Iterator<Datom> iterator = db.datoms(Database.AVET, ELEMENT_ID, uuid).iterator();
        if (iterator.hasNext()) {
            return iterator.next().e();
        } else {
            throw new NoSuchElementException(uuid.toString());
        }
    }

    public UUID uuidFromId(Database db, Object id) throws NoSuchElementException {
        Iterator<Datom> iterator = db.datoms(Database.EAVT, id, ELEMENT_ID).iterator();
        if (iterator.hasNext()) {
            return (UUID) iterator.next().v();
        } else {
            throw new NoSuchElementException(id.toString());
        }
    }

    /**
     * Fetch an entity given its UUID.
     *
     * @param uuid The external UUID of the element
     * @return The entity
     * @throws NoSuchElementException
     */
    public Entity entityFromUuid(Database db, UUID uuid) throws NoSuchElementException {
        return db.entity(idFromUuid(db, uuid));
    }

    /**
     * Get all out vertices connected with a vertex.
     *
     * @param vertexId The vertex ID
     * @param labels   The labels to follow
     * @return An iterable of ID/Uuid pairs
     */
    public Iterator<List<Object>> getOutVertices(Database db, Object vertexId, String... labels) {
        return getVertices(db, vertexId, OUT, labels);
    }

    /**
     * Get all in vertices connected with a vertex.
     *
     * @param vertexId The vertex ID
     * @param labels   The labels to follow
     * @return An iterable of ID/Uuid pairs
     */
    public Iterator<List<Object>> getInVertices(Database db, Object vertexId, String... labels) {
        return getVertices(db, vertexId, IN, labels);
    }

    /**
     * Get a vertex's edges for in, out, or both directions, with the given label(s).
     *
     * @param vertexId  The vertex ID
     * @param direction The direction
     * @param labels    The label(s)
     * @return An iterable of ID/Uuid edge pairs
     */
    public Iterator<List<Object>> getEdges(Database db, Object vertexId, Direction direction, String... labels) {
        List<String> labelList = Arrays.asList(labels);
        switch (direction) {
            case IN:
                Set<Entity> inVertices = (Set<Entity>) db.entity(vertexId).get(":graph.edge/_inVertex");
                if (inVertices == null) return Collections.emptyIterator();
                else {
                    return inVertices.stream()
                            .filter(e -> labelList.isEmpty() || labelList.contains(e.get(":graph.edge/label")))
                            .map(e -> list(
                                    e.get(":db/id"),
                                    e.get(":graph.element/id"),
                                    e.get(":graph.edge/label")))
                            .iterator();
                }
            case OUT:
                Set<Entity> outVertices = (Set<Entity>) db.entity(vertexId).get(":graph.edge/_outVertex");
                if (outVertices == null) return Collections.emptyIterator();
                else {
                    return outVertices.stream()
                            .filter(e -> labelList.isEmpty() || labelList.contains(e.get(":graph.edge/label")))
                            .map(e -> list(
                                    e.get(":db/id"),
                                    e.get(":graph.element/id"),
                                    e.get(":graph.edge/label")))
                            .iterator();
                }
            default:
                return IteratorUtils.concat(getEdges(db, vertexId, OUT, labels), getEdges(db, vertexId, IN, labels));
        }
    }

    private static List<Object> list(Object... items) {
        if (items == null) {
            return Collections.emptyList();
        } else {
            ArrayList list = new ArrayList(items.length);
            Collections.addAll(list, items);
            return Collections.unmodifiableList(list);
        }
    }

    /**
     * Get a vertex's edges for in, out, or both directions, with the given label(s).
     *
     * @param vertexId  The vertex ID
     * @param direction The direction
     * @param labels    The label(s)
     * @return An iterable of ID/Uuid edge pairs
     */
    public Iterator<List<Object>> getEdgesByUUID(Database db, UUID vertexId, Direction direction, String... labels) {
        switch (direction) {
            case OUT:
            case IN:
                return Peer.q("[:find ?e ?uuid ?label" +
                                " :in $ ?vuuid ?dir " + (labels.length > 0 ? "[?label ...] " : "") +
                                " :where [?v :graph.element/id ?vuuid] " +
                                " [?e ?dir ?v]" +
                                " [?e :graph.edge/label ?label]" +
                                " [?e :graph.element/id ?uuid] ]",
                        db, vertexId, directionKeyword(direction), labels).iterator();
            default:
                return Peer.q("[:find ?e ?uuid ?label" +
                                " :in $ ?vuuid " + (labels.length > 0 ? "[?label ...] " : "") +
                                " :where [?v :graph.element/id ?vuuid] " +
                                " (or [?e :graph.edge/inVertex ?v] [?e :graph.edge/outVertex ?v])" +
                                " [?e :graph.edge/label ?label]" +
                                " [?e :graph.element/id ?uuid] ]",
                        db, vertexId, labels).iterator();
        }
    }

    private Keyword directionKeyword(Direction direction) {
        switch (direction) {
            case OUT: return OUT_VERTEX;
            case IN: return IN_VERTEX;
            default: throw new UnsupportedOperationException();
        }
    }

    /**
     * Get all in vertices connected with a vertex.
     *
     * @param vertexId The vertex ID
     * @param labels   The label(s) to follow
     * @return An iterable of ID/Uuid pairs
     */
    public Iterator<List<Object>> getVertices(Database db, Object vertexId, Direction direction, String... labels) {
        // This is REALLY crap right now...
        switch (direction) {
            case OUT:
            case IN:
                return vertexQuery(db, vertexId,
                        directionKeyword(direction), directionKeyword(direction.opposite()), labels);
            default:
                return IteratorUtils.concat(
                        getInVertices(db, vertexId), getOutVertices(db, vertexId));
        }
    }

    private Iterator<List<Object>> vertexQuery(Database db, Object vertexId, Keyword dir1, Keyword dir2,
            String... labels) {
        return Peer.q("[:find ?other ?uuid ?dir1 ?label" +
                " :in $ ?v ?dir1 ?dir2 " + (labels.length > 0 ? "[?label ...] " : "") +
                " :where [?e ?dir2 ?other] " +
                " [?e ?dir1 ?v]" +
                " [?e :graph.edge/label ?label]" +
                " [?other :graph.element/id ?uuid] ]",
                db, vertexId, dir1, dir2, labels).iterator();
    }

    /**
     * Get all in vertices connected with a vertex.
     *
     * @param vertexId The vertex ID
     * @param labels   The label(s) to follow
     * @return An iterable of ID/Uuid pairs
     */
    public Iterator<List<Object>> getVerticesByUuid(Database db, UUID vertexId, Direction direction,
            String... labels) {
        switch (direction) {
            case OUT:
            case IN:
                return vertexQueryByUuid(db, vertexId, directionKeyword(direction),
                        directionKeyword(direction.opposite()), labels);
            default:
                return IteratorUtils.concat(
                        getVerticesByUuid(db, vertexId, OUT, labels),
                        getVerticesByUuid(db, vertexId, IN, labels));
        }
    }

    private Iterator<List<Object>> vertexQueryByUuid(Database db, UUID vertexId, Keyword dir1,
                                                     Keyword dir2, String... labels) {
        return Peer.q("[:find ?other ?uuid ?dir1 ?label" +
                " :in $ ?vuuid ?dir1 ?dir2 " + (labels.length > 0 ? "[?label ...] " : "") +
                " :where [?e ?dir2 ?other] " +
                " [?v :graph.element/id ?vuuid ]" +
                " [?e ?dir1 ?v]" +
                " [?e :graph.edge/label ?label]" +
                " [?other :graph.element/id ?uuid] ]",
                db, vertexId, dir1, dir2, labels).iterator();
    }

    /**
     * Return an ID/UUID pair for a given edge's out vertex.
     *
     * @param edge An edge ID
     * @return An ID/UUID pair
     */
    public List<Object> getOutVertex(Database db, UUID edge) {
        return getVertex(db, edge, OUT_VERTEX);
    }

    /**
     * Return an ID/UUID pair for a given edge's in vertex.
     *
     * @param edge An edge ID
     * @return An ID/UUID pair
     */
    public List<Object> getInVertex(Database db, UUID edge) {
        return getVertex(db, edge, IN_VERTEX);
    }

    private List<Object> getVertex(Database db, UUID edge, Keyword direction) {
        return Peer.q("[:find ?v ?uuid ?label :in $ ?euuid ?d :where " +
                "[?e :graph.element/id ?euuid] " +
                "[?e ?d ?v] " +
                "[?v :graph.element/id ?uuid] " +
                "[?v :graph.vertex/label ?label]]",
                db, edge, direction).iterator().next();
    }

    /**
     * Fetch a property from an element
     *
     * @param uuid         The graph UUID
     * @param elementClass The class of the element, either Vertex or Edge
     * @param key          The property key
     * @param valueClass   The property's value class
     * @return The property value, or null if it doesn't exist
     */
    public Object getPropertyByUuid(Database db, UUID uuid, Class elementClass, String key, Class valueClass) {
        String keyName = TinkermicUtil.createKey(key, valueClass, elementClass);
        Collection<List<Object>> lists
                = Peer.q("[:find ?p :in $ ?euuid ?k :where [?e ?k ?p] [?e :graph.element/id ?euuid ]]",
                db, uuid, keyName);
        Iterator<List<Object>> iterator = lists.iterator();
        return iterator.hasNext() ? iterator.next().get(0) : null;
    }

    public Object getPropertyByUuid(Database db, UUID uuid, String key) {
        Entity entity = db.entity(idFromUuid(db, uuid));
        if (!TinkermicUtil.isReservedKey(key)) {
            for (String property : entity.keySet()) {
                Optional<String> propertyName = TinkermicUtil.getPropertyName(property);
                if (propertyName.isPresent()) {
                    if (key.equals(propertyName.get())) {
                        return entity.get(property);
                    }
                }
            }
            // We didn't find the value
            return null;
        } else {
            return entity.get(key);
        }
    }

    /**
     * Fetch a property from an element
     *
     * @param id           The graph internal id
     * @param elementClass The class of the element, either Vertex or Edge
     * @param key          The property key
     * @param valueClass   The property's value class
     * @return The property value, or null if it doesn't exist
     */
    public Object getProperty(Database db, Object id, Class elementClass, String key, Class valueClass) {
        String keyword = TinkermicUtil.createKey(key, valueClass, elementClass);
        Collection<List<Object>> lists = Peer.q("[:find ?p :in $ ?e ?k :where [?e ?k ?p]]",
                db, id, keyword);
        Iterator<List<Object>> iterator = lists.iterator();
        return iterator.hasNext() ? iterator.next().get(0) : null;
    }

    /**
     * Add a property to an id, returning the uncommitted statements.
     *
     * @param id           The graph internal ID
     * @param elementClass The class of the element, either Vertex or Edge
     * @param key          The property key
     * @param value        The property value
     * @return A set of database-altering statements, ready to be committed
     */
    public Addition addProperty(Database db, Object id, Class elementClass, String key, Object value) {
        return new Addition(id, Util.list(Util.map(":db/id", id,
                TinkermicUtil.createKey(key, value.getClass(), elementClass), value)));
    }

    /**
     * Add a property to an id, returning the uncommitted statements.
     *
     * @param uuid         The graph UUID
     * @param elementClass The class of the element, either Vertex or Edge
     * @param key          The property key
     * @param value        The property value
     * @return A set of database-altering statements, ready to be committed
     */
    public Addition addPropertyByUuid(Database db, UUID uuid, Class elementClass, String key, Object value) {
        return new Addition(uuid, Util.list(Util.map(":db/id", Keyword.intern(uuid.toString()),
                TinkermicUtil.createKey(key, value.getClass(), elementClass), value)));
    }

    /**
     * Remove a property from an id, returning the uncommitted statements.
     *
     * @param id           The graph internal ID
     * @param elementClass The class of the element, either Vertex or Edge
     * @param key          The property key
     * @param valueClass   The property's value class
     * @return A set of database-altering statements, ready to be committed
     */
    public List removeProperty(Database db, Object id, Class elementClass, String key, Class valueClass) {
        Object currentValue = getProperty(db, id, elementClass, key, valueClass);
        if (currentValue != null) {
            return Util.list(Util.list(":db/retract", id,
                    TinkermicUtil.createKey(key, valueClass, elementClass), currentValue));
        } else {
            return Util.list();
        }
    }

    /**
     * Remove a property from an id, returning the uncommitted statements.
     *
     * @param id           The graph internal ID
     * @param elementClass The class of the element, either Vertex or Edge
     * @param key          The property key
     * @param valueClass   The property's value class
     * @return A set of database-altering statements, ready to be committed
     */
    public List removePropertyByUuid(Database db, UUID id, Class elementClass, String key, Class valueClass) {
        Object currentValue = getPropertyByUuid(db, id, elementClass, key, valueClass);
        if (currentValue != null) {
            return Util.list(Util.list(":db/retract", Keyword.intern(id.toString()),
                    TinkermicUtil.createKey(key, valueClass, elementClass), currentValue));
        } else {
            return Util.list();
        }
    }

    public Set<String> getPropertyKeys(Database db, Object id) {
        Entity entity = db.entity(id);
        Set<String> filtered = Sets.newHashSet();
        for (String key : entity.keySet()) {
            if (!TinkermicUtil.isReservedKey(key) && !Graph.Hidden.isHidden(key)) {
                filtered.add(TinkermicUtil.getPropertyName(key).get());
            }
        }
        return filtered;
    }

    public Set<String> getPropertyKeysByUuid(Database db, UUID id) {
        Entity entity = db.entity(idFromUuid(db, id));
        Set<String> filtered = Sets.newHashSet();
        for (String property : entity.keySet()) {
            if (!TinkermicUtil.isReservedKey(property)) {
                filtered.add(TinkermicUtil.getPropertyName(property).get());
            }
        }
        return filtered;
    }

    public Addition vertexAddition(UUID uuid, String label) {
        Object tempId = Peer.tempid(":vertex");
        return new Addition(tempId, Util.list(Util.map(
                ":db/id", tempId,
                VERTEX_LABEL, label,
                ELEMENT_ID, uuid
        )));
    }

    public Addition edgeAddition(UUID uuid, String label, Object outVertex, Object inVertex) {
        Object tempid = Peer.tempid(":edge");
        return new Addition(tempid, Util.list(Util.map(
                ":db/id", tempid,
                EDGE_LABEL, label,
                OUT_VERTEX, outVertex,
                IN_VERTEX, inVertex,
                ELEMENT_ID, uuid
        )));
    }
}
