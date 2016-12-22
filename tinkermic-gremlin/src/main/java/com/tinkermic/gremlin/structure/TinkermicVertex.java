package com.tinkermic.gremlin.structure;

import datomic.*;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

import static org.apache.tinkerpop.gremlin.structure.Direction.IN;
import static org.apache.tinkerpop.gremlin.structure.Direction.OUT;

public final class TinkermicVertex extends TinkermicElement implements Vertex {
    public TinkermicVertex(TinkermicGraph tinkermicGraph, Optional<Database> database, UUID uuid, Object id, String label) {
        super(tinkermicGraph, database, uuid, id, label);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... labels) {
        graph.tx().readWrite();

        if (direction.equals(OUT)) {
            return getOutEdges(labels);
        } else if (direction.equals(IN))
            return getInEdges(labels);
        else {
            return IteratorUtils.concat(getInEdges(labels), getOutEdges(labels));
        }
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... labels) {
        Iterator<List<Object>> vertices = graph.helper().getVertices(database(), graphId, direction, labels);

        return IteratorUtils.stream(vertices)
                .map(vertex -> (Vertex) new TinkermicVertex(graph, database, (UUID) vertex.get(1), vertex.get(0), (String) vertex.get(3))).iterator();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (removed) throw elementRemoved(Vertex.class, id());
        return graph.addEdge(this, inVertex, label, keyValues);
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        return property(VertexProperty.Cardinality.single, key, value);
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        if (removed) throw elementRemoved(Vertex.class, id());
        ElementHelper.validateProperty(key, value);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();
        if (cardinality != VertexProperty.Cardinality.single)
            throw VertexProperty.Exceptions.multiPropertiesNotSupported();
        if (keyValues.length > 0)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        if (!PROPERTY_KEY_MATCHER.matchesAllOf(key)) throw propertyKeyIllegalSymbol(key);

        graph.tx().readWrite();

        // If the property does not exist yet, create the attribute definition
        graph.tx().addVertexAttribute(key, value.getClass());

        String keyKeyword = TinkermicUtil.createKey(key, value.getClass(), Vertex.class);
        if (graph.tx().newInThisTx(this)) {
            graph.tx().setProperty(this, keyKeyword, value);
        } else if (graph.tx().modInThisTx(this)) {
            graph.tx().setProperty(this, keyKeyword, value);
        } else {
            // optimistic locking using Datomic's compare-and-swap function
            if (key.equals("_version")) {
                VertexProperty<V> existingProperty = property(key);

                graph.tx().mod(this, Util.list(":db.fn/cas", graphId, keyKeyword,
                        existingProperty.isPresent() ? existingProperty.value() : null, value));
            } else {
                graph.tx().mod(this, Util.map(":db/id", graphId, keyKeyword, value));
            }
        }

        return new TinkermicVertexProperty<>(this, key, value);
    }

    @Override
    public <V> VertexProperty<V> property(String key) {
        if (!PROPERTY_KEY_MATCHER.matchesAllOf(key)) throw propertyKeyIllegalSymbol(key);

        graph.tx().readWrite();

        if (graph.tx().newInThisTx(this)) {
            Map statements = graph.tx().getStatements(this);
            for (Object statementKey : statements.keySet()) {
                Optional<String> propertyName = TinkermicUtil.getPropertyName(statementKey.toString());
                if (propertyName.isPresent() && propertyName.get().equals(key)) {
                    return new TinkermicVertexProperty<>(this, key, (V) statements.get(statementKey));
                }
            }
            return VertexProperty.empty();
        } else {
            Entity entity = database().entity(graphId);
            for (String property : entity.keySet()) {
                Optional<String> propertyName = TinkermicUtil.getPropertyName(property);
                if (propertyName.isPresent() && key.equals(propertyName.get())) {
                    return new TinkermicVertexProperty<>(this, key, (V) entity.get(property));
                }
            }
            return VertexProperty.empty();
        }
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        if (removed) return Collections.emptyIterator();

        graph.tx().readWrite();

        if (graph.tx().newInThisTx(this)) {
            Iterable<String> keys = keys();
            Iterator<String> filter = IteratorUtils.filter(keys.iterator(),
                    key -> ElementHelper.keyExists(key, propertyKeys));
            return IteratorUtils.map(filter, this::property);
        } else {
            Entity entity = database().entity(graphId);
            return entity.keySet().stream()
                    .filter(key -> ElementHelper.keyExists(TinkermicUtil.getPropertyName(key).get(), propertyKeys) && !TinkermicUtil.isReservedKey(key) && !Graph.Hidden.isHidden(key))
                    .map(key -> (VertexProperty<V>) new TinkermicVertexProperty<>(this, TinkermicUtil.getPropertyName(key).get(), (V) entity.get(key)))
                    .iterator();
        }
    }

    private Iterator<Edge> getInEdges(String... labels) {
        if (graph.tx().newInThisTx(this)) {
            return IteratorUtils.stream(graph.helper().getEdges(database(), graph.helper().idFromUuid(database(), id()), IN, labels))
                    .map(input -> (Edge) new TinkermicEdge(graph, database, (UUID) input.get(1), input.get(0), (String) input.get(2))).iterator();
        }
        return IteratorUtils.stream(graph.helper().getEdges(database(), graphId, IN, labels))
                .map(input -> (Edge) new TinkermicEdge(graph, database, (UUID) input.get(1), input.get(0), (String) input.get(2))).iterator();
    }

    private Iterator<Edge> getOutEdges(String... labels) {
        if (graph.tx().newInThisTx(this)) {
            return IteratorUtils.stream(graph.helper().getEdges(database(), graph.helper().idFromUuid(database(), id()), OUT, labels))
                    .map(input -> (Edge) new TinkermicEdge(graph, database, (UUID) input.get(1), input.get(0), (String) input.get(2))).iterator();
        }
        return IteratorUtils.stream(graph.helper().getEdges(database(), graphId, OUT, labels))
                .map(input -> (Edge) new TinkermicEdge(graph, database, (UUID) input.get(1), input.get(0), (String) input.get(2))).iterator();
    }

    @Override
    public void remove() {
        if (removed) return;
        graph.removeVertex(this);
        removed = true;
    }
}
