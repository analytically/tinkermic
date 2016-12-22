package com.tinkermic.gremlin.structure;

import datomic.Database;
import datomic.Entity;
import datomic.Util;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

public final class TinkermicEdge extends TinkermicElement implements Edge {
    public TinkermicEdge(TinkermicGraph tinkermicGraph, Optional<Database> database, UUID uuid, Object graphId, String label) {
        super(tinkermicGraph, database, uuid, graphId, label);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        if (removed) return Collections.emptyIterator();

        graph.tx().readWrite();

        switch (direction) {
            case OUT:
                List<Object> outVertex = graph.helper().getOutVertex(database(), uuid);
                return IteratorUtils.of(new TinkermicVertex(graph, database, (UUID) outVertex.get(1), outVertex.get(0), (String) outVertex.get(2)));
            case IN:
                List<Object> inVertex = graph.helper().getInVertex(database(), uuid);
                return IteratorUtils.of(new TinkermicVertex(graph, database, (UUID) inVertex.get(1), inVertex.get(0), (String) inVertex.get(2)));
            default:
                Database queryDb = database();
                List<Object> out = graph.helper().getOutVertex(queryDb, uuid);
                List<Object> in = graph.helper().getInVertex(queryDb, uuid);
                return IteratorUtils.of(new TinkermicVertex(graph, database, (UUID) out.get(1), out.get(0), (String) out.get(2)),
                        new TinkermicVertex(graph, database, (UUID) in.get(1), in.get(0), (String) in.get(2)));
        }
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
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
                    .map(key -> (Property<V>) new TinkermicProperty<>(this, TinkermicUtil.getPropertyName(key).get(), (V) entity.get(key)))
                    .iterator();
        }
    }

    @Override
    public <V> Property<V> property(String key) {
        if (removed) return Property.empty();
        if (!PROPERTY_KEY_MATCHER.matchesAllOf(key)) throw propertyKeyIllegalSymbol(key);

        graph.tx().readWrite();

        if (graph.tx().newInThisTx(this)) {
            Map statements = graph.tx().getStatements(this);
            for (Object statementKey : statements.keySet()) {
                Optional<String> propertyName = TinkermicUtil.getPropertyName(statementKey.toString());
                if (propertyName.isPresent() && key.equals(propertyName.get())) {
                    return new TinkermicProperty<>(this, key, (V) statements.get(statementKey));
                }
            }
            return Property.empty();
        } else {
            Entity entity = database().entity(graphId);
            for (String property : entity.keySet()) {
                Optional<String> propertyName = TinkermicUtil.getPropertyName(property);
                if (propertyName.isPresent() && key.equals(propertyName.get())) {
                    return new TinkermicProperty<>(this, key, (V) entity.get(property));
                }
            }
            return Property.empty();
        }
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        if (removed) throw elementRemoved(Edge.class, id());
        ElementHelper.validateProperty(key, value);
        if (!PROPERTY_KEY_MATCHER.matchesAllOf(key)) throw propertyKeyIllegalSymbol(key);

        graph.tx().readWrite();

        // If the property does not exist yet, create the attribute definition
        graph.tx().addEdgeAttribute(key, value.getClass());

        String keyKeyword = TinkermicUtil.createKey(key, value.getClass(), Edge.class);
        if (graph.tx().newInThisTx(this)) {
            graph.tx().setProperty(this, keyKeyword, value);
        } else {
            // optimistic locking using Datomic's compare-and-swap function
            if (key.equals("_version")) {
                Property<V> existingProperty = property(key);

                graph.tx().mod(this, Util.list(":db.fn/cas", graphId, keyKeyword,
                        existingProperty.isPresent() ? existingProperty.value() : null, value));
            } else {
                graph.tx().mod(this, Util.map(":db/id", graphId, keyKeyword, value));
            }
        }

        return new TinkermicProperty<>(this, key, value);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public void remove() {
        if (removed) return;
        graph.removeEdge(this);
        removed = true;
    }
}
