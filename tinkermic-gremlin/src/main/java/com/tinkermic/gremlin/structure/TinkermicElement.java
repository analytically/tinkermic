package com.tinkermic.gremlin.structure;

import com.google.common.base.CharMatcher;
import com.google.common.collect.Sets;
import datomic.Database;
import datomic.Entity;
import datomic.Peer;
import datomic.Util;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.threeten.extra.Interval;

import java.util.*;

public abstract class TinkermicElement implements Element {
    static final CharMatcher PROPERTY_KEY_MATCHER = CharMatcher.inRange('0', '9')
            .or(CharMatcher.inRange('a', 'z'))
            .or(CharMatcher.inRange('A', 'Z'))
            .or(CharMatcher.anyOf("_-"))
            .precomputed();

    final Optional<Database> database;
    final TinkermicGraph graph;
    final UUID uuid;
    Object graphId; // the datomic entity id
    final String label;
    boolean removed = false;

    private volatile int hashCode; // cache the hashcode of the UUID

    TinkermicElement(TinkermicGraph graph, Optional<Database> database, UUID uuid, Object graphId, String label) {
        assert graph != null;
        assert database != null;
        assert uuid != null;
        assert graphId != null;
        assert label != null;

        this.database = database;
        this.graph = graph;
        this.uuid = uuid; // UUID used to retrieve the actual datomic id later on
        this.graphId = graphId;
        this.label = label;
    }

    @Override
    public UUID id() {
        return uuid;
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Set<String> keys() {
        graph.tx().readWrite();

        if (graph.tx().newInThisTx(this)) {
            Set<String> properties = Sets.newHashSet();
            graph.tx().getPropertyKeys(this).stream()
                    .filter(key -> !TinkermicUtil.isReservedKey(key) && !Graph.Hidden.isHidden(key))
                    .forEach(key -> {
                        Optional<String> propertyName = TinkermicUtil.getPropertyName(key);
                        if (propertyName.isPresent()) {
                            properties.add(propertyName.get());
                        }
                    });
            return properties;
        } else {
            return graph.helper().getPropertyKeys(database(), graphId);
        }
    }

    void removeProperty(String key) {
        if (!PROPERTY_KEY_MATCHER.matchesAllOf(key)) throw propertyKeyIllegalSymbol(key);

        Object oldValue = property(key).value();
        String keyKeyword = TinkermicUtil.createKey(key, oldValue.getClass(), getClass());

        if (graph.tx().newInThisTx(this)) {
            graph.tx().removeProperty(this, keyKeyword);
        } else {
            graph.tx().mod(this, Util.list(":db/retract", graphId, keyKeyword, oldValue));
        }
    }

    @Override
    public boolean equals(Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        int result = hashCode;
        if (result == 0) {
            result = uuid.hashCode();
            hashCode = result;
        }
        return result;
    }

    protected Database database() {
        return database.orElse(graph.database());
    }

    static IllegalStateException elementRemoved(Class<? extends Element> clazz, Object id) {
        return new IllegalStateException(String.format("%s with id %s was removed.", clazz.getSimpleName(), id));
    }

    static IllegalArgumentException propertyKeyIllegalSymbol(String key) {
        return new IllegalArgumentException("Property key contains illegal symbol: " + key);
    }
}