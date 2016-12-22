package com.tinkermic.gremlin.structure;

import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;

public final class TinkermicVertexProperty<V> implements VertexProperty<V> {
    private final TinkermicVertex vertex;
    private final String key;
    private final V value;
    private boolean removed = false;

    TinkermicVertexProperty(TinkermicVertex vertex, String key, V value) {
        this.vertex = vertex;
        this.key = key;
        this.value = value;
    }

    @Override
    public Object id() {
        // TODO: we need a better ID system for VertexProperties
        return (long) (key.hashCode() + value.hashCode() + vertex.id().hashCode());
    }

    @Override
    public Vertex element() {
        return vertex;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() {
        if (!isPresent()) throw FastNoSuchElementException.instance();
        return value;
    }

    @Override
    public boolean isPresent() {
        return null != value;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public void remove() {
        if (removed) return;
        vertex.removeProperty(key);
        removed = true;
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
}