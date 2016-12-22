package com.tinkermic.gremlin.structure;

import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public final class TinkermicProperty<V> implements Property<V> {
    private final TinkermicElement element;
    private final String key;
    private final V value;
    private boolean removed = false;

    TinkermicProperty(TinkermicElement element, String key, V value) {
        this.element = element;
        this.key = key;
        this.value = value;
    }

    @Override
    public Element element() {
        return element;
    }

    @Override
    public void remove() {
        if (removed) return;
        element.removeProperty(key);
        removed = true;
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
    public boolean equals(Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
}