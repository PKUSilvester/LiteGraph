package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * Created by wangzhenzhong on 16/8/5.
 */
public abstract class LiteElement implements Element {
    protected final Object id;
    protected final String label;
    protected boolean removed = false;
    public boolean properties;

    protected LiteElement(final Object id, final String label) {
        this.id = id;
        this.label = label;
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public String label() {
        return this.label;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    protected static IllegalStateException elementAlreadyRemoved(final Class<? extends Element> clazz, final Object id) {
        return new IllegalStateException(String.format("%s with id %s was removed.", clazz.getSimpleName(), id));
    }
    
    public boolean isRemoved()
    {
    	return removed;
    }
}
