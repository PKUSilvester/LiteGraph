package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.tinkergraph.Util.Utility.EDGE_PROPERTIES;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.RedisHelper.*;

/**
 * Created by wangzhenzhong on 16/8/5.
 */
public final class LiteEdge extends LiteElement implements Edge {
    protected Map<String, Property> properties;
    public Vertex inVertex;
    public Vertex outVertex;

    protected LiteEdge(Object id, String label, Vertex outVertex, Vertex inVertex) {
        super(id, label);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        
        properties = new HashMap<>();
        loadEdgePropertiesFromRedis(this);
    }
    
    protected LiteEdge(Object id, String label, Vertex outVertex, Vertex inVertex, boolean isLoadProp) {
        super(id, label);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        
        properties = new HashMap<>();
        if(isLoadProp) loadEdgePropertiesFromRedis(this);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        if (removed) return Collections.emptyIterator();
        
        switch (direction) {
            case OUT:
                return IteratorUtils.of(this.outVertex);
            case IN:
                return IteratorUtils.of(this.inVertex);
            default:
                return IteratorUtils.of(this.outVertex, this.inVertex);
        }
    }

    @Override
    public Graph graph() {
        return this.graph();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        if (this.removed) throw elementAlreadyRemoved(Edge.class, id);
        ElementHelper.validateProperty(key, value);
     //   final Property oldProperty = super.property(key);
        final Property<V> newProperty = new LiteProperty<V>(this, key, value);
        this.properties.put(key,newProperty);//此处逻辑是不更新,直接替换
        insertProperty(EDGE_PROPERTIES + this.id, key, value);
        return newProperty;
    }

    @Override
    public <V> Property<V> property(final String key) {
    	
    	//return null == this.properties ? Property.<V>empty() : this.properties.getOrDefault(key, Property.<V>empty());
    	return this.properties.getOrDefault(key, Property.<V>empty());
    }

    @Override
    public void remove() {
        //remove VE records
        removeEdgeRelation(this);
        //remove Edge information and edge properties
        removeEdgePropertiesAndInformation(this);

        this.removed = true;
    }


    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
    	//if (null == this.properties) return Collections.emptyIterator();
    	if (this.properties.isEmpty()) return Collections.emptyIterator();
        if (propertyKeys.length == 1) {
            final Property<V> property = this.properties.get(propertyKeys[0]);
            return null == property ? Collections.emptyIterator() : IteratorUtils.of(property);
        } else
            return (Iterator) this.properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).map(entry -> entry.getValue()).collect(Collectors.toList()).iterator();

    }
}
