package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.Util.Utility;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.structure.Direction.BOTH;
import static org.apache.tinkerpop.gremlin.tinkergraph.Util.Utility.*;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.HugeZset.queryAllEdge;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.HugeZset.queryEdge;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.RedisHelper.*;

/**
 * Created by wangzhenzhong on 16/8/5.
 */
public final class LiteVertex extends LiteElement implements Vertex {
    private final LiteGraph graph;
    protected Map<String, List<VertexProperty>> properties;

    protected LiteVertex(final Object id, final String label, final LiteGraph graph) {
        super(id, label);
        this.graph = graph;
        this.properties = new HashMap<String, List<VertexProperty>>();
        
        loadVertexPropertiesFromRedis(this);
        
    }

    /*
    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, this.id);
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = graph.edgeIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));

        final Edge edge;
        if (null != idValue) {
            if (graph.edges.containsKey(idValue))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = graph.edgeIdManager.getNextId(graph);
        }
        // 1. test. add keyValues to properties in memory
        edge = new LiteEdge(idValue, label, this, inVertex);
        if (keyValues.length != 0) {
            Property property = new LiteProperty(edge, keyValues[0].toString(), keyValues[1]);
            LiteEdge liteEdge = (LiteEdge) edge;
            liteEdge.properties = new HashMap<>();
            liteEdge.properties.put(keyValues[0].toString(), property);
        }
        insertEdgeIntoRedis((LiteEdge) edge, keyValues);
        return edge;
    }
*/
    
    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, this.id);
    //    ElementHelper.validateLabel(label);
    //    ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = graph.edgeIdManager.getNextId(graph);

        LiteEdge edge = new LiteEdge(idValue, label, this, inVertex, false);
      //  edge.properties = new HashMap<>();
        
        if (keyValues.length != 0) {
        for (int i = 0; i < keyValues.length; i = i + 2) {
        	//edge.property(keyValues[i].toString(), keyValues[i+1]);
        	Property newProperty = new LiteProperty(edge, keyValues[i].toString(), keyValues[i+1]);
            edge.properties.put(keyValues[i].toString(),newProperty);
        }
        }
        insertEdgeIntoRedis((LiteEdge) edge, keyValues);
        return edge;
    }
    
    public Edge addEdge(boolean allowDup, String label, Vertex inVertex, Object... keyValues) {
        if(allowDup)
        {
        	return addEdge(label, inVertex, keyValues);
        }
    	
    	if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, this.id);
    //    ElementHelper.validateLabel(label);
    //    ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = graph.edgeIdManager.getNextId(graph);

        LiteEdge edge = new LiteEdge(idValue, label, this, inVertex, false);
      //  edge.properties = new HashMap<>();
        
        if (keyValues.length != 0) {
        for (int i = 0; i < keyValues.length; i = i + 2) {
        	//edge.property(keyValues[i].toString(), keyValues[i+1]);
        	Property newProperty = new LiteProperty(edge, keyValues[i].toString(), keyValues[i+1]);
            edge.properties.put(keyValues[i].toString(),newProperty);
        }
        }
        insertEdgeIntoRedis((LiteEdge) edge, keyValues);
        return edge;
    }
    
    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (this.removed) return VertexProperty.empty();
    //    if (this.properties == null || this.properties.isEmpty()) loadVertexPropertiesFromRedis(this);
        if (this.properties != null && this.properties.containsKey(key)) {
            final List<VertexProperty> list = (List) this.properties.get(key);
            if (list.size() == 1) {
                return list.get(0);
            } else {
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            }
        } else
            return VertexProperty.<V>empty();
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, id);
    //    ElementHelper.legalPropertyKeyValueArray(keyValues);
    //    ElementHelper.validateProperty(key, value);
        final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
        if (optionalVertexProperty.isPresent()) return optionalVertexProperty.get();

        final VertexProperty<V> vertexProperty = new LiteVertexProperty(this, key, value, keyValues);// later:property ignore
        //new TinkerVertexProperty<V>(idValue, this, key, value);

      //  if (null == this.properties) this.properties = new HashMap<>();
        final List<VertexProperty> list = new ArrayList<>();//this.properties.getOrDefault(key, new ArrayList<>());

        list.add(vertexProperty);
        this.properties.put(key, list);

        if (keyValues.length != 0)
            ElementHelper.attachProperties(vertexProperty, keyValues);

        insertProperty(VERTEX_PROPERTIES + this.id(), key, value);
        if (keyValues.length != 0) {
            for (int i = 0; i < keyValues.length; i += 2) {
                insertProperty(VERTEX_PROPERTIES + this.id(), keyValues[i].toString(), keyValues[i + 1]);
            }
        }
        //insertProperties(this, keyValues);

        return vertexProperty;

    }
    
    /*
    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, id);
    //    if (this.properties == null || this.properties.isEmpty()) loadVertexPropertiesFromRedis(this);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        final Optional<Object> optionalId = ElementHelper.getIdValue(keyValues);
        final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
        if (optionalVertexProperty.isPresent()) return optionalVertexProperty.get();


        final Object idValue = optionalId.isPresent() ?
                graph.vertexPropertyIdManager.convert(optionalId.get()) :
                graph.vertexPropertyIdManager.getNextId(graph);

        final VertexProperty<V> vertexProperty = null;// later:property ignore
        //new TinkerVertexProperty<V>(idValue, this, key, value);

        if (null == this.properties) this.properties = new HashMap<>();
        final List<VertexProperty> list = this.properties.getOrDefault(key, new ArrayList<>());

        list.add(vertexProperty);
        this.properties.put(key, list);

        if (keyValues.length != 0)
            ElementHelper.attachProperties(vertexProperty, keyValues);

        insertProperty(VERTEX_PROPERTIES + this.id(), key, value);
        if (keyValues.length != 0) {
            for (int i = 0; i < keyValues.length; i += 2) {
                insertProperty(VERTEX_PROPERTIES + this.id(), keyValues[i].toString(), keyValues[i + 1]);
            }
        }
        //insertProperties(this, keyValues);

        return vertexProperty;

    }
    */

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        //todo 6. code 10 buffer
        Iterator<Edge> iterator = new HashSet<Edge>().iterator();//null;
        if (edgeLabels.length == 0) {
            iterator = HugeZset.queryAllEdge(direction, this.graph, this.id.toString(), Utility.VERTEX_EDGE+this.id);
        	/*
            switch (direction) {
                case IN:
                    iterator = queryAllEdge(this.graph, this.id.toString(), VERTEX_EDGE + this.id);
                case OUT:
                    iterator = queryAllEdge(this.graph, this.id.toString(), VERTEX_EDGE + this.id);
                default:
                    iterator = queryAllEdge(this.graph, this.id.toString(), VERTEX_EDGE + this.id);
                    iterator = IteratorUtils.concat(iterator, queryAllEdge(this.graph, this.id.toString(), VERTEX_EDGE + this.id));
            }
            */
        } else {
            for (String label : edgeLabels) {
            	switch (direction) {
                    case IN:
                    	iterator = IteratorUtils.concat(iterator, queryEdge(this.graph, this.id.toString(), VERTEX_EDGE + this.id, String.valueOf(ADD_SIGN) + DOT + label));
                    case OUT://todo
                    	iterator = IteratorUtils.concat(iterator, queryEdge(this.graph, this.id.toString(), VERTEX_EDGE + this.id, String.valueOf(MINUS_SIGN) + DOT + label));
                    default:
                    	iterator = IteratorUtils.concat(iterator, queryEdge(this.graph, this.id.toString(), VERTEX_EDGE + this.id, String.valueOf(ADD_SIGN) + DOT + label));
                        iterator = IteratorUtils.concat(iterator, queryEdge(this.graph, this.id.toString(), VERTEX_EDGE + this.id, String.valueOf(MINUS_SIGN) + DOT + label));
                }
            }
        }
        return iterator;
    }


    @Override
    public Iterator<Vertex> vertices(Direction direction, String... vertexLabels) {
        Set<Vertex> vertexSet = new HashSet<Vertex>();
        Iterator<Edge> edgesIterator = this.edges(direction);
        while (edgesIterator.hasNext()) {
            Edge edge = edgesIterator.next();
            String edgeLabel = edge.label();
            for (String vertexLabel : vertexLabels) {
                if (edgeLabel.compareTo(vertexLabel) == 0) {
                    if (this.id == edge.inVertex().id()) {//this==inVertex
                        vertexSet.add(edge.outVertex());
                    } else {
                        vertexSet.add(edge.inVertex());
                    }
                }
            }
        }

        return vertexSet.iterator();
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public void remove() {
        //get all edges
        Iterator<Edge> iterator = this.edges(BOTH);
        while (iterator.hasNext()) {
            //remove all edges
            LiteEdge liteEdge = (LiteEdge) iterator.next();
            liteEdge.remove();
        }
        removeVertexFromAllVertex(this);
        //remove Vertex property
        removeVertexProperties(this);
        //remove Vertex Info
        removeVertexInformation(this);
        this.removed = true;
    }


    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        if (this.removed) return Collections.emptyIterator();

   //     if (this.properties == null || this.properties.isEmpty()) loadVertexPropertiesFromRedis(this);
        
        if (null == this.properties) return Collections.emptyIterator();// later:property ignore
        if (propertyKeys.length == 1) {
            final List<VertexProperty> properties = this.properties.getOrDefault(propertyKeys[0], Collections.emptyList());
            if (properties.size() == 1) {
                return IteratorUtils.of(properties.get(0));
            } else if (properties.isEmpty()) {
                return Collections.emptyIterator();
            } else {
                return (Iterator) new ArrayList<>(properties).iterator();
            }
        } else
            return (Iterator) this.properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).flatMap(entry -> entry.getValue().stream()).collect(Collectors.toList()).iterator();

    }

}
