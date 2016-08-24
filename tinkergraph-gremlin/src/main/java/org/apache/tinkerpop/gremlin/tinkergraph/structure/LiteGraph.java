/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.Util.RedisResource;
import org.apache.tinkerpop.gremlin.tinkergraph.Util.Utility;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputerView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization.LiteGraphStepStrategy;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.tinkergraph.structure.HugeZset.queryEdgesFromRedisAsEdge;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.HugeZset.queryVerticesFromRedisAsVertex;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.RedisHelper.insertVertexIntoRedis;

/**
 * An in-memory (with optional persistence on calls to {@link #close()}), reference implementation of the property
 * graph interfaces provided by TinkerPop.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_PERFORMANCE)
public final class LiteGraph implements Graph {

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, LiteGraph.class.getName());
    }};

    /**
     * @deprecated As of release 3.1.0, replaced by {@link LiteGraph#GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER}
     */
    @Deprecated
    public static final String CONFIG_VERTEX_ID = "gremlin.tinkergraph.vertexIdManager";
    /**
     * @deprecated As of release 3.1.0, replaced by {@link LiteGraph#GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER}
     */
    @Deprecated
    public static final String CONFIG_EDGE_ID = "gremlin.tinkergraph.edgeIdManager";
    /**
     * @deprecated As of release 3.1.0, replaced by {@link LiteGraph#GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER}
     */
    @Deprecated
    public static final String CONFIG_VERTEX_PROPERTY_ID = "gremlin.tinkergraph.vertexPropertyIdManager";
    /**
     * @deprecated As of release 3.1.0, replaced by {@link LiteGraph#GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY}
     */
    @Deprecated
    public static final String CONFIG_DEFAULT_VERTEX_PROPERTY_CARDINALITY = "gremlin.tinkergraph.defaultVertexPropertyCardinality";
    /**
     * @deprecated As of release 3.1.0, replaced by {@link LiteGraph#GREMLIN_TINKERGRAPH_GRAPH_LOCATION}
     */
    @Deprecated
    public static final String CONFIG_GRAPH_LOCATION = "gremlin.tinkergraph.graphLocation";
    /**
     * @deprecated As of release 3.1.0, replaced by {@link LiteGraph#GREMLIN_TINKERGRAPH_GRAPH_FORMAT}
     */
    @Deprecated
    public static final String CONFIG_GRAPH_FORMAT = "gremlin.tinkergraph.graphFormat";
    ///////
    public static final String GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER = "gremlin.tinkergraph.vertexIdManager";
    public static final String GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER = "gremlin.tinkergraph.edgeIdManager";
    public static final String GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER = "gremlin.tinkergraph.vertexPropertyIdManager";
    public static final String GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY = "gremlin.tinkergraph.defaultVertexPropertyCardinality";
    public static final String GREMLIN_TINKERGRAPH_GRAPH_LOCATION = "gremlin.tinkergraph.graphLocation";
    public static final String GREMLIN_TINKERGRAPH_GRAPH_FORMAT = "gremlin.tinkergraph.graphFormat";

    private final TinkerGraphFeatures features = new TinkerGraphFeatures();

    protected AtomicLong currentId = new AtomicLong(-1l);
    protected Map<Object, Vertex> vertices = new ConcurrentHashMap<>();
    protected Map<Object, Edge> edges = new ConcurrentHashMap<>();

    protected TinkerGraphVariables variables = null;
    protected TinkerGraphComputerView graphComputerView = null;
    protected TinkerIndex<LiteVertex> vertexIndex = null;
    protected TinkerIndex<LiteEdge> edgeIndex = null;

    protected final IdManager<?> vertexIdManager;
    protected final IdManager<?> edgeIdManager;
    protected final IdManager<?> vertexPropertyIdManager;
    protected final VertexProperty.Cardinality defaultVertexPropertyCardinality;

    private final Configuration configuration;
    private final String graphLocation;
    private final String graphFormat;
    public static TreeMap<String, TreeMap<String, Lamp>> hugeZsetTreeMap = null;


    /**
     * An empty private constructor that initializes {@link LiteGraph}.
     */
    private LiteGraph(final Configuration configuration) {
        this.configuration = configuration;
        vertexIdManager = selectIdManager(configuration, GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, Vertex.class);
        edgeIdManager = selectIdManager(configuration, GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, Edge.class);
        vertexPropertyIdManager = selectIdManager(configuration, GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, VertexProperty.class);
        defaultVertexPropertyCardinality = VertexProperty.Cardinality.valueOf(
                configuration.getString(GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.single.name()));

        graphLocation = configuration.getString(GREMLIN_TINKERGRAPH_GRAPH_LOCATION, null);
        graphFormat = configuration.getString(GREMLIN_TINKERGRAPH_GRAPH_FORMAT, null);

        if ((graphLocation != null && null == graphFormat) || (null == graphLocation && graphFormat != null))
            throw new IllegalStateException(String.format("The %s and %s must both be specified if either is present",
                    GREMLIN_TINKERGRAPH_GRAPH_LOCATION, GREMLIN_TINKERGRAPH_GRAPH_FORMAT));

        if (graphLocation != null) loadGraph();
        
        if(hugeZsetTreeMap == null)
        {
        	hugeZsetTreeMap = new TreeMap<String, TreeMap<String, Lamp>>();
        	loadLamp();
        }
    }


    /**
     * Open a new {@link LiteGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> If a {@link Graph} implementation does not require a {@code Configuration}
     * (or perhaps has a default configuration) it can choose to implement a zero argument
     * {@code open()} method. This is an optional constructor method for TinkerGraph. It is not enforced by the Gremlin
     * Test Suite.
     */
    public static LiteGraph open() {
        
    	return open(EMPTY_CONFIGURATION);

    }

    /**
     * Open a new {@code TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> This method is the one use by the {@link GraphFactory} to instantiate
     * {@link Graph} instances.  This method must be overridden for the Structure Test Suite to pass. Implementers have
     * latitude in terms of how exceptions are handled within this method.  Such exceptions will be considered
     * implementation specific by the test suite as all test generate graph instances by way of
     * {@link GraphFactory}. As such, the exceptions get generalized behind that facade and since
     * {@link GraphFactory} is the preferred method to opening graphs it will be consistent at that level.
     *
     * @param configuration the configuration for the instance
     * @return a newly opened {@link Graph}
     */
    public static LiteGraph open(final Configuration configuration) {
        return new LiteGraph(configuration);
    }

    ////////////// STRUCTURE API METHODS //////////////////

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = vertexIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        if (null != idValue) {
            if (this.vertices.containsKey(idValue))
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = vertexIdManager.getNextId(this);
        }
        final Vertex vertex = new LiteVertex(idValue, label, this);
        insertVertexIntoRedis(vertex, this, keyValues);
        ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
        return vertex;
    }

    public static void loadLamp() {

        hugeZsetTreeMap = new TreeMap<String, TreeMap<String, Lamp>>();
        Set<String> set = RedisResource.jedis.zrange(Utility.ALLLAMP,0,-1);
        Iterator iterator = set.iterator();
        while (iterator.hasNext()) {
            String setName = (String) iterator.next();
            String hugeZsetTreeMapString = RedisHelper.split(setName)[1];
            TreeMap<String, Lamp> treeMap = new TreeMap<>();

            Set<String> lampSet = RedisResource.jedis.zrange(setName,0,-1);//

            Iterator lampSetIterator = lampSet.iterator();//
            while (lampSetIterator.hasNext()) {//
                String lampString = (String) lampSetIterator.next();//
                String[] temp = RedisHelper.split(lampString);

                int sit=lampString.length()-temp[temp.length-2].length()-temp[temp.length-1].length()-2;
                String minLamp=lampString.substring(0,sit);
      //          System.out.println(Utility.DEBUG+minLamp);
                Lamp lamp = new Lamp(minLamp, Long.valueOf(temp[temp.length-2]), temp[temp.length-1]);

                treeMap.put(lamp.minValue, lamp);//
            }
            hugeZsetTreeMap.put(hugeZsetTreeMapString, treeMap);
        }
        System.out.println(Utility.DEBUG+"----loadLamp,size="+hugeZsetTreeMap.size());
    }

    public static void saveLamp() {
        Iterator iterator = hugeZsetTreeMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, TreeMap<String, Lamp>> entry = (Map.Entry<String, TreeMap<String, Lamp>>) iterator.next();
            String jedisKey = Utility.HUGEZSETLAMP + Utility.DOT + entry.getKey();
            RedisResource.jedis.zadd(Utility.ALLLAMP,0, jedisKey);
            Iterator lampIterator = entry.getValue().entrySet().iterator();
            while (lampIterator.hasNext()) {
                Map.Entry<String, Lamp> jedisEntry = (Map.Entry<String, Lamp>) lampIterator.next();
                String field = jedisEntry.getKey();
                Lamp lamp = jedisEntry.getValue();
                String value = lamp.minValue + Utility.DOT + lamp.counter + Utility.DOT + lamp.next;
                RedisResource.jedis.zadd(jedisKey, 0, value);//
            }
        }
        System.out.println(Utility.DEBUG+"----saveLamp,size="+hugeZsetTreeMap.size());

    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        if (!graphComputerClass.equals(TinkerGraphComputer.class))
            throw Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass);
        //return (C) new TinkerGraphComputer(this);
        System.out.println("===Skip TinkerGraphComputer");
        return null;
    }

    @Override
    public GraphComputer compute() {
        System.out.println("===Skip TinkerGraphComputer 2");
        return null;
        //return new TinkerGraphComputer(this);
    }

    @Override
    public Variables variables() {
        if (null == this.variables)
            this.variables = new TinkerGraphVariables();
        return this.variables;
    }

    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        return (I) builder.graph(this).registry(TinkerIoRegistry.getInstance()).create();
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "vertices:" + this.vertices.size() + " edges:" + this.edges.size());
    }

    public void clear() {
        this.vertices.clear();
        this.edges.clear();
        this.variables = null;
        this.currentId.set(-1l);
        this.vertexIndex = null;
        this.edgeIndex = null;
        this.graphComputerView = null;
    }

    @Override
    public void close() {
        if (graphLocation != null) saveGraph();
        saveLamp();
        hugeZsetTreeMap = null;
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        //todo 7. buffer
        if(vertexIds.length == 0) return HugeZset.queryAllVertex(this, Utility.ALLVERTEX);
        
    	return queryVerticesFromRedisAsVertex(this, vertexIds);
        //return createElementIterator(Vertex.class, vertices, vertexIdManager, vertexIds);
    }


    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        //todo 8. buffer
    	if(edgeIds.length == 0)
    	{
    		Iterator iterator = this.vertices();
    		Iterator<Edge> finalIterator = null;
    		while(iterator.hasNext())
    		{
    			LiteVertex liteVertex = (LiteVertex) iterator.next();
    			finalIterator = IteratorUtils.concat(finalIterator, HugeZset.queryAllEdge(this, liteVertex.id.toString(), Utility.VERTEX_EDGE+liteVertex.id));
    		}
    		return finalIterator;
    	}
    	
        return queryEdgesFromRedisAsEdge(this, edgeIds);
        //return createElementIterator(Edge.class, edges, edgeIdManager, edgeIds);
    }


    private void loadGraph() {
    	final File f = new File(graphLocation);
        if (f.exists() && f.isFile()) {
            try {
                if (graphFormat.equals("graphml")) {
                    io(IoCore.graphml()).readGraph(graphLocation);
                } else if (graphFormat.equals("graphson")) {
                    io(IoCore.graphson()).readGraph(graphLocation);
                } else if (graphFormat.equals("gryo")) {
                    io(IoCore.gryo()).readGraph(graphLocation);
                } else {
                    io(IoCore.createIoBuilder(graphFormat)).readGraph(graphLocation);
                }
            } catch (Exception ex) {
                throw new RuntimeException(String.format("Could not load graph at %s with %s", graphLocation, graphFormat), ex);
            }
        }
    }

    private void saveGraph() {
    	
    	final File f = new File(graphLocation);
        if (f.exists()) {
            f.delete();
        } else {
            final File parent = f.getParentFile();
            if (!parent.exists()) {
                parent.mkdirs();
            }
        }

        try {
            if (graphFormat.equals("graphml")) {
                io(IoCore.graphml()).writeGraph(graphLocation);
            } else if (graphFormat.equals("graphson")) {
                io(IoCore.graphson()).writeGraph(graphLocation);
            } else if (graphFormat.equals("gryo")) {
                io(IoCore.gryo()).writeGraph(graphLocation);
            } else {
                io(IoCore.createIoBuilder(graphFormat)).writeGraph(graphLocation);
            }
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Could not save graph at %s with %s", graphLocation, graphFormat), ex);
        }
    }

/*    private <T extends Element> Iterator<T> createElementIterator(final Class<T> clazz, final Map<Object, T> elements,
                                                                  final IdManager idManager,
                                                                  final Object... ids) {
        final Iterator<T> iterator;
        if (0 == ids.length) {
            iterator = elements.values().iterator();
        } else {
            final List<Object> idList = Arrays.asList(ids);
            validateHomogenousIds(idList);

            // if the type is of Element - have to look each up because it might be an Attachable instance or
            // other implementation. the assumption is that id conversion is not required for detached
            // stuff - doesn't seem likely someone would detach a Titan vertex then try to expect that
            // vertex to be findable in OrientDB
            return clazz.isAssignableFrom(ids[0].getClass()) ?
                    IteratorUtils.filter(IteratorUtils.map(idList, id -> elements.get(clazz.cast(id).id())).iterator(), Objects::nonNull)
                    : IteratorUtils.filter(IteratorUtils.map(idList, id -> elements.get(idManager.convert(id))).iterator(), Objects::nonNull);
        }
        return TinkerHelper.inComputerMode(this) ?
                (Iterator<T>) (clazz.equals(Vertex.class) ?
                        IteratorUtils.filter((Iterator<Vertex>) iterator, t -> this.graphComputerView.legalVertex(t)) :
                        IteratorUtils.filter((Iterator<Edge>) iterator, t -> this.graphComputerView.legalEdge(t.outVertex(), t))) :
                iterator;
    }*/

    /**
     * Return TinkerGraph feature set.
     * <p/>
     * <b>Reference Implementation Help:</b> Implementers only need to implement features for which there are
     * negative or instance configured features.  By default, all
     * {@link Features} return true.
     */
    @Override
    public Features features() {
        return features;
    }

    private void validateHomogenousIds(final List<Object> ids) {
        final Iterator<Object> iterator = ids.iterator();
        Object id = iterator.next();
        if (id == null)
            throw Exceptions.idArgsMustBeEitherIdOrElement();
        final Class firstClass = id.getClass();
        while (iterator.hasNext()) {
            id = iterator.next();
            if (id == null || !id.getClass().equals(firstClass))
                throw Exceptions.idArgsMustBeEitherIdOrElement();
        }
    }

    public class TinkerGraphFeatures implements Features {

        private final TinkerGraphGraphFeatures graphFeatures = new TinkerGraphGraphFeatures();
        private final TinkerGraphEdgeFeatures edgeFeatures = new TinkerGraphEdgeFeatures();
        private final TinkerGraphVertexFeatures vertexFeatures = new TinkerGraphVertexFeatures();

        private TinkerGraphFeatures() {
        }

        @Override
        public GraphFeatures graph() {
            return graphFeatures;
        }

        @Override
        public EdgeFeatures edge() {
            return edgeFeatures;
        }

        @Override
        public VertexFeatures vertex() {
            return vertexFeatures;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }

    }

    public class TinkerGraphVertexFeatures implements Features.VertexFeatures {

        private final TinkerGraphVertexPropertyFeatures vertexPropertyFeatures = new TinkerGraphVertexPropertyFeatures();

        private TinkerGraphVertexFeatures() {
        }

        @Override
        public Features.VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return vertexIdManager.allow(id);
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return defaultVertexPropertyCardinality;
        }
    }

    public class TinkerGraphEdgeFeatures implements Features.EdgeFeatures {

        private TinkerGraphEdgeFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return edgeIdManager.allow(id);
        }
    }

    public class TinkerGraphGraphFeatures implements Features.GraphFeatures {

        private TinkerGraphGraphFeatures() {
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

    }

    public class TinkerGraphVertexPropertyFeatures implements Features.VertexPropertyFeatures {

        private TinkerGraphVertexPropertyFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return vertexIdManager.allow(id);
        }
    }

    ///////////// GRAPH SPECIFIC INDEXING METHODS ///////////////

    /**
     * Create an index for said element class ({@link Vertex} or {@link Edge}) and said property key.
     * Whenever an element has the specified key mutated, the index is updated.
     * When the index is created, all existing elements are indexed to ensure that they are captured by the index.
     *
     * @param key          the property key to index
     * @param elementClass the element class to index
     * @param <E>          The type of the element class
     */
    public <E extends Element> void createIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            //if (null == this.vertexIndex) this.vertexIndex = new TinkerIndex<>(this, TinkerVertex.class);
            this.vertexIndex.createKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            //if (null == this.edgeIndex) this.edgeIndex = new TinkerIndex<>(this, TinkerEdge.class);
            this.edgeIndex.createKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Drop the index for the specified element class ({@link Vertex} or {@link Edge}) and key.
     *
     * @param key          the property key to stop indexing
     * @param elementClass the element class of the index to drop
     * @param <E>          The type of the element class
     */
    public <E extends Element> void dropIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            if (null != this.vertexIndex) this.vertexIndex.dropKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            if (null != this.edgeIndex) this.edgeIndex.dropKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Return all the keys currently being index for said element class  ({@link Vertex} or {@link Edge}).
     *
     * @param elementClass the element class to get the indexed keys for
     * @param <E>          The type of the element class
     * @return the set of keys currently being indexed
     */
    public <E extends Element> Set<String> getIndexedKeys(final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return null == this.vertexIndex ? Collections.emptySet() : this.vertexIndex.getIndexedKeys();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return null == this.edgeIndex ? Collections.emptySet() : this.edgeIndex.getIndexedKeys();
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Construct an {@link LiteGraph.IdManager} from the TinkerGraph {@code Configuration}.
     */
    private static IdManager<?> selectIdManager(final Configuration config, final String configKey, final Class<? extends Element> clazz) {
        final String vertexIdManagerConfigValue = config.getString(configKey, DefaultIdManager.ANY.name());
        try {
            return DefaultIdManager.valueOf(vertexIdManagerConfigValue);
        } catch (IllegalArgumentException iae) {
            try {
                return (IdManager) Class.forName(vertexIdManagerConfigValue).newInstance();
            } catch (Exception ex) {
                throw new IllegalStateException(String.format("Could not configure TinkerGraph %s id manager with %s", clazz.getSimpleName(), vertexIdManagerConfigValue));
            }
        }
    }

    /**
     * TinkerGraph will use an implementation of this interface to generate identifiers when a user does not supply
     * them and to handle identifier conversions when querying to provide better flexibility with respect to
     * handling different data types that mean the same thing.  For example, the
     * {@link DefaultIdManager#LONG} implementation will allow {@code g.vertices(1l, 2l)} and
     * {@code g.vertices(1, 2)} to both return values.
     *
     * @param <T> the id type
     */
    public interface IdManager<T> {
        /**
         * Generate an identifier which should be unique to the {@link LiteGraph} instance.
         */
        T getNextId(final LiteGraph graph);

        /**
         * Convert an identifier to the type required by the manager.
         */
        T convert(final Object id);

        /**
         * Determine if an identifier is allowed by this manager given its type.
         */
        boolean allow(final Object id);
    }

    /**
     * A default set of {@link IdManager} implementations for common identifier types.
     */
    public enum DefaultIdManager implements IdManager {
        /**
         * Manages identifiers of type {@code Long}. Will convert any class that extends from {@link Number} to a
         * {@link Long} and will also attempt to convert {@code String} values
         */
        LONG {
            @Override
            public Long getNextId(final LiteGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof Long)
                    return id;
                else if (id instanceof Number)
                    return ((Number) id).longValue();
                else if (id instanceof String)
                    return Long.parseLong((String) id);
                else
                    throw new IllegalArgumentException(String.format("Expected an id that is convertible to Long but received %s", id.getClass()));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof Number || id instanceof String;
            }
        },

        /**
         * Manages identifiers of type {@code Integer}. Will convert any class that extends from {@link Number} to a
         * {@link Integer} and will also attempt to convert {@code String} values
         */
        INTEGER {
            @Override
            public Integer getNextId(final LiteGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).map(Long::intValue).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof Integer)
                    return id;
                else if (id instanceof Number)
                    return ((Number) id).intValue();
                else if (id instanceof String)
                    return Integer.parseInt((String) id);
                else
                    throw new IllegalArgumentException(String.format("Expected an id that is convertible to Integer but received %s", id.getClass()));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof Number || id instanceof String;
            }
        },

        /**
         * Manages identifiers of type {@link java.util.UUID}. Will convert {@code String} values to
         * {@link java.util.UUID}.
         */
        UUID {
            @Override
            public UUID getNextId(final LiteGraph graph) {
                return java.util.UUID.randomUUID();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof java.util.UUID)
                    return id;
                else if (id instanceof String)
                    return java.util.UUID.fromString((String) id);
                else
                    throw new IllegalArgumentException(String.format("Expected an id that is convertible to UUID but received %s", id.getClass()));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof UUID || id instanceof String;
            }
        },

        /**
         * Manages identifiers of any type.  This represents the default way {@link LiteGraph} has always worked.
         * In other words, there is no identifier conversion so if the identifier of a vertex is a {@code Long}, then
         * trying to request it with an {@code Integer} will have no effect. Also, like the original
         * {@link LiteGraph}, it will generate {@link Long} values for identifiers.
         */
        ANY {
            @Override
            public Long getNextId(final LiteGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                return id;
            }

            @Override
            public boolean allow(final Object id) {
                return true;
            }
        }
    }
}
