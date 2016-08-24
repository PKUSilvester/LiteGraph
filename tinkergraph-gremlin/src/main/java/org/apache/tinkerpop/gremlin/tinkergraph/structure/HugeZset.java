package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

import static org.apache.tinkerpop.gremlin.tinkergraph.Util.RedisResource.SETSKIPSIZE;
import static org.apache.tinkerpop.gremlin.tinkergraph.Util.RedisResource.jedis;
import static org.apache.tinkerpop.gremlin.tinkergraph.Util.Utility.*;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.LiteGraph.hugeZsetTreeMap;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.RedisHelper.split;

/**
 * Created by wangzhenzhong on 16/7/29.
 */
public class HugeZset {
    /**
     * Structures
     */
    public TreeMap<String, Lamp> lampTreeMap;
    public String zsetName;

    public TreeMap<String, Lamp> getTreeMap() {
        return lampTreeMap;
    }

    private static long getTailRank(String setname, String condition) {
        condition = condition + "~";
        jedis.zadd(HUGEZSETLAMP + DOT + setname, 0, condition);
        Long rank = jedis.zrank(HUGEZSETLAMP + DOT + setname, condition);
        jedis.zrem(HUGEZSETLAMP + DOT + setname, condition);
        return rank;
    }

    public static long getFirstRank(String setname, String condition) {
        jedis.zadd(HUGEZSETLAMP + DOT + setname, 0, condition);
        Long rank = jedis.zrank(HUGEZSETLAMP + DOT + setname, condition);
        jedis.zrem(HUGEZSETLAMP + DOT + setname, condition);
        return rank;
    }

    public static Lamp getLamp(String zsetName, String value) {
        long rank = getFirstRank(zsetName, value) - 1;
        Lamp lamp = null;
        if (rank >= 0) {
            lamp = getLampByRank(zsetName, value, rank);
        }
        //jedis.zrevrangeByLex(HUGEZSETLAMP + DOT + zsetName, value, value + TAIL, 1, 1);
        return lamp;
    }

    public static Lamp getLampByRank(String zsetName, String value, long rank) {
        Set<String> lampSet = jedis.zrange(HUGEZSETLAMP + DOT + zsetName, 0, -1);
        String lampString;
        Lamp lamp = null;
        Iterator<String> lampStringIterator = lampSet.iterator();
        while (lampStringIterator.hasNext()) {
            lampString = lampStringIterator.next();

            String[] temp = split(lampString);

            long counter = Long.valueOf(temp[temp.length - 2]);
            String next = temp[temp.length - 1];
            String minValue = lampString.substring(0, lampString.length() - 1 - (temp[temp.length - 1].length() + temp[temp.length - 2].length() + 1));

            lamp = new Lamp(minValue, counter, next);
        }
        return lamp;
    }


    public static void removeFromHugeZset(String zsetName, String value) {
        Lamp lamp = getLamp(zsetName, value);
        if (lamp != null) {
            jedis.zrem(zsetName + DOT + lamp.minValue, value);
        }
    }

    public static void updateLampCouter() {

    }

    public static void splitZset(String zsetName, Lamp oldLamp) {

        String firstString = getTheValueLocaledAtZset(SETSKIPSIZE / 2, zsetName + DOT + oldLamp.minValue);
        Set<String> newSet = jedis.zrange(zsetName + DOT + oldLamp.minValue, SETSKIPSIZE / 2, -1);
        Lamp newLamp = new Lamp(firstString, oldLamp.counter - SETSKIPSIZE / 2, oldLamp.next);
        hugeZsetTreeMap.get(zsetName).put(newLamp.minValue, newLamp);
        Iterator<String> iterator = newSet.iterator();
        while (iterator.hasNext()) {
            String value = iterator.next();
            jedis.zadd(zsetName + DOT + newLamp.minValue, 0, value);
        }
        jedis.zremrangeByRank(zsetName + DOT + oldLamp.minValue, SETSKIPSIZE / 2, -1);
        oldLamp.counter -= SETSKIPSIZE / 2;
    }

    public static Iterator<Vertex> queryVerticesFromRedisAsVertex(Graph graph, Object... vertexIds) {

        if (vertexIds.length != 0) {
            Set<Vertex> set = new HashSet<>();
            for (Object vertexID : vertexIds) {
                if (vertexID == null) continue;
                System.out.println(DEBUG + vertexID);
                Vertex vertex = getVertexFromRedis(graph, vertexID.toString());
                set.add(vertex);

            }
            return set.iterator();
        } else
            return ((Set)(new HashSet<>())).iterator();
    }

    public static Vertex getVertexFromRedis(Graph graph, String vertexID) {
        String vertexLabel = jedis.get(VERTEX_LABEL + vertexID);
        String value = vertexLabel + DOT + vertexID;
        
        try{
        	long tmpid = Long.parseLong(vertexID);
        } catch(Exception e)
        {
        	System.out.println("===err vid: "+vertexID);
        }
        
        try{
        	long tmpl = Long.parseLong(vertexID);
        } catch(Exception e)
        {
        	System.err.print("===err vid: "+vertexID);
        }
        
        LiteVertex vertex = new LiteVertex(Long.parseLong(vertexID), vertexLabel, (LiteGraph) graph);
        
    //    RedisHelper.loadVertexPropertiesFromRedis(vertex);
        
        //todo add properties
        return vertex;
    }

    public static Edge getEdgeFromRedis(Graph graph, String edgeID) {
        String Edgelabel = jedis.get(EDGE_LABEL + edgeID);
        String inVertexID = jedis.get(IN_VERTEX_ID + edgeID);
        String outVertexID = jedis.get(OUT_VERTEX_ID + edgeID);
        Vertex inVertex = getVertexFromRedis(graph, inVertexID);
        Vertex outVertex = getVertexFromRedis(graph, outVertexID);
        LiteEdge edge = new LiteEdge(Long.parseLong(edgeID), Edgelabel, outVertex, inVertex);
        
  //      RedisHelper.loadEdgePropertiesFromRedis(edge);
        
        //todo add properties
        return edge;
    }

    public static Iterator<Edge> queryEdgesFromRedisAsEdge(Graph graph, Object... edgeIds) {

        if (edgeIds.length != 0) {
            Set<Edge> set = new HashSet<>();
            for (Object edgeID : edgeIds) {
                System.out.println(DEBUG + edgeID);
                Edge edge = getEdgeFromRedis(graph, edgeID.toString());
                set.add(edge);
            }
            return set.iterator();
        } else

            return ((Set)(new HashSet<>())).iterator();
    }

    /*
    public static Iterator<Vertex> queryVertex(Graph graph, String zsetName, String condition) {

        Lamp lamp = getLamp(zsetName, condition);
        String setname = zsetName + DOT + lamp.minValue;
        long firstRank = getFirstRank(setname, condition);
        long lastRank = getTailRank(setname, condition);
        Set<Vertex> vertexSet = new HashSet<>();
        Set<String> vertexString = jedis.zrange(zsetName + DOT + lamp.minValue, firstRank, lastRank);
        Iterator<String> vertexStringIterator = vertexString.iterator();
        while (vertexStringIterator.hasNext()) {
            String vertexInfo = vertexStringIterator.next();
            Vertex vertex = queryVertexByID(split(vertexInfo)[1], graph);
            vertexSet.add(vertex);
        }
        return vertexSet.iterator();
    }

    public static Iterator<Edge> queryEdge(Graph graph, String vertexID, String zsetName, String condition) {
        Lamp lamp = getLamp(zsetName, condition);
        if (lamp == null) {
            return new ArrayList<Edge>().iterator();
        }

        String setname = zsetName + DOT + lamp.minValue;//todo
        long firstRank = getFirstRank(setname, condition);
        long lastRank = getTailRank(setname, condition);
        Set<Edge> edgeSet = new HashSet<>();
        Set<String> edgeString = jedis.zrange(zsetName + DOT + lamp.minValue, firstRank, lastRank);
        Iterator<String> edgeStringIterator = edgeString.iterator();
        Object edgeID;
        String edgeLabel;
        Vertex inVertex;
        Vertex outVertex;
        String[] temp;
        while (edgeStringIterator.hasNext()) {
            temp = split(edgeStringIterator.next());
            edgeID = temp[2];

            Edge edge = queryEdgeByID((String) edgeID, graph);//new LiteEdge(edgeID, edgeLabel, outVertex, inVertex);
            edgeSet.add(edge);
        }

        return edgeSet.iterator();
    }
*/

    public static Iterator<Edge> queryAllEdge(Graph graph, String vertexID, String zsetName) {

        Set<String> lampString = jedis.zrange(HUGEZSETLAMP + DOT + VERTEX_EDGE + vertexID, 0, -1);
        Set<Edge> edgeSet = new HashSet<>();

        Iterator<String> lampIterator = lampString.iterator();
        while (lampIterator.hasNext()) {
            String[] temp = split(lampIterator.next());
            Lamp lamp = new Lamp(temp[0], Long.valueOf(temp[1]), temp[2]);
            Set<String> allEdgeString = jedis.zrange(zsetName + DOT + lamp.minValue, 0, -1);
            Iterator<String> allEdgeStringIterator = allEdgeString.iterator();
            while (allEdgeStringIterator.hasNext()) {
                String[] edgeInfoTemp = split(allEdgeStringIterator.next());
                String edgeID = edgeInfoTemp[2];
                Edge edge = queryEdgeByID(edgeID, graph);
                edgeSet.add(edge);
            }
        }
        return edgeSet.iterator();
    }
    
    public static Iterator<Edge> queryAllEdge(Direction direction, Graph graph, String vertexID, String zsetName) {

        Set<String> lampString = jedis.zrange(HUGEZSETLAMP + DOT + VERTEX_EDGE + vertexID, 0, -1);
        Set<Edge> edgeSet = new HashSet<>();

        Iterator<String> lampIterator = lampString.iterator();
        while (lampIterator.hasNext()) {
            String[] temp = split(lampIterator.next());
            Lamp lamp = new Lamp(temp[0], Long.valueOf(temp[1]), temp[2]);
            Set<String> allEdgeString = jedis.zrange(zsetName + DOT + lamp.minValue, 0, -1);
            Iterator<String> allEdgeStringIterator = allEdgeString.iterator();
            while (allEdgeStringIterator.hasNext()) {
                String[] edgeInfoTemp = split(allEdgeStringIterator.next());
                String edgeID = edgeInfoTemp[2];
                Edge edge = queryEdgeByID(edgeID, graph);
                switch (direction) {
                    case IN:
                        if (edge.inVertex().id().toString().compareTo(vertexID) == 0) edgeSet.add(edge);
                    case OUT:
                        if (edge.outVertex().id().toString().compareTo(vertexID) == 0) edgeSet.add(edge);
                    default:
                        edgeSet.add(edge);
                }
            }
        }
        return edgeSet.iterator();
    }

    public static Vertex queryVertexByID(String vertexID, Graph graph) {
        String vertexLabel = jedis.get(VERTEX_LABEL + vertexID);
        LiteVertex vertex = new LiteVertex(Long.parseLong(vertexID), vertexLabel, ((LiteGraph) graph));
        
    //    RedisHelper.loadVertexPropertiesFromRedis(vertex);
        
        return vertex;
    }

    public static Edge queryEdgeByID(String edgeID, Graph graph) {
        String edgeLabel = jedis.get(EDGE_LABEL + edgeID);
        String inVertexID = jedis.get(IN_VERTEX_ID + edgeID);
        String outVertexID = jedis.get(OUT_VERTEX_ID + edgeID);
        
        Vertex inVertex = getVertexFromRedis(graph, inVertexID);
        Vertex outVertex = getVertexFromRedis(graph, outVertexID);
        
        LiteEdge edge = new LiteEdge(Long.parseLong(edgeID), edgeLabel, outVertex, inVertex);
        
  //      RedisHelper.loadEdgePropertiesFromRedis(edge);
        
        return edge;
    }

    public static Iterator<Vertex> queryAllVertexOld(Graph graph, String zsetName) {
        Set<String> lampString = jedis.zrange(HUGEZSETLAMP + DOT + zsetName, 0, -1);
        Set<Vertex> vertexSet = new HashSet<>();

        Iterator<String> lampIterator = lampString.iterator();
        while (lampIterator.hasNext()) {
            String[] temp = split(lampIterator.next());
            Lamp lamp = new Lamp(temp[0], Long.valueOf(temp[1]), temp[2]);
            Set<String> allVertexString = jedis.zrange(zsetName + DOT + lamp.minValue, 0, -1);
            Iterator<String> allVertexStringIterator = allVertexString.iterator();
            while (allVertexStringIterator.hasNext()) {
                String vertexID = split(allVertexStringIterator.next())[1];
                Vertex vertex = queryVertexByID(vertexID, graph);
                vertexSet.add(vertex);
            }
        }
        return vertexSet.iterator();
    }

    public static Iterator<Vertex> queryAllVertex(Graph graph, String zsetName) {
        Set<Vertex> vertexSet = new HashSet<>();

        TreeMap<String, Lamp> tmp = hugeZsetTreeMap.get(zsetName);
        if(tmp != null)
        {
        Iterator mapIterator = tmp.entrySet().iterator();
        while (mapIterator.hasNext()) {
            Map.Entry<String, Lamp> entry = (Map.Entry<String, Lamp>) mapIterator.next();
            Lamp lamp = entry.getValue();
        	
        	Set<String> allVertexString = jedis.zrange(zsetName + DOT + lamp.minValue, 0, -1);
            Iterator<String> allVertexStringIterator = allVertexString.iterator();
            while (allVertexStringIterator.hasNext()) {
                String vertexID = split(allVertexStringIterator.next())[1];
                Vertex vertex = queryVertexByID(vertexID, graph);
                vertexSet.add(vertex);
            }
        }
        }
        return vertexSet.iterator();
    }
    

    /**
     * Debug Tools
     */
/*    public void showHugeZsetInfo() {
        System.out.println(DEBUG + "zsetName=" + zsetName);
        Iterator iter = lampTreeMap.entrySet().iterator();
        while (iter.next()) {
            Map.Entry<Integer, Lamp> entry = (Map.Entry<Integer, Lamp>) iter.next();
            Lamp lamp = entry.getValue();
            System.out.print("\t\t");
            showLamp(lamp);
        }
        System.out.println("****************************LampTreeMap****************************");
    }*/
    public static void showLamp(Lamp newLamp) {
        if (newLamp != null)
            System.out.println(DEBUG + "lamp=" + newLamp.minValue + "\tnext=" + newLamp.next + "\tCounter=" + newLamp.counter);
    }

/*    public static void showSetInMemory(Set<String> set) {
        int i = 0;
        Iterator iterator = set.iterator();
        while (iterator.next()) {

            System.out.println(DEBUG + i + ":" + iterator.next());
            i++;
        }
    }*/

    /**
     * init
     */

    protected HugeZset(String zsetName) {
        this.zsetName = zsetName;
        //importLampsFromRedis(zsetName);
        //constructor
    }

    /**
     * TOOLS:
     * this value is the label.hashcode of vertex or edge
     * the return of funtion is the suitable Set lamp
     */
    private Lamp getItsLampOnlyForInsert(String value) {//this value is the label of vertex or edge

        if (this.getTreeMap().floorEntry(value) == null) {
            if (this.getTreeMap().ceilingEntry(value) != null) {

                Lamp ceilingLamp = this.getTreeMap().ceilingEntry(value).getValue();
                if (ceilingLamp.counter < SETSKIPSIZE) {
                    ceilingLamp.setMinValue(value);
                    return ceilingLamp;
                }
            }
            String hashnext;
            if (this.getTreeMap().ceilingEntry(value) == null) {
                hashnext = null;
            } else {
                hashnext = this.getTreeMap().ceilingEntry(value).getValue().minValue;
            }
            Lamp lamp = new Lamp(value, 0, null);
            this.getTreeMap().put(value, lamp);
            return lamp;
        }
        return this.getTreeMap().floorEntry(value).getValue();//
    }

    public Lamp getItsLamp(String value) {//this value is the label of vertex or edge

        if (this.getTreeMap().floorEntry(value) == null) {
            return null;
        }
        return this.getTreeMap().floorEntry(value).getValue();//
    }

    public Lamp getItsPreLamp(Lamp lamp) {
        if (this.getTreeMap().lowerEntry(lamp.minValue) == null) {
            return null;
        }
        return this.getTreeMap().lowerEntry(lamp.minValue).getValue();
    }


    public Lamp getItsNextLamp(Lamp lamp) {
        if (this.getTreeMap().higherEntry(lamp.minValue) == null) {
            return null;
        }
        return this.getTreeMap().higherEntry(lamp.minValue).getValue();
    }

    public Lamp getItsCeilingLamp(String value) {

        if (this.getTreeMap().ceilingEntry(value) == null) {
            return null;
        }
        return this.getTreeMap().ceilingEntry(value).getValue();//
    }

    private static String getTheValueLocaledAtZset(long rank, String setName) {

        String value = jedis.zrange(setName, rank, rank).iterator().next();
        return value;
    }

    public Object[] getSomeItemInRange(Lamp nowLamp, long currentRedisRange, long l) {
        Set bufferSet = jedis.zrange(zsetName + DOT + nowLamp.minValue, currentRedisRange, currentRedisRange + l);
        Object[] buffer = bufferSet.toArray();

        return buffer;
    }

    /**
     * Private action:
     * <p>
     * 0.lamp:(Object)min, (long)counter
     * 1.lampTreeMap: key=(Object)min.hashcode(), value=(lamp)lamp
     * 2.HugeZset{lampTreeMap,zsetName}
     * 3.(zset)AllVertex:score=0, value=label.Vid (score=sum,value=counter)
     * 4.(zset)VE{id}:score=0,value=pre.Elabel.Eid.Vid(score=sum,value=counter)
     * 5.(Hashmap)VP{id}:key=name,value=S/M.type.value.value.value......
     * 6.(Hashmap)EP{id}:key=name,value=S/M.type.value.value.value......
     */
    private TreeMap<String, Lamp> importLampsFromRedis(String zsetName) {
        debugInfoSwitch = false;
        lampTreeMap = new TreeMap<String, Lamp>();
        String redisString;
        //get lamp list
        Set<String> redisSet = jedis.zrange(HUGEZSETLAMP + DOT + zsetName, 0, -1);//这里的zsetName=原来set的Name,用来寻找原来set

        if (redisSet.isEmpty()) {
            System.out.println(WARNING + "没有lamp记录。");
        } else {
            Iterator<String> redisSetIterator = redisSet.iterator();
            while (redisSetIterator.hasNext()) {
                redisString = redisSetIterator.next();
                String temp[] = split(redisString);
                String min = temp[0];
                long counter = Long.valueOf(temp[1]);
                String next = temp[2];
                Lamp lamp = new Lamp(min, counter, next);
                lampTreeMap.put(min, lamp);
            }
        }
        debugInfoSwitch = true;
        return lampTreeMap;
    }


    /**
     * 将索引信息存到redis中,在关机时必须调用
     */
    private void saveLampsToRedis(TreeMap<String, Lamp> tempLamptreemap) {
        jedis.del(HUGEZSETLAMP + DOT + zsetName);
        //Map<Integer, String> finalMap = new HashMap<Integer, String>();
        Iterator iter = tempLamptreemap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer, Lamp> entry = (Map.Entry<Integer, Lamp>) iter.next();
            //finalMap.put(entry.getKey(),String.valueOf(entry.getValue().minValue)+DOT+entry.getValue().getMaxLamp()+DOT+entry.getValue().getCounter());
            jedis.zadd(HUGEZSETLAMP + DOT + zsetName, entry.getKey(), String.valueOf(entry.getValue().minValue) + DOT + entry.getValue().getCounter() + DOT + entry.getValue().next);

        }
        //jedis.zadd(HUGEZSETLAMP+DOT+zsetName,finalMap);
    }

    /**
     * Public Methods:
     */


    public void insertItem(String value) {
        Lamp lamp = getItsLampOnlyForInsert(value);
        //String minValue=lamp.minValue;
        lamp.counter++;
        if (lamp.getCounter() > SETSKIPSIZE) {
            lamp = splitZset(lamp);
        }

        jedis.zadd(zsetName + DOT + lamp.minValue, 0, value);

    }

    public void replaceItem(String oldValue, String newValue) {
        this.removeItem(oldValue);
        this.insertItem(newValue);
    }
    //  给定一个lamp,令其自动分裂,返回新Set的lamp,方便将该数据放进去

    private Lamp splitZset(Lamp oldLamp) {

        String firstString = getTheValueLocaledAtZset(SETSKIPSIZE / 2, zsetName + DOT + oldLamp.minValue);
        Set<String> newSet = jedis.zrange(zsetName + DOT + oldLamp.minValue, SETSKIPSIZE / 2, -1);
        Lamp newLamp = new Lamp(firstString, oldLamp.counter - SETSKIPSIZE / 2, oldLamp.next);
        Iterator<String> iterator = newSet.iterator();
        while (iterator.hasNext()) {
            String str = iterator.next();
            zaddValue(zsetName, newLamp, str);
        }
        jedis.zremrangeByRank(zsetName + DOT + oldLamp.minValue, SETSKIPSIZE / 2, -1);
        oldLamp.counter -= SETSKIPSIZE / 2;
        //oldLamp.next = true;
        this.getTreeMap().put(newLamp.minValue, newLamp);
        return newLamp;
    }

    private static void zaddValue(String zsetName, Lamp lamp, String value) {
        jedis.zadd(zsetName + DOT + lamp.minValue, 0, value);
    }

    public void removeItem(String value) {
        Lamp lamp = getItsLamp(value);
        if (lamp == null) {
            System.err.println(WARNING + "分片信息被损坏。");
        }
        jedis.zrem(zsetName + DOT + lamp.minValue, value);
        lamp.counter--;
        if (lamp.counter <= 0) {

            //vertexHugeZsetTreeMap.remove(lamp.minValue);
        } else if (lamp.counter < SETSKIPSIZE / 3) {
            Lamp prelamp = getItsPreLamp(lamp);
            Lamp nextlamp = getItsNextLamp(lamp);
            if (prelamp != null && prelamp.counter < SETSKIPSIZE / 3) {//中断判断,悬空指针,这个地方写了擦边球
                mergeZset(prelamp, lamp);
            } else if (nextlamp != null && nextlamp.counter < SETSKIPSIZE / 3) {
                mergeZset(lamp, nextlamp);
            } else {
                //do nothing
            }
        }
    }

    public ScaleIterator query(String start, String ending) {
        ScaleIterator scaleIterator = null;
        return scaleIterator;
    }


    //  给定一个lamp,让其自动合并,返回新set的lamp,方便将该put的数据继续放进去

    public long mergeZset(Lamp firstLamp, Lamp secondLamp) {
        //todo 待验证
        String firstSetName = zsetName + DOT + firstLamp.minValue;
        String secondSetName = zsetName + DOT + secondLamp.minValue;
        long unionNumbers = jedis.zunionstore(firstSetName, firstSetName, secondSetName);
        firstLamp.counter = firstLamp.counter + secondLamp.counter;
        this.getTreeMap().remove(secondLamp.minValue);
        return unionNumbers;
    }


    public String firstValue(Lamp lamp) {
        return jedis.zrange(zsetName + DOT + lamp.minValue, 0, 0).iterator().next();
    }

    public String lastValue(Lamp lamp) {
        return jedis.zrange(zsetName + DOT + lamp.minValue, -1, -1).iterator().next();
    }
    
    
    public static Iterator<Edge> queryEdge(Graph graph, String vertexID, String zsetName, String condition) {
        Lamp firstLamp = getItsLamp(zsetName, condition);//todo no update
        Lamp currentLamp=firstLamp;
        Lamp lastLamp = getItsLamp(zsetName, condition + TAIL);

        if(firstLamp == null || lastLamp == null)
        {
        	return new HashSet<Edge>().iterator();
        }
        
        String setname = zsetName + DOT + firstLamp.minValue;
        long firstRank = getRank(setname, condition);
        if (debugInfoSwitch) System.out.println(DEBUG + "firstRank=" + firstRank);
        long lastRank = getRank(setname, condition + TAIL);
        if (debugInfoSwitch) System.out.println(DEBUG + "lastRank=" + lastRank);
        Set<Edge> edgeSet = new HashSet<>();


        if (firstLamp.minValue.compareTo(lastLamp.minValue) == 0) {//same lamp
            Set<String> edgeString = jedis.zrange(zsetName + DOT + firstLamp.minValue, firstRank, lastRank);
            stringToEdge(edgeString.iterator(), edgeSet, graph);
        } else {// different lamp
            Set<String> edgeString1 = jedis.zrange(zsetName + DOT + firstLamp.minValue, firstRank, -1);
            stringToEdge(edgeString1.iterator(), edgeSet, graph);
            while (currentLamp.minValue.compareTo(lastLamp.minValue)!=0){
                currentLamp=getItsNextLamp(zsetName,currentLamp);
                Set<String> edgeString3 = jedis.zrange(zsetName + DOT + currentLamp.minValue, 0, -1);
                stringToEdge(edgeString3.iterator(), edgeSet, graph);
            }
            Set<String> edgeString2 = jedis.zrange(zsetName + DOT + lastLamp.minValue, 0, lastRank);
            stringToEdge(edgeString2.iterator(), edgeSet, graph);
        }
        return edgeSet.iterator();
    }


    private static void stringToEdge(Iterator iterator, Set set, Graph graph) {
        Iterator<String> stringIterator = iterator;
        while (stringIterator.hasNext()) {// todo no update
            String edgeInfo = stringIterator.next();
            Edge edge = queryEdgeByID(split(edgeInfo)[2], graph);
            set.add(edge);
        }
    }


    public static Iterator<Vertex> queryVertex(Graph graph, String zsetName, String condition) {


        Lamp firstLamp = getItsLamp(zsetName, condition);//todo no update
        Lamp currentLamp=firstLamp;
        Lamp lastLamp = getItsLamp(zsetName, condition + TAIL);
        
        if(firstLamp == null || lastLamp == null)
        {
        	return new HashSet<Vertex>().iterator();
        }
        
        String setname = zsetName + DOT + firstLamp.minValue;
        long firstRank = getRank(setname, condition);
        if (debugInfoSwitch) System.out.println(DEBUG + "firstRank=" + firstRank);
        long lastRank = getRank(setname, condition + TAIL);
        if (debugInfoSwitch) System.out.println(DEBUG + "lastRank=" + lastRank);
        Set<Vertex> vertexSet = new HashSet<>();
        if (firstLamp.minValue.compareTo(lastLamp.minValue) == 0) {//same lamp
            Set<String> vertexString = jedis.zrange(zsetName + DOT + firstLamp.minValue, firstRank, lastRank);
            stringToVertex(vertexString.iterator(), vertexSet, graph);
        } else {// different lamp
            Set<String> vertexString1 = jedis.zrange(zsetName + DOT + firstLamp.minValue, firstRank, -1);
            stringToVertex(vertexString1.iterator(), vertexSet, graph);
            while (currentLamp.minValue.compareTo(lastLamp.minValue)!=0){
                currentLamp=getItsNextLamp(zsetName,currentLamp);
                Set<String> vertexString3 = jedis.zrange(zsetName + DOT + currentLamp.minValue, 0, -1);
                stringToVertex(vertexString3.iterator(), vertexSet, graph);
            }
            Set<String> vertexString2 = jedis.zrange(zsetName + DOT + firstLamp.minValue, 0, lastRank);
            stringToVertex(vertexString2.iterator(), vertexSet, graph);
        }
        return vertexSet.iterator();
    }


    private static void stringToVertex(Iterator iterator, Set set, Graph graph) {
        Iterator<String> stringIterator = iterator;
        while (stringIterator.hasNext()) {// todo no update
            String vertexInfo = stringIterator.next();
            Vertex vertex = queryVertexByID(split(vertexInfo)[1], graph);
            set.add(vertex);
        }
    }


    private static long getRank(String setname, String condition) {
        jedis.zadd(HUGEZSETLAMP + DOT + setname, 0, condition);//todo no update
        Long rank = jedis.zrank(HUGEZSETLAMP + DOT + setname, condition);
        jedis.zrem(HUGEZSETLAMP + DOT + setname, condition);
        return rank;
    }


    public static Lamp getItsLamp(String zsetName, String value) {//this value is the label of vertex or edge


        if (hugeZsetTreeMap.get(zsetName) == null || hugeZsetTreeMap.get(zsetName).floorEntry(value) == null) {
            return null;
        }// todo no update
        return hugeZsetTreeMap.get(zsetName).floorEntry(value).getValue();//
    }


    public static Lamp getItsPreLamp(String zsetName, Lamp lamp) {
        if (hugeZsetTreeMap.get(zsetName) == null || hugeZsetTreeMap.get(zsetName).lowerEntry(lamp.minValue) == null) {
            return null;
        }// todo no update
        return hugeZsetTreeMap.get(zsetName).lowerEntry(lamp.minValue).getValue();
    }


    public static Lamp getItsNextLamp(String zsetName, Lamp lamp) {
        if (hugeZsetTreeMap.get(zsetName) == null || hugeZsetTreeMap.get(zsetName).higherEntry(lamp.minValue) == null) {
            return null;
        }// todo no update
        return hugeZsetTreeMap.get(zsetName).higherEntry(lamp.minValue).getValue();
    }
}
