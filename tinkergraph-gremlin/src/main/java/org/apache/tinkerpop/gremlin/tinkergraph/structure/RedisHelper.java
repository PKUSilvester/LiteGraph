package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.tinkergraph.Util.Utility;

import java.util.*;
import java.util.Map.Entry;

import static org.apache.tinkerpop.gremlin.tinkergraph.Util.RedisResource.SETSKIPSIZE;
import static org.apache.tinkerpop.gremlin.tinkergraph.Util.RedisResource.jedis;
import static org.apache.tinkerpop.gremlin.tinkergraph.Util.Utility.*;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.HugeZset.showLamp;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.HugeZset.splitZset;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.LiteGraph.hugeZsetTreeMap;

/**
 * Created by wangzhenzhong on 16/7/27.
 */
public class RedisHelper {
    public static List<LiteVertex> queryVertexIndex(final LiteGraph graph, final String key, final Object value) {
        if (debugInfoSwitch)
            System.out.println(DEBUG + "Redis helper step.sideEffect key=" + key + "\tvalue=" + value.getClass() + value.toString());
        return null == graph.vertexIndex ? Collections.emptyList() : graph.vertexIndex.get(key, value);
    }

    public static List<LiteEdge> queryEdgeIndex(final LiteGraph graph, final String key, final Object value) {
        return null == graph.edgeIndex ? Collections.emptyList() : graph.edgeIndex.get(key, value);
    }

    public static void removeEdgeProperties(LiteEdge liteEdge) {
        String id = liteEdge.id.toString();
        jedis.del(EDGE_PROPERTIES + id);
    }

    public static void removeEdgePropertiesAndInformation(LiteEdge liteEdge) {
        String id = liteEdge.id.toString();
        jedis.del(EDGE_PROPERTIES + liteEdge.id, EDGE_LABEL + id, IN_VERTEX_ID + id, OUT_VERTEX_ID + id);
    }

    public static void removeEdgeInformation(LiteEdge liteEdge) {
        String id = liteEdge.id.toString();
        jedis.del(EDGE_LABEL + id, IN_VERTEX_ID + id, OUT_VERTEX_ID + id);
    }

    public static void removeVertexProperties(LiteVertex liteVertex) {
        String id = liteVertex.id.toString();
        jedis.del(VERTEX_PROPERTIES + id);
    }

    public static void removeVertexPropertiesAndInformation(LiteVertex liteVertex) {
        String id = liteVertex.id.toString();
        jedis.del(VERTEX_LABEL + id, VERTEX_PROPERTIES + id);
    }

    public static void removeVertexInformation(LiteVertex liteVertex) {
        String id = liteVertex.id.toString();
        jedis.del(VERTEX_LABEL + id);
    }

    public static void removeEdgeRelation(LiteEdge liteEdge) {

        removeFromHugeSet(VERTEX_EDGE + liteEdge.outVertex.id(),
                String.valueOf(MINUS_SIGN) + DOT + liteEdge.label + DOT + liteEdge.id + DOT + liteEdge.inVertex.id());
        removeFromHugeSet(VERTEX_EDGE + liteEdge.inVertex.id(),
                String.valueOf(ADD_SIGN) + DOT + liteEdge.label + DOT + liteEdge.id + DOT + liteEdge.outVertex.id());
    }


/*    public static void addNewVertexToRedis(LiteVertex vertex, Graph graph, Object... keyValues) {
        //graph.allVertexHugeZset.insertItem(valueAVToString(vertex.id.toString(), vertex.label));
        addVertexPeoperties(vertex.id(), getProperties(vertex));
    }*/

    public static void insertVertexIntoRedis(Vertex vertex, Graph graph, Object... keyValues) {

        LiteVertex liteVertex = (LiteVertex) vertex;
        //插入Vertex 信息
        if (debugInfoSwitch) System.out.println(((LiteVertex) vertex).id);
        if (debugInfoSwitch) System.out.println(vertex.toString());
        setVertexInfo(liteVertex);
        if (debugInfoSwitch) programLamp();
        //向ALLvertex巨型表插入数据


        insertIntoHugeZset(ALLVERTEX, vertexToString(liteVertex));
        if (debugInfoSwitch) programLamp();
        //insertProperties(liteVertex, keyValues);//放在litevertex.property中
        //if (debugInfoSwitch) programLamp();

        //if (liteVertex.property())
        //addVertexPeoperties(vertex.id(), null == liteVertex.properties ? Collections.emptyMap() : liteVertex.properties);
    }

    public static void insertProperties(Vertex liteVertex, Object[] keyValues) {
        if (keyValues.length % 2 != 0) {
            System.err.println("key value需配对出现");
        } else {
            if (keyValues.length >= 2) {
                //Map<String, List<VertexProperty>>
                //insertPropertiesToMemory(liteVertex, keyValues);
                insertPropertiesToRedis(liteVertex, keyValues);
            }
        }

    }

    public static void insertProperty(String setName, String key, Object value) {
        jedis.hset(setName,
                    key, String.valueOf(PRE_SINGLE) + DOT + getTypeOfValue(value) + DOT + value);
    }
    
    public static void insertPropertyAppend(String setName, String key, Object value) {
        if (!jedis.hexists(setName, key)) {
            jedis.hset(setName,
                    key, String.valueOf(PRE_SINGLE) + DOT + getTypeOfValue(value) + DOT + value);
        } else {
            String oldString = jedis.hget(setName, key);
            oldString = oldString.substring(4);
            value = value.toString() + DOT + oldString;
            jedis.hset(setName, key, String.valueOf(PRE_MULTI) + DOT + getTypeOfValue(value) + DOT + value);
        }
    }

    private static void insertPropertiesToRedis(Vertex liteVertex, Object[] keyValues) {
        if (keyValues.length == 2) {
            insertProperty(VERTEX_PROPERTIES + liteVertex.id(), keyValues[0].toString(), keyValues[1]);
        } else {
            for (int i = 0; i < keyValues.length; i += 2) {
                insertProperty(VERTEX_PROPERTIES + liteVertex.id(), keyValues[i].toString(), keyValues[i + 1]);
            }
        }
    }

    private static void insertPropertiesToMemory(Vertex vertex, Object[] keyValues) {
        Object[] newKeyValues = new Object[keyValues.length - 2];
        for (int i = 2; i < keyValues.length; i++) {
            newKeyValues[i - 2] = keyValues[i];
        }
        LiteVertex liteVertex = (LiteVertex) vertex;
        List<VertexProperty> list = null;
        if (keyValues.length == 2) {
            insertIntoPropertiesMap(list, liteVertex, keyValues[0].toString(), keyValues[1]);
        } else {
            Object[] objects = new Object[keyValues.length - 2];
            for (int i = 2; i < keyValues.length; i++) {
                String key = keyValues[i].toString();
                Object value = keyValues[i + 1];
                insertIntoPropertiesMap(list, liteVertex, key, value);
            }
        }
    }

    private static void insertIntoPropertiesMap(List<VertexProperty> list, LiteVertex liteVertex, String key, Object value) {
        LiteVertexProperty liteVertexProperty = new LiteVertexProperty(liteVertex, key, value);
        if (liteVertex.properties.containsKey(key)) {
            liteVertex.properties.get(key).add(liteVertexProperty);
        } else {
            list.add(liteVertexProperty);
            liteVertex.properties.put(key, list);
        }
    }

    /**
     * Vertex Properties store type is hashmap, the zset's scord is double/long type not enough
     * 这里将properties(map<String,List<VertexProperty></>></>)中的属性项目,合并同类项后存入内存中的hashmap。使用hmset一个指令放到redis中
     * 合并后形如: key  pre.type.value.value.value...
     */
    private static void addVertexPeoperties(Object id, Map<String, List<VertexProperty>> properties) {//HashMap
        HashMap<String, String> propertiesMap = new HashMap<String, String>();
        char type;
        char pre;
        for (Entry<String, List<VertexProperty>> entry : properties.entrySet()) {
            type = 'x';
            String finalKey = "";
            String finalValue = "";
            pre = PRE_SINGLE;
            String key = entry.getKey();
            List<VertexProperty> value = entry.getValue();
            if (value.size() > 1 || propertiesMap.containsKey(key)) {
                pre = PRE_MULTI;
            }
            Iterator<VertexProperty> iteratorValue = value.iterator();
            while (iteratorValue.hasNext()) {
                VertexProperty vertexProperty = iteratorValue.next();
                finalKey = vertexProperty.key();
                type = getTypeOfValue(vertexProperty.value());
                finalValue = finalValue + DOT + vertexProperty.value().toString();
            }
            finalValue = String.valueOf(pre) + DOT + type + finalValue;
            propertiesMap.put(finalKey, finalValue);
            jedis.hmset(VERTEX_PROPERTIES + id, propertiesMap);

        }
    }

    public static Lamp stringToLamp(String value) {
        Lamp lamp;
        String[] temp = split(value);
        if(temp[temp.length - 1].equals("null"))
        {
        	
        long counter = Long.valueOf(temp[temp.length - 2]);
        String next = temp[temp.length - 1];
        String minValue = value.substring(0, value.length() - 1 - (temp[temp.length - 1].length() + temp[temp.length - 2].length() + 1));
        lamp = new Lamp(minValue, counter, next.equals("null") ? null : next);
        return lamp;
        }
        
        if(temp[0].length() < 1)
        {
        	long counter = Long.valueOf(temp[1]);
        	String minValue = temp[0];
        	String next = value.substring(2+temp[1].length(), value.length());
            
            lamp = new Lamp(minValue, counter, next.equals("null") ? null : next);
            return lamp;
        }
        
        int cnt_idx = temp.length / 2;
        long counter = Long.valueOf(temp[cnt_idx]);
        String minValue = temp[0];
        for(int i = 1; i < cnt_idx; i++)
        {
        	minValue = minValue + Utility.DOT+temp[i];
        }
        
        String next = temp[cnt_idx + 1];
        for(int i = cnt_idx + 2; i < cnt_idx; i++)
        {
        	next = next + Utility.DOT+temp[i];
        }
        
        lamp = new Lamp(minValue, counter, next);
        return lamp;
        
    }
/*
    public static Lamp stringToLamp(String value) {
        Lamp lamp;
        String[] temp = split(value);
        if(temp.length <= 6)
        {
        	System.out.println("===err 6 velamp: "+value);
        	
        long counter = Long.valueOf(temp[temp.length - 2]);
        String next = temp[temp.length - 1];
        String minValue = value.substring(0, value.length() - 1 - (temp[temp.length - 1].length() + temp[temp.length - 2].length() + 1));
        lamp = new Lamp(minValue, counter, next);
        return lamp;
        }
        
        System.out.println("===err 8 velamp: "+value);
        long counter = Long.valueOf(temp[4]);
        String next = temp[5]+Utility.DOT+temp[6]+Utility.DOT+temp[7]+Utility.DOT+temp[8];
        String minValue = temp[0]+Utility.DOT+temp[1]+Utility.DOT+temp[2]+Utility.DOT+temp[3];
        lamp = new Lamp(minValue, counter, next);
        return lamp;
        
    }
    */
    public static void showTreeMap(TreeMap<String, TreeMap<String, Lamp>> mapTreeMap) {
        System.out.println(DEBUG + "****************************** hugeSetMap ******************************");
        if (mapTreeMap != null) {
            Iterator iter = mapTreeMap.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, TreeMap<String, Lamp>> entry = (Map.Entry<String, TreeMap<String, Lamp>>) iter.next();
                System.out.println(DEBUG + "\t" + entry.getKey() + "\t" + entry.getValue());
                TreeMap<String, Lamp> lampTreeMap = entry.getValue();
                Iterator lampTreeIterator = lampTreeMap.entrySet().iterator();
                while (lampTreeIterator.hasNext()) {
                    Map.Entry<String, Lamp> lampEntry = (Entry<String, Lamp>) lampTreeIterator.next();
                    System.out.println(DEBUG + lampEntry.getKey() + "\t");
                    showLamp(lampEntry.getValue());
                }
            }
        } else {
            System.out.println(DEBUG + "\t Null");
        }
        System.out.println("********************************* END *********************************");

    }

    public static void insertIntoHugeZset(String zsetName, String value) {
        if (debugInfoSwitch) System.out.println(DEBUG + "zsetName=" + zsetName + "\tvalue=" + value);
        if (debugInfoSwitch)
            System.out.println(DEBUG + "hugeZsetTreeMap.getOrDefault(zsetName, null)=" + hugeZsetTreeMap.getOrDefault(zsetName, null));
        if (hugeZsetTreeMap.getOrDefault(zsetName, null) == null) {//导入hugeTreemap
            //get the LampTree from hugeZsetTreemap, if you get null, import from redis
            TreeMap<String, Lamp> lampMap = new TreeMap<String, Lamp>();
            Set<String> lampSet = jedis.zrange(HUGEZSETLAMP + zsetName, 0, -1);
            if (debugInfoSwitch) System.out.println("lampSet=" + lampSet);
            if (lampSet != null)
                if (debugInfoSwitch) System.out.println("Set<String> lampSet = " + lampMap.isEmpty());
            if (lampSet != null && !lampSet.isEmpty()) {
                Iterator<String> lampString = lampSet.iterator();
                while (lampString.hasNext()) {
                    Lamp lamp = stringToLamp(lampString.next());
                    lampMap.put(lamp.minValue, lamp);
                }
                hugeZsetTreeMap.put(zsetName, lampMap);
            }
        }
        if (hugeZsetTreeMap.get(zsetName) == null) {
            TreeMap<String, Lamp> treeMap = new TreeMap<String, Lamp>();
            hugeZsetTreeMap.put(zsetName, treeMap);
        }
        if (hugeZsetTreeMap.get(zsetName).isEmpty()) {
            //构建treemap
            Lamp newlamp = new Lamp("", 0, null);
            hugeZsetTreeMap.get(zsetName).put(newlamp.minValue, newlamp);
        }
        //write data
        
        System.out.println("===err writeHuge: "+zsetName+" : "+value);
        
        writeDataToHugeSetOnRedis(zsetName, value);
            /*Lamp lamp = getLamp(zsetName, value);
            if (lamp == null) {
                System.out.println("lamp==null");
                jedis.zadd(zsetName + DOT + value, 0, value);
                lamp = new Lamp(value, 1, null);
                if (debugInfoSwitch)
                    System.out.println(DEBUG + "lamp== null    lamp=" + lamp.minValue + lamp.counter + lamp.next);
                jedis.zadd(HUGEZSETLAMP + DOT + zsetName, 0, value + DOT + 1 + DOT + null);
            } else {

                jedis.zrem(HUGEZSETLAMP + DOT + zsetName, lamp.minValue + DOT + lamp.counter + DOT + lamp.next);
                lamp.counter++;
                jedis.zadd(HUGEZSETLAMP + DOT + zsetName, 0, lamp.minValue + DOT + lamp.counter + DOT + lamp.next);
                jedis.zadd(zsetName + DOT + lamp.minValue, 0, value);

                if (debugInfoSwitch)
                    System.out.println(DEBUG + "lamp!=null    lamp=" + lamp.minValue + lamp.counter + lamp.next);
                if (lamp.counter >= SETSKIPSIZE) {
                    if (debugInfoSwitch) System.out.println("splitZset!");
                    splitZset(zsetName, lamp);
                }
            }*/

    }
/*
    private static void writeDataToHugeSetOnRedis(String zsetName, String value) {
        Lamp currentLamp = getInsertLamp(zsetName, value);
        if (currentLamp == null) {
            currentLamp = new Lamp(value, 1, hugeZsetTreeMap.get(zsetName).higherEntry(value).getValue().minValue);
            hugeZsetTreeMap.get(zsetName).put(currentLamp.minValue, currentLamp);
        }
        jedis.zadd(zsetName + DOT + currentLamp.minValue, 0, value);
        
        System.out.println("===err zadd: "+zsetName + " : "+ currentLamp.minValue + " : "+ value);
        
        currentLamp.counter++;
        if (currentLamp.counter >= SETSKIPSIZE) {
            splitZset(zsetName, currentLamp);
        }
    }
*/
    private static void writeDataToHugeSetOnRedis(String zsetName, String value) {
        Lamp currentLamp = HugeZset.getItsLamp(zsetName, value);
        
        if (currentLamp == null) {
        	
        	System.out.println("===err lamp1: "+ "null" + " : "+zsetName + " : "+ value);
        	
            //currentLamp = new Lamp(value, 0, null);
        	
        	currentLamp = new Lamp(value, 0, hugeZsetTreeMap.get(zsetName).higherEntry(value).getValue().minValue);
        	
        	hugeZsetTreeMap.get(zsetName).put(currentLamp.minValue, currentLamp);
            
        }
        
        else
        {
        	System.out.println("===err lamp: "+ currentLamp.minValue + " : "+zsetName + " : "+ value);
        }
        
        
        
        jedis.zadd(zsetName + DOT + currentLamp.minValue, 0, value);
        
        System.out.println("===err zadd: "+zsetName + " : "+ currentLamp.minValue + " : "+ value);
        
        currentLamp.counter++;
        if (currentLamp.counter >= SETSKIPSIZE) {
            splitZset(zsetName, currentLamp);
        }
    }
    
    public static void removeVertexFromAllVertex(LiteVertex liteVertex) {
        String value = liteVertex.label() + DOT + liteVertex.id;
        removeFromHugeSet(ALLVERTEX, value);
        
    }

    public static void removeFromHugeSet(String zsetName, String value) {
        Lamp lamp = getInsertLamp(zsetName, value);
        if (lamp == null) {
            System.err.println(WARNING + "分片信息被损坏。");
        }
        jedis.zrem(zsetName + DOT + lamp.minValue, value);
        lamp.counter--;
        if (lamp.counter <= 0) {
            hugeZsetTreeMap.get(zsetName).remove(lamp.minValue);
        } else if (lamp.counter < SETSKIPSIZE / 3) {
            Lamp prelamp = getPreLamp(zsetName, value);
            Lamp nextlamp = getNextLamp(zsetName, value);
            if (prelamp != null && prelamp.counter < SETSKIPSIZE / 3) {//中断判断,悬空指针,这个地方写了擦边球
                mergeZset(zsetName, prelamp, lamp);
            } else if (nextlamp != null && nextlamp.counter < SETSKIPSIZE / 3) {
                mergeZset(zsetName, lamp, nextlamp);
            } else {
                //do nothing
            }
        }
    }

    public static long mergeZset(String zsetName, Lamp firstLamp, Lamp secondLamp) {
        String firstSetName = zsetName + DOT + firstLamp.minValue;
        String secondSetName = zsetName + DOT + secondLamp.minValue;
        long unionNumbers = jedis.zunionstore(firstSetName, firstSetName, secondSetName);
        firstLamp.counter = firstLamp.counter + secondLamp.counter;
        hugeZsetTreeMap.get(zsetName).remove(secondLamp.minValue);
        return unionNumbers;
    }

    public static Lamp getInsertLamp(String zsetName, String value) {
        if (hugeZsetTreeMap.get(zsetName).floorEntry(value) != null)
            return hugeZsetTreeMap.get(zsetName).floorEntry(value).getValue();
        else
            return null;
    }

    public static Lamp getPreLamp(String zsetName, String value) {

        Lamp currentLamp = getInsertLamp(zsetName, value);
        if (hugeZsetTreeMap.get(zsetName).lowerEntry(currentLamp.minValue) != null)
            return hugeZsetTreeMap.get(zsetName).lowerEntry(currentLamp.minValue).getValue();
        else
            return null;
    }

    public static Lamp getNextLamp(String zsetName, String value) {
        Lamp currentLamp = getInsertLamp(zsetName, value);
        if (hugeZsetTreeMap.get(zsetName).higherEntry(currentLamp.minValue) != null)
            return hugeZsetTreeMap.get(zsetName).higherEntry(currentLamp.minValue).getValue();
        else
            return null;
        /*test(hugeZsetTreeMap.get(zsetName).floorEntry(value));
        test(hugeZsetTreeMap.get(zsetName).ceilingEntry(value));
        test(hugeZsetTreeMap.get(zsetName).lowerEntry(value));
        test(hugeZsetTreeMap.get(zsetName).higherEntry(value));
        test(hugeZsetTreeMap.get(zsetName).firstEntry());
        test(hugeZsetTreeMap.get(zsetName).lastEntry());*/
    }

    private static void test(Entry<String, Lamp> stringLampEntry) {
        if (stringLampEntry != null)
            showLamp(stringLampEntry.getValue());
        else
            System.out.println("Null");
    }

    private static Lamp getLamp(String zsetName, String value) {
        // code.3 得到当前Lamp
        if (hugeZsetTreeMap.get(zsetName).floorEntry(value) == null)
            return hugeZsetTreeMap.get(zsetName).floorEntry(value).getValue();
        else
            return null;
    }

    public static String vertexToString(LiteVertex liteVertex) {
        return liteVertex.label + DOT + liteVertex.id;
    }

    public static void loadVertexPropertiesFromRedis(LiteVertex vertex) {
        // 4.test. load Vertex Properties From Redis
        String setName = VERTEX_PROPERTIES + vertex.id;
        Map<String, String> map = jedis.hgetAll(setName);

        Iterator iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = (Entry<String, String>) iterator.next();
            String propertyName = entry.getKey();
            String propertyValue = entry.getValue();
            /*String[] temp = split(propertyValue);
            String[] keyValues = new String[temp.length - 2];
            for (int i = 0; i < keyValues.length; i += 2) {
                keyValues[i] = temp[i + 2];
            }*/
            loadVertexPropertyFromString(vertex, propertyName, propertyValue);
        }
    }

    private static void loadVertexPropertyFromString(LiteVertex vertex, String propertyName, String propertyValue) {
        if (debugInfoSwitch) {
            programLamp();
            System.out.println("String propertyName=" + propertyName);
            System.out.println("String propertyValue=" + propertyValue);
        }
        List<VertexProperty> list = new LinkedList<>();
        String[] temp = split(propertyValue);

        if (temp.length > 2) {
            for (int i = 2; i < temp.length; i += 2) {
                Object value = castToOriginalType(propertyValue.charAt(2), temp[i]);
                VertexProperty property = new LiteVertexProperty(vertex, propertyName, value);
                list.add(property);
            }
            vertex.properties.put(propertyName, list);
        }

    }

    private static List<VertexProperty> stringToPropertyList(Vertex vertex, String propertyName, String value) {
        List<VertexProperty> propertyList = null;
        /*char sorm = value.charAt(0);
        char type = value.charAt(2);
        String keyValueString = value.substring(4);*/
        //String[] temp = split(keyValueString);
        //String keyValues = keyValueString.substring(temp[0].length() + 1 + temp[1].length() + 1);
        // 5. later 将string中提取出的keyvalue变成vertexProperty,添加到list中
        loadVertexPropertyFromString((LiteVertex) vertex, propertyName, value);
        return propertyList;

    }

    public static void insertEdgeIntoRedis(LiteEdge edge, Object... keyValues) {
        insertIntoHugeZset(VERTEX_EDGE + edge.outVertex.id(), String.valueOf(MINUS_SIGN) + DOT + edge.label + DOT + edge.id + DOT + edge.inVertex.id());
        insertIntoHugeZset(VERTEX_EDGE + edge.inVertex.id(), String.valueOf(ADD_SIGN) + DOT + edge.label + DOT + edge.id + DOT + edge.outVertex.id());

        System.out.println("===err edge: "+edge.outVertex.id()+" : "+edge.inVertex.id()+ " : "+ edge.label + " : " + edge.id);
        
       // insertEdgeProperties(edge, keyValues);
        for(int i = 0; i < keyValues.length; i+=2)
        {
        	insertProperty(EDGE_PROPERTIES + edge.id, keyValues[i].toString(), keyValues[i+1]);
        }
        setEdgeInfo(edge);
    }

    public static void loadEdgePropertiesFromRedis(LiteEdge edge) {
        // 3.test. load edge Properties From Redis
        String setName = EDGE_PROPERTIES + edge.id;
        Map<String, String> map = jedis.hgetAll(setName);

        edge.properties = new HashMap<>();
        
        String propertyName;
        String propertyValue;
        Iterator iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = (Entry<String, String>) iterator.next();
            propertyName = entry.getKey();
            propertyValue = entry.getValue();

            if(propertyValue == null || split(propertyValue).length < 3)
            {
            	System.err.println("===err eprop: "+ edge.id+" : "+ propertyName + " : "+ propertyValue);
            }
            
            Property property = new LiteProperty(edge, propertyName, split(propertyValue)[2]);
            edge.properties.put(propertyName, property);
        }
    }


    public static void setVertexInfo(Vertex vertex) {
        jedis.mset(VERTEX_LABEL + vertex.id(), vertex.label());
    }

    public static String getVertexLabel(String vertexID) {
        return jedis.get(VERTEX_LABEL + vertexID);
    }

    public static void setEdgeInfo(Edge edge) {
        jedis.mset(EDGE_LABEL + edge.id(), edge.label(),
                IN_VERTEX_ID + edge.id(), edge.inVertex().id().toString(),
                OUT_VERTEX_ID + edge.id(), edge.outVertex().id().toString());
    }

    public static String getEdgeLabel(String edgeID) {
        return jedis.get(EDGE_LABEL + edgeID);
    }

    public static String getInVertexID(String edgeID) {
        return jedis.get(IN_VERTEX_ID + edgeID);
    }

    public static String getOutVertexID(String edgeID) {
        return jedis.get(OUT_VERTEX_ID + edgeID);
    }

    public static List<String> getEdgeInfo(String edgeID) {
        return jedis.mget(EDGE_LABEL + edgeID, IN_VERTEX_ID + edgeID, OUT_VERTEX_ID + edgeID);
    }


    /**
     * 此处转换出原类型代号
     */
    public static char getTypeOfValue(Object value) {
        char type = 'x';
        if (value.getClass().toString().compareTo("class java.lang.Character") == 0) type = 'c';
        if (value.getClass().toString().compareTo("class java.lang.Integer") == 0) type = 'i';
        if (value.getClass().toString().compareTo("class java.lang.Long") == 0) type = 'l';
        if (value.getClass().toString().compareTo("class java.lang.String") == 0) type = 'S';
        if (value.getClass().toString().compareTo("class java.lang.Float") == 0) type = 'f';
        if (value.getClass().toString().compareTo("class java.lang.Double") == 0) type = 'd';
        if (value.getClass().toString().compareTo("class java.lang.Short") == 0) type = 's';
        if (value.getClass().toString().compareTo("class java.lang.Byte") == 0) type = 'b';

        return type;
    }

    private static Object castToOriginalType(char sign, String value) {

        switch (sign) {
            case 'c':
                return value.charAt(0);
            case 'i':
                return Integer.valueOf(value);
            case 'l':
                return Long.valueOf(value);
            case 'S':
                return value;
            case 'f':
                return Float.valueOf(value);
            case 'd':
                return Double.valueOf(value);
            case 's':
                return Short.valueOf(value);
            case 'b':
                return Byte.valueOf(value);
            default:
                return value;
        }
    }

/*    private static void addElementToZset(String setName, long scord, String value) {

        jedis.zadd(setName, scord, value);
    }*/


    public static String[] split(String originalString) {
        String[] temp = originalString.split("\\.", -1);
        return temp;
    }

    public static void removeAllEdgesInOtherVertex(LiteVertex vertex) {
        /*if (debugInfoSwitch) programLamp();
        if (debugInfoSwitch)
            System.out.println(DEBUG + "verHuge=" + vertexHugeZsetTreeMap.get(vertex.id.toString()).getTreeMap().isEmpty());
        if (!vertexHugeZsetTreeMap.get(vertex.id.toString()).getTreeMap().isEmpty()) {
            if (debugInfoSwitch) programLamp();
            ScaleIterator allItemsScaleIterator = new ScaleIterator(vertexHugeZsetTreeMap.get(vertex.id.toString()));
            if (debugInfoSwitch) System.out.println(DEBUG + "Create new ScaleIterator successful.");
            while (allItemsScaleIterator.hasNext()) {
                if (debugInfoSwitch) programLamp();
                String item = (String) allItemsScaleIterator.next();
                if (debugInfoSwitch) System.out.println(DEBUG + "item=" + item);
                String[] temp = split(item);//0=sign,1=Elabel 2=Eid 3=Vid
                //ValueVEToString(String Elabel,String Eid,String Vid,char sign)
                char pre = ADD_SIGN;
                if (temp[0].compareTo(String.valueOf(ADD_SIGN)) == 0) {
                    pre = MINUS_SIGN;
                }
                if (debugInfoSwitch) programLamp();
                //TinkerVertex otherVertex = (TinkerVertex) (vertex.graph().vertices(temp[2]).next());
                vertexHugeZsetTreeMap.get(temp[3]).removeItem(valueVEToString(temp[1], temp[2], vertex.id.toString(), pre));
            }
        }*/

    }


    public static void removeEdge(LiteEdge edge) {
        /*String firstSet = VERTEX_EDGE + edge.outVertex.id();
        String firstString = String.valueOf(RIGHT) + DOT + edge.label + DOT + edge.id + DOT + edge.inVertex.id();
        String secondSet = VERTEX_EDGE + edge.inVertex.id();
        String secondString = String.valueOf(LEFT) + DOT + edge.label + DOT + edge.id + DOT + edge.outVertex.id();
        jedis.zrem(firstSet,firstString);
        jedis.zrem(secondSet,secondString);*/
        //((TinkerVertex) edge.outVertex).VEHugeZset.removeItem(valueVEToString(edge.label, edge.id.toString(), edge.inVertex.id().toString(), MINUS_SIGN));
        //((TinkerVertex) edge.inVertex).VEHugeZset.removeItem(valueVEToString(edge.label, edge.id.toString(), edge.outVertex.id().toString(), ADD_SIGN));
        jedis.del(EDGE_PROPERTIES + edge.id);
    }

    protected static void addVertexEdgeRelationSet(LiteVertex outVertex, LiteVertex inVertex, Object edgeID, String edgeLabel) {
        //vertexHugeZsetTreeMap.get(outVertex.id.toString()).insertItem(valueVEToString(edgeLabel, edgeID.toString(), inVertex.id.toString(), MINUS_SIGN));
        //vertexHugeZsetTreeMap.get(inVertex.id.toString()).insertItem(valueVEToString(edgeLabel, edgeID.toString(), outVertex.id.toString(), ADD_SIGN));
        //addElementToZset(VERTEX_EDGE + outVertex.id, SCORE, String.valueOf(RIGHT) + DOT + edgeLabel + DOT + edgeID + DOT + inVertex.id);
        //addElementToZset(VERTEX_EDGE + inVertex.id, SCORE, String.valueOf(LEFT) + DOT + edgeLabel + DOT + edgeID + DOT + outVertex.id);
    }

    protected static void addVertexEdgeToRedis(LiteVertex outVertex, LiteVertex inVertex, Object edgeID, String edgeLabel) {
        //vertexHugeZsetTreeMap.get(outVertex.id.toString()).insertItem(valueVEToString(edgeLabel, edgeID.toString(), inVertex.id.toString(), MINUS_SIGN));
        //vertexHugeZsetTreeMap.get(inVertex.id.toString()).insertItem(valueVEToString(edgeLabel, edgeID.toString(), outVertex.id.toString(), ADD_SIGN));
        //addElementToZset(VERTEX_EDGE + outVertex.id, SCORE, String.valueOf(RIGHT) + DOT + edgeLabel + DOT + edgeID + DOT + inVertex.id);
        //addElementToZset(VERTEX_EDGE + inVertex.id, SCORE, String.valueOf(LEFT) + DOT + edgeLabel + DOT + edgeID + DOT + outVertex.id);
    }

    /*
    protected static void insertEdgeProperties(Edge edge, Object[] keyValues) {
        if(keyValues.length != 0)
        {
    	if (keyValues.length % 2 != 0) {
            System.out.println("key/values must in pair");
        } else {
        	
        	for (int i = 0; i < keyValues.length; i = i + 2) {
            jedis.hset(EDGE_PROPERTIES + edge.id(), keyValues[i].toString(),
                    String.valueOf(PRE_SINGLE) + DOT + getTypeOfValue(keyValues[i+1]) + DOT + keyValues[1].toString());
        	}

        }
        }
    }
    */


    public static String getLabel(String value) {
        String[] temp = split(value);
        return temp[0];
    }

    public static String valueAVToString(String Vid, String Vlabel) {
        return Vlabel + DOT + Vid;
    }

    public static String valueVEToString(String Elabel, String Eid, String Vid, char sign) {
        return String.valueOf(sign) + DOT + Elabel + DOT + Eid + DOT + Vid;
    }
}
