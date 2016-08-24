package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

import static org.apache.tinkerpop.gremlin.tinkergraph.Util.RedisResource.BUFFERSIZE;
import static org.apache.tinkerpop.gremlin.tinkergraph.Util.RedisResource.jedis;
import static org.apache.tinkerpop.gremlin.tinkergraph.Util.Utility.*;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.RedisHelper.split;

/**
 * Created by wangzhenzhong on 16/8/1.
 */
public class ScaleIterator<T extends Element> implements Iterator {
    private String start = null;
    private String ending = null;
    private Lamp startLamp;
    private Lamp endingLamp;
    private Lamp nowLamp = null;
    private long currentRedisRange;
    HugeZset hugeZset;
    private long pointer;
    Object[] buffer = new String[BUFFERSIZE];
    boolean isEmpty = false;
    private Long endingRank;
    Object edgeIds;

    ScaleIterator(HugeZset hugeZset) {//全部提取模式
        if (debugInfoSwitch) programLamp();
        this.hugeZset = hugeZset;
        if (hugeZset.getTreeMap().firstEntry() != null) {
            this.startLamp = hugeZset.getTreeMap().firstEntry().getValue();
            this.endingLamp = hugeZset.getTreeMap().lastEntry().getValue();
            this.nowLamp = startLamp;

            this.currentRedisRange = 0;
            this.pointer = -1;
/*
            if (debugInfoSwitch) System.out.println(this.hugeZset);
            if (debugInfoSwitch) System.out.println(this.nowLamp);
            if (debugInfoSwitch) System.out.println(this.currentRedisRange);
            if (debugInfoSwitch) System.out.println(this.pointer);*/

            this.start = hugeZset.firstValue(startLamp);
            this.ending = hugeZset.lastValue(endingLamp);
            endingRank = jedis.zrank(this.hugeZset.zsetName + DOT + this.endingLamp.minValue, ending);
        } else {
            isEmpty = true;
        }
    }

    ScaleIterator(HugeZset hugeZset, String start, String ending) {//全部提取模式
        //if (debugInfoSwitch) programLamp();
        this.hugeZset = hugeZset;
        this.currentRedisRange = 0;
        this.pointer = -1;
        this.start = start;
        this.ending = ending;
        if (hugeZset.getTreeMap().firstEntry() != null) {
            //if (debugInfoSwitch) System.out.println("Treemap!=Null");
            this.startLamp = hugeZset.getTreeMap().get(start);
            this.endingLamp = hugeZset.getTreeMap().get(ending);
            this.nowLamp = this.startLamp;
        } else {
            isEmpty = true;
            //if (debugInfoSwitch) System.out.println("Treemap=Null");
        }
        endingRank = jedis.zrank(this.hugeZset.zsetName + DOT + endingLamp.minValue, ending);

       /* if (debugInfoSwitch) System.out.println(this.hugeZset);
        if (debugInfoSwitch) System.out.println(this.nowLamp);
        if (debugInfoSwitch) System.out.println(this.currentRedisRange);
        if (debugInfoSwitch) System.out.println(this.pointer);*/

    }

    @Override
    public boolean hasNext() {
        isEmpty = nowLamp.minValue.compareTo(endingLamp.minValue) == 0 && currentRedisRange == endingRank + 1 && pointer == -1;
        return !isEmpty;
    }

    @Override
    public Object next() {
        if (isEmpty) return null;
        if (this.hasNext()) {
            if (pointer == -1) {//没有子弹,该换弹夹了
                //ending在本set
                if (nowLamp.minValue.compareTo(endingLamp.minValue) == 0) {//换弹夹
                    if (currentRedisRange + BUFFERSIZE < endingRank) {//满弹
                        //if (debugInfoSwitch) System.out.println("ifesle1");
                        buffer = hugeZset.getSomeItemInRange(nowLamp, currentRedisRange, BUFFERSIZE);
                        currentRedisRange += BUFFERSIZE;
                        pointer = BUFFERSIZE;
                    } else {
                        //if (debugInfoSwitch) System.out.println("ifesle2");
                        buffer = hugeZset.getSomeItemInRange(nowLamp, currentRedisRange, endingRank);
                        pointer = endingRank - currentRedisRange;
                        currentRedisRange = endingRank + 1;
                    }
                } else {//ending不在本set
                    if (currentRedisRange + BUFFERSIZE < endingRank) {//满弹
                        //if (debugInfoSwitch) System.out.println("ifesle3");
                        buffer = hugeZset.getSomeItemInRange(nowLamp, currentRedisRange, BUFFERSIZE);
                        currentRedisRange += BUFFERSIZE;
                        pointer = BUFFERSIZE;
                    } else {//到结尾
                        //if (debugInfoSwitch) System.out.println("ifesle4");
                        buffer = hugeZset.getSomeItemInRange(nowLamp, currentRedisRange, -1);
                        pointer = buffer.length - 1;
                        currentRedisRange = 0;
                        nowLamp = hugeZset.getItsNextLamp(nowLamp);
                    }
                    //if (debugInfoSwitch) System.out.println("else patch over");
                }
            }
            pointer--;
            /*if (debugInfoSwitch)
                System.out.println(pointer + "\t" + BUFFERSIZE + "\t" + currentRedisRange + "\t" + nowLamp.counter);
            if (debugInfoSwitch)
                System.out.println(DEBUG + "buffer.length=" + buffer.length);
            if (debugInfoSwitch) {
                for (int i = 0; i < buffer.length; i++) {
                    System.out.println(i + ":" + buffer[i]);
                }
            }*/
            return buffer[(int) (pointer + 1)];
        } else {//over
            return null;
        }
    }

    public Iterator<Edge> returnEdges(Object... edgeIDs) {
        ScaleIterator scaleIterator = this;
        Iterator<Edge> iterator = new Iterator<Edge>() {

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Edge next() {
                return null;
            }
        };
        return iterator;
    }

    public Iterator<Edge> returnEdges(TinkerVertex outVertex, Direction direction, final String... edgeLabels) {
        ScaleIterator scaleIterator = this;
        Iterator<Edge> iterator = new Iterator<Edge>() {
            @Override
            public boolean hasNext() {
                return scaleIterator.hasNext();
            }

            @Override
            public Edge next() {
                Object obj = scaleIterator.next();
                String[] temp = split(obj.toString());

                String pre = temp[0];
                String label = temp[1];
                String Eid = temp[2];
                String inV = temp[3];
                TinkerVertex inVertex ;//= new TinkerVertex(inV, null);
                TinkerEdge edge = null; //= new TinkerEdge(Eid, outVertex, label, inVertex, false);
                return edge;
            }
        };
        return iterator;
    }

    public Iterator<Vertex> returnVertes() {
        ScaleIterator scaleIterator = this;
        Iterator<Vertex> iterator = new Iterator<Vertex>() {
            @Override
            public boolean hasNext() {
                return scaleIterator.hasNext();
            }

            @Override
            public Vertex next() {
                Object obj = scaleIterator.next();
                String[] temp = split(obj.toString());
                String label = temp[0];
                String vid = temp[1];
                TinkerVertex vertex = null;//= new TinkerVertex(vid, label);
                return vertex;
            }
        };
        return iterator;
    }
}
