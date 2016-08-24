package org.apache.tinkerpop.gremlin.tinkergraph.structure;

/**
 * Created by wangzhenzhong on 16/8/5.
 */
public class Lamp {
    public String minValue;//==label
    public long counter;
    public String next;

    Lamp(String min, long counter, String hasNext) {
        this.minValue = min;//==label, change to label.hashcode() when store into the redis
        this.counter = counter;
        this.next = hasNext;
    }

    public void setMinValue(String min) {
        this.minValue = min;
    }

    public String getMinValue() {
        return minValue;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }

    public long getCounter() {
        return counter;
    }

    public void setNext(String value) {
        this.next = value;
    }

    public String getNext() {
        return this.next;
    }
}
