package datalevin.utl;

import java.util.*;

public class LRUCache {
    int capacity;
    Map<Object, Object> map;

    long target;

    long generation;

    boolean disabled;

    public LRUCache(int capacity) {
        this.capacity = capacity;
        disabled = false;
        map = Collections.synchronizedMap(new LinkedHashMap<Object, Object>(capacity,
                                                                            0.75f,
                                                                            true) {
                protected boolean	removeEldestEntry(Map.Entry<Object, Object> oldest) {
                    return size() > capacity;
                }
            });
    }

    public LRUCache(int capacity, long target) {
        this(capacity);
        this.target = target;
    }

    public synchronized boolean isDisabled() {
        return disabled;
    }

    public synchronized void disable() {
        disabled = true;
    }

    public synchronized void enable() {
        disabled = false;
    }

    public synchronized long target() {
        return target;
    }

    public synchronized void setTarget(long target) {
        this.target = target;
    }

    public synchronized long generation() {
        return generation;
    }

    public synchronized void beginInvalidation(long target) {
        this.target = target;
        generation++;
    }

    public synchronized Object get(Object key) {
        if (disabled == true) return null;
        return map.get(key);
    }

    public synchronized void put(Object key, Object value) {
        if (disabled == true) return;
        map.put(key, value);
    }

    public synchronized boolean putIfGeneration(Object key, Object value,
                                                long expectedGeneration) {
        if (disabled == true || generation != expectedGeneration) return false;
        map.put(key, value);
        return true;
    }

    public synchronized Object remove(Object key) {
        return map.remove(key);
    }

    public synchronized Set<Object> keys() {
        return new HashSet<Object>(map.keySet());
    }

    public synchronized boolean isEmpty() {
        return map.isEmpty();
    }

    public synchronized List<Object> orderedKeys() {
        return new ArrayList<Object>(map.keySet());
    }

    public synchronized void clear() {
        map.clear();
    }
}
