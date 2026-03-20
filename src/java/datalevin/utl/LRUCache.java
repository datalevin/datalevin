package datalevin.utl;

import java.util.*;

public class LRUCache {
    int capacity;
    Map<Object, Object> map;

    long target;

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

    public boolean isDisabled() {
        return disabled;
    }

    public void disable() {
        disabled = true;
    }

    public void enable() {
        disabled = false;
    }

    public long target() {
        return target;
    }

    public void setTarget(long target) {
        this.target = target;
    }

    public Object get(Object key) {
        if (disabled == true) return null;
        return map.get(key);
    }

    public void put(Object key, Object value) {
        if (disabled == true) return;
        map.put(key, value);
    }

    public Object remove(Object key) {
        return map.remove(key);
    }

    public Set<Object> keys() {
        synchronized(map) {
            return new HashSet<Object>(map.keySet());
        }
    }

    public List<Object> orderedKeys() {
        synchronized(map) {
            return new ArrayList<Object>(map.keySet());
        }
    }

    public void clear() {
        map.clear();
    }
}
