/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.packager.types;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * I need this class because of Java erasure of Generics during runtime.
 * Maybe there is a better way, but I need instanceof to ensure that
 * I have retrieved the correct object from the persistent store
 * @author waltz
 */
public class LogAccessMap extends HashMap implements Map, Serializable {

    private Map<String, Long> eventMap;

    public LogAccessMap() {
        eventMap = new HashMap<String, Long>();
    }

    @Override
    public int size() {
        return eventMap.size();
    }

    @Override
    public boolean isEmpty() {
        return eventMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object o) {
        if (o instanceof String) {
            return eventMap.containsKey((String) o);
        } else {
            return false;
        }
    }

    public boolean containsKey(String o) {
        return eventMap.containsKey(o);
    }

    @Override
    public boolean containsValue(Object o) {
        if (o instanceof Map) {
            return eventMap.containsValue((Long) o);
        } else {
            return false;
        }
    }

    public boolean containsValue(Long o) {
        return eventMap.containsValue(o);

    }

    @Override
    public Object get(Object o) {
        return eventMap.get((String) o);
    }

    public Long get(String o) {
        return eventMap.get(o);
    }

    @Override
    public Object put(Object k, Object v) {
        return eventMap.put((String) k, (Long) v);
    }

    public Object put(String k, Long v) {
        return eventMap.put(k, v);
    }

    @Override
    public Object remove(Object o) {
        return eventMap.remove((String) o);
    }

    public Long remove(String o) {
        return eventMap.remove(o);
    }

    @Override
    public void putAll(Map map) {
        eventMap.putAll((Map<String, Long>) map);
    }

    @Override
    public void clear() {
        eventMap.clear();
    }

    @Override
    public Set keySet() {
        return eventMap.keySet();
    }

    @Override
    public Collection values() {
        return eventMap.values();
    }

    @Override
    public Set entrySet() {
        return eventMap.entrySet();
    }

    public Map<String, Long> getMap() {
        return eventMap;
    }

    public void setMap(Map<String, Long> eventMap) {
        this.eventMap = eventMap;
    }
}
