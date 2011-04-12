    /*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.harvest.types;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * I need this class because of Java erasure of Generics during runtime.
 * Maybe there is a better way, but I need instanceof to ensure that
 * I have retrieved the correct object from the persistent store
 * @author waltz
 */
public class NodeMap extends HashMap implements Map, Serializable {

    private Map<String, Date> eventMap;

    public NodeMap() {
        eventMap = new HashMap<String, Date>();
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
            return eventMap.containsValue((Date) o);
        } else {
            return false;
        }
    }

    public boolean containsValue(Date o) {
        return eventMap.containsValue(o);

    }

    @Override
    public Object get(Object o) {
        return eventMap.get((String) o);
    }

    public Date get(String o) {
        return eventMap.get(o);
    }

    @Override
    public Object put(Object k, Object v) {
        return eventMap.put((String) k, (Date) v);
    }

    public Object put(String k, Date v) {
        return eventMap.put(k, v);
    }

    @Override
    public Object remove(Object o) {
        return eventMap.remove((String) o);
    }

    public Date remove(String o) {
        return eventMap.remove(o);
    }

    @Override
    public void putAll(Map map) {
        eventMap.putAll((Map<String, Date>) map);
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

    public Map<String, Date> getMap() {
        return eventMap;
    }

    public void setMap(Map<String, Date> eventMap) {
        this.eventMap = eventMap;
    }
}
