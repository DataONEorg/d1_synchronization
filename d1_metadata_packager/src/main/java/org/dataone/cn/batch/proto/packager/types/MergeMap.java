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
public class MergeMap extends HashMap implements Map, Serializable {

    private Map<String, Map<String, String>> mergeMap;

    public MergeMap() {
        mergeMap = new HashMap<String, Map<String, String>>();
    }


    @Override
    public int size() {
        return mergeMap.size();
    }

    @Override
    public boolean isEmpty() {
        return mergeMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object o) {
        if (o instanceof String) {
            return mergeMap.containsKey((String)o);
        } else {
            return false;
        }
    }

    public boolean containsKey(String s) {
            return mergeMap.containsKey((String)s);
    }
    
    @Override
    public boolean containsValue(Object o) {
        if (o instanceof Map) {
            return mergeMap.containsValue((Map)o);
        } else {
            return false;
        }
    }
    public boolean containsValue(Map<String, String> m) {
            return mergeMap.containsValue(m);
    }

    @Override
    public Object get(Object o) {
        return mergeMap.get((String)o);
    }
    public Map<String, String> get(String s) {
        return mergeMap.get(s);
    }
    @Override
    public Object put(Object k, Object v) {
        return mergeMap.put((String)k, (Map)v);
    }
    public Map<String, String> put(String k, Map<String, String> v) {
        return mergeMap.put(k, v);
    }
    @Override
    public Object remove(Object o) {
        return mergeMap.remove((String)o);
    }

    public Map<String, String> remove(String o) {
        return mergeMap.remove((String)o);
    }
    @Override
    public void putAll(Map map) {
        mergeMap.putAll((Map<String, Map<String, String>>)map);
    }

    @Override
    public void clear() {
        mergeMap.clear();
    }

    @Override
    public Set keySet() {
        return mergeMap.keySet();
    }

    @Override
    public Collection values() {
        return mergeMap.values();
    }

    @Override
    public Set entrySet() {
        return mergeMap.entrySet();
    }

    public Map<String, Map<String, String>> getMap() {
        return mergeMap;
    }

    public void setMap(Map<String, Map<String, String>> mergeMap) {
        this.mergeMap = mergeMap;
    }

}
