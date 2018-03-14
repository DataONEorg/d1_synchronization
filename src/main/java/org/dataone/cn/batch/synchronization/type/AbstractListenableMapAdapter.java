package org.dataone.cn.batch.synchronization.type;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.dataone.cn.batch.synchronization.type.DistributedDataClient.ListenableMap;

import com.hazelcast.core.EntryListener;


/**
 * A helper class to simplify making maps listenable, cleaving to the Hazelcast definition
 * @author rnahf
 *
 * @param <K> the key
 * @param <V> the value
 */
public abstract class AbstractListenableMapAdapter<K,V> implements ListenableMap<K, V>, Map<K, V> {

    protected Map<K,V> map;
 
    
    public  AbstractListenableMapAdapter(Map<K,V> mapImplementation) {
        map = mapImplementation;
    }

    @Override
    public abstract void notifyEntryListeners(String action, K key, V value);
    

    @Override
    public abstract void addEntryListener(EntryListener<K, V> listener, boolean includeValue);
    
    //////////// boiler-plate forwards to wrapped implementation
    
    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return (V) map.get(key);
    }

    @Override
    public V put(K key, V value) {
        return (V) map.put(key, value);
    }

    @Override
    public V remove(Object key) {
        return (V) map.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        map.putAll(m);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return map.entrySet();
    }
}
