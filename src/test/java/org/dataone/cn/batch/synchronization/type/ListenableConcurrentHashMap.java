package org.dataone.cn.batch.synchronization.type;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.dataone.cn.batch.synchronization.type.DistributedDataClient.ListenableMap;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;

public class ListenableConcurrentHashMap<K,V> extends ConcurrentHashMap<K,V> implements ListenableMap<K,V> {

    private static final long serialVersionUID = -7359990440891548534L;
    
    
    Map<EntryListener<K,V>,Boolean> listeners = new HashMap<>();
    
    @Override
    public V put(K key, V value) {
        
        V previous = null;
        if (containsKey(key)) {
            super.put(key, value);
        } else {
            previous = get(key);
            super.put(key, value);
            notifyEntryListeners("INSERT", key, value);
        }
        return previous;
    }
    
    @Override
    public void notifyEntryListeners(String action, K key, V value) {
        if (action.equals("INSERT")) {
            for (Map.Entry<EntryListener<K,V>,Boolean> mapEntry : listeners.entrySet()) {     
                EntryEvent<K,V> event = new EntryEvent<K,V>(this.toString(), null,EntryEvent.TYPE_ADDED, key, mapEntry.getValue() ? value: null);
                mapEntry.getKey().entryAdded(event);
            }
        }
    }

    @Override
    public void addEntryListener(EntryListener<K,V> listener, boolean includeValue) {
        listeners.put(listener, includeValue);
    }

}
