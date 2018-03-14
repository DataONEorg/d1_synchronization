package org.dataone.cn.batch.synchronization.type;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ILock;

public interface DistributedDataClient {
    
    public <K,V> Map<K,V> getMap(String mapName);
       
    public <E> BlockingQueue <E> getQueue(String queueName);
    
    public <E> Set <E> getSet(String setName);
   
    public ILock getLock(String lockName);

    public interface ListenableMap<K,V> {
        
        public void notifyEntryListeners(String action, K key, V value);
        
        public void addEntryListener(EntryListener<K,V> listener, boolean includeValue);
        
    }
    
}