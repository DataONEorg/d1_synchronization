package org.dataone.cn.batch.synchronization.type;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.dataone.cn.synchronization.types.SyncObject;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.MapClientProxy;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISet;

public class SyncQueueFacadeTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    
    SyncQueueFacade sqf;
    
    @Before
    public void setUp() throws Exception {

        sqf = new SyncQueueFacade(new DistributedDataClient() {

            
            class ListenableConcurentHashMap<K,V> extends ConcurrentHashMap<K,V> implements ListenableMap<K,V> {

                HashMap<EntryListener,Boolean> listeners = new HashMap<>();
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
                        for (Map.Entry<EntryListener,Boolean> mapEntry : listeners.entrySet()) {
                            
                            EntryEvent event = new EntryEvent<K,V>(this.toString(), null,EntryEvent.TYPE_ADDED, key, mapEntry.getValue() ? value: null);
                            mapEntry.getKey().entryAdded(event);
                        }
                    }
                }

                @Override
                public void addEntryListener(EntryListener<K,V> listener,  boolean includeValue) {
                    listeners.put(listener, includeValue);
                    
                }
                
            }
            
            Map<String,Map> mapMap = new HashMap<>();
            Map<String,BlockingQueue> queueMap = new HashMap<>();
            {
                // these first two define the top level data structures that are the directory for syncQueues
                mapMap.put("dataone.synchronization.queueMap", new ListenableConcurentHashMap<String,String>());
                mapMap.put("dataone.synchronization.priority.queueMap", new ListenableConcurentHashMap<String,String>());

                // adds an queue entry for fake MN "TestTest", populates it, and adds to the sync queue directory map
                mapMap.get("dataone.synchronization.queueMap").put("urn:node:TestTest","dataone.synchronization.queueMap.urn:node:TestTest");
                
                BlockingQueue testTestQueue = new ArrayBlockingQueue<SyncObject>(1000);
                for (int i = 1; i <= 30; i++) {
                    SyncObject so = new SyncObject("urn:node:TestTest", String.format("iiiiii%2d",i));
                    testTestQueue.add(so);  
                }
                queueMap.put("dataone.synchronization.queueMap.urn:node:TestTest",testTestQueue);
                
                
                // prepopulates the legacy existing synchronization queue
                // but doesn't add it to the maps - SyncQueueFacade should do upon its initialization
                BlockingQueue defaultQueue = new ArrayBlockingQueue<SyncObject>(1000);
                for (int i = 101; i <= 120; i++) {
                    SyncObject so = new SyncObject("default", String.format("xxxxxx%2d",i));
                    defaultQueue.add(so);  
                }
                queueMap.put("default",defaultQueue);
                
            }
                
            @Override
            public <K, V> Map<K, V> getMap(String mapName) {
                if (!mapMap.containsKey(mapName)) {
                    mapMap.put(mapName, new ListenableConcurentHashMap<K,V>());
                }
                return mapMap.get(mapName);
            }

            @Override
            public <E> BlockingQueue <E> getQueue(String queueName) {
                if (!queueMap.containsKey(queueName)) {
                    queueMap.put(queueName, new ArrayBlockingQueue<E>(1000));
                }
                return queueMap.get(queueName);
            }

            @Override
            public <E> ISet<E> getSet(String setName) {
                // Not needed for SyncQueueFacade
                return null;
            }

            @Override
            public ILock getLock(String lockName) {
                // Not needed for SyncQueueFacade
                return null;
            }     
        });

        assertEquals("There should be 2 queues so far", 2, sqf.getQueueNames().size());
        assertEquals("legacy queue should have 20 objects", 20, sqf.size("legacy"));
        assertEquals("testTest queue should have 30 objects", 30, sqf.size("urn:node:TestTest"));
        assertEquals("Total size should be 50", 50, sqf.size());
        
    }
    
    
    @Test
    public void testPreloadedDrain() throws InterruptedException {
               
        int remaining = 50;
        while (remaining > 0) {
            SyncObject syncO = sqf.poll(100, TimeUnit.MILLISECONDS);
            if (syncO != null) {
                System.out.println(syncO.getPid());
                remaining--;
            }
        }  
    }
    
    @Test
    public void testAddedContentDrain() throws InterruptedException {
               
        int remaining = 50;
 
        while (remaining > 30) {
            SyncObject syncO = sqf.poll(100, TimeUnit.MILLISECONDS);
            if (syncO != null) {
                System.out.println(syncO.getNodeId() + "\t" + syncO.getPid());
                remaining--;
            }
        }  
        
        for (int i = 301; i <= 320; i++) {
            SyncObject so = new SyncObject("urn:node:YAMN", String.format("yyyyyy%2d",i));
            sqf.add(so);
        }
        remaining = sqf.size();
        System.out.println("========= New objects added, new total is: " + remaining);
        while (remaining > 0) {
            SyncObject syncO = sqf.poll(100, TimeUnit.MILLISECONDS);
            if (syncO != null) {
                System.out.println(syncO.getNodeId() + "\t" + syncO.getPid());
                remaining--;
            }
        }  
        
    }

    @Test
    public void testRetryContentDrain() throws InterruptedException {
               
        int remaining = 58;
 
        while (remaining > 0) {
            SyncObject syncO = sqf.poll(100, TimeUnit.MILLISECONDS);
            
            if (remaining % 7 == 0) {
                sqf.addWithPriority(syncO);
                System.out.println(remaining + ": Return to queue with priority: " + syncO.getNodeId() + " / " + syncO.getPid());
                remaining--;
            } else
            if (syncO != null) {
                System.out.println(remaining + ": " + syncO.getNodeId() + "\t" + syncO.getPid());
                remaining--;
            }
        }  
        
 
        
    }

    
        
        
        
       
        
        
 

}
