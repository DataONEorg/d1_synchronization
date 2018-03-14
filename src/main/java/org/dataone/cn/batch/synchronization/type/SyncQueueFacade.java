package org.dataone.cn.batch.synchronization.type;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.dataone.cn.batch.synchronization.tasks.SyncObjectTask;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.synchronization.types.SyncObject;
import org.dataone.configuration.Settings;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISet;



/**
 * A class to manage the particulars of adding and removing SyncObjects to the set of backing 
 * synchronization queues.  As an EntryListener, it tracks when new nodes are added to the map,
 * even from other CN instances. 
 * 
 * The typical usage pattern is expected to be:
 * 
 *  Harvester:
 *       syncQueueFacade.add(syncObject)
 * 
 *  and cn.synchronize
 *       syncQueueFacade.addWithPriority(syncObject)
 * 
 *  and even:
 *       try {
 *          process(synObject)
 *       } catch (retryableException) 
 *           syncQueueFacade.addWithPriority(syncObject)
 *       }
 *       
 *  SyncObjectTask:
 *       syncObject = syncQMan.poll(10, TimeUnit.MILLISECONDS)
 *       process(syncObject)
 *       
 *
 * @author rnahf
 *
 */
public class SyncQueueFacade implements EntryListener<String, String> {

    static final Logger logger = Logger.getLogger(SyncObjectTask.class);
    
    /* the list of nodeIds that are the keys of the queueMap(s) */
    /* for shared roundrobin, a "SyncQueueQueue" could be implemented instead... */
    protected Deque<String> nodeIdRoundRobin = new LinkedList<String>(); 
    
    protected String synchronizationObjectQueue = Settings.getConfiguration().getString("dataone.hazelcast.synchronizationObjectQueue","default"); 
    
    
    DistributedDataClient processingClient = null;

    // these are maps of queue names, instead of the IQueues themselves, to avoid possible
    // complications with IMap.gets, which in the javadoc states that map values returned are
    // clones of the original.
    Map<String,String> queueMap = null;
    Map<String,String> priorityQueueMap = null;
    

    /**
     * A convenience constructor that implements HazelcastClient
     */
    public SyncQueueFacade() {
        this(new DistributedDataClient() {
         
            @SuppressWarnings("unchecked")
            @Override
            public <K, V> Map<K, V> getMap(String mapName) {
                
               return new AbstractListenableMapAdapter<K,V>(
                    (Map<K,V>) HazelcastClientFactory.getProcessingClient().getMap(mapName)) {

                        @Override
                        public void notifyEntryListeners(String action, Object key, Object value) {
                            // the wrapped IMap will handle this
                        }

                        @Override
                        public void addEntryListener(EntryListener<K,V> listener, boolean includeValue) {
                            ((IMap<K,V>)this.map).addEntryListener(listener, includeValue);
                        }
                };
            }

            @Override
            public <E> IQueue<E> getQueue(String queueName) {
                return HazelcastClientFactory.getProcessingClient().getQueue(queueName);
            }

            @Override
            public <E> ISet<E> getSet(String setName) {
                return HazelcastClientFactory.getProcessingClient().getSet(setName);
            }

            @Override
            public ILock getLock(String lockName) {
                return HazelcastClientFactory.getProcessingClient().getLock(lockName);
            }
        });     
    }
    
    
    public SyncQueueFacade(DistributedDataClient client) {
                

        processingClient = client;
        
        
        queueMap = processingClient.getMap("dataone.synchronization.queueMap");
        if (queueMap instanceof DistributedDataClient.ListenableMap)
            ((DistributedDataClient.ListenableMap<String,String>)queueMap).addEntryListener(this, false);
        
        priorityQueueMap = processingClient.getMap("dataone.synchronization.priority.queueMap");      
        if (priorityQueueMap instanceof DistributedDataClient.ListenableMap)
            ((DistributedDataClient.ListenableMap<String,String>)priorityQueueMap).addEntryListener(this, false);
        
        
        // populate the local round-robin of queue names
        Iterator<String> it = getQueueNames().iterator();
        while (it.hasNext()) {
            nodeIdRoundRobin.add(it.next());
        }
        
        // this adds the legacy all in one queue to the map
        // the listener should put this into the queue-name round robin
        if (!queueMap.containsKey("legacy"))
            queueMap.put("legacy", synchronizationObjectQueue);
    
        

    }
    
    
    /**
     * Adds a task to the appropriate queue, creating the queue if one doesn't exist
     * @param syncObject
     */
    public void add(SyncObject syncObject) {
        
        String nodeId = syncObject.getNodeId() == null ? "generic" : syncObject.getNodeId();
        
        if (!queueMap.containsKey(nodeId)) {
            queueMap.put(nodeId,"dataone.synchronization.queue." + nodeId);
        }
        processingClient.getQueue(queueMap.get(nodeId)).add(syncObject);
    }
    
    /**
     * Adds a task to the appropriate priority queue, creating the queue if one doesn't exist.
     * this.poll() method tries the priority queue before trying the regular ones 

     * @param syncObject
     */
    public void addWithPriority(SyncObject syncObject) {
        
        String nodeId = syncObject.getNodeId() == null ? "generic" : syncObject.getNodeId();
        
        if (!priorityQueueMap.containsKey(nodeId)) {
            priorityQueueMap.put(nodeId,"dataone.synchronization.priority.queue." + nodeId);
        }
        processingClient.getQueue(priorityQueueMap.get(nodeId)).add(syncObject);
    }

    
    
    

    /**
     * Returns the next SyncObject from one of the syncQueues, or null if none
     * exist in any of the SyncQueues.  This method iterates through all of the
     * queues at most one time.
     * 
     * Note that multiple queues may be polled, so the method may block for 
     * multiple per-queue timeouts.
     * 
     * The general ordering strategy for pulling from the queues is to check the priority queue for
     * the first node, then the regular queue for the same node if nothing is returned, and then
     * to repeat for subsequent nodes until an item is found, or all of the queues have
     * been polled one time.
     * 
     * @param perQueueTimeout
     * @param unit
     * @return
     * @throws InterruptedException
     */
    public SyncObject poll(long perQueueTimeout, TimeUnit unit) throws InterruptedException {
 
        SyncObject item = null;            
        
        //  go through the round robin no more than one time, or until you find an object
        for (int i=0; i< nodeIdRoundRobin.size(); i++) {
            String nextQueue = getNextNodeId();
        

            if (priorityQueueMap.containsKey(nextQueue)) {
                item = (SyncObject) processingClient.getQueue(priorityQueueMap.get(nextQueue)).poll(100,TimeUnit.MICROSECONDS);
            }
            if (item == null) {
                if (queueMap.containsKey(nextQueue)) {
                    item = (SyncObject) processingClient.getQueue(queueMap.get(nextQueue)).poll(perQueueTimeout, unit);
                }
            }
            if (item != null) 
                break;
        }
        return item;
    }

    
    /**
     * implements the Round Robin approach to reading from multiple queues
     * It reads the first item of the queue, then cycles it to the end of the queue
     * and returns that value.
     * @return
     */
    protected String getNextNodeId() {
        
        
        if (nodeIdRoundRobin.size() == 1)
            return nodeIdRoundRobin.getLast();
            
        if (nodeIdRoundRobin.size() == 0) 
            return null;
        
        String nextNodeId = nodeIdRoundRobin.removeFirst();
        nodeIdRoundRobin.addLast(nextNodeId);
        return nextNodeId;
    }
 

    /**
     * Returns the total number of items in the sync queues
     * @return
     */
    public int size() {
        
        int size = 0;
        for (String name : getQueueNames()) 
            size += size(name);
            
        return size;
    }
    
    
    public int size(String nodeId) {

        int size = 0;

        if (priorityQueueMap.containsKey(nodeId)) 
            size += processingClient.getQueue(priorityQueueMap.get(nodeId)).size();       

        if (queueMap.containsKey(nodeId)) 
            size += processingClient.getQueue(queueMap.get(nodeId)).size();
        
        return size;
    }

    
    /**
     * A special accessor for the legacy queue to support synchronization
     * metrics collection.
     * @return
     */
    public BlockingQueue<Object> getLegacyQueue() {
        return processingClient.getQueue(queueMap.get("legacy"));
    }

    
    /**
     * builds a sorted set from the keys of the two queue maps
     * @return
     */
    public TreeSet<String> getQueueNames() {
        TreeSet<String> queueNames = new TreeSet<>(queueMap.keySet());
        queueNames.addAll(priorityQueueMap.keySet());
        return queueNames;
    }
    
//    /**
//     * Returns the map of non-prioritized synchronization queues
//     * @return
//     */
//    protected IMap<String, IQueue<Object>> getSyncQueueMap() {
//        return queueMap;
//
//    }
//    
//    /**
//     * Returns the map of prioritized synchronization queues
//     * @return
//     */
//    protected IMap<String, IQueue<Object>> getPrioritySyncQueueMap() {
//        return priorityQueueMap;   
//    }
    
    
    
    
 

    
    /**
     * listens to map entries so that the round robin can be expanded
     * to include the new node entry.
     * 
     * Listens to both the regular and prioritized maps, and simply stores
     * the nodeId the first time it is encountered, not twice, so that equity
     * is maintained.
     */
    @Override
    public synchronized void entryAdded(EntryEvent<String, String> event) {
        if (!nodeIdRoundRobin.contains(event.getKey())) {
            nodeIdRoundRobin.add(event.getKey());
            logger.info("Added queue named '" + event.getKey() + "' to the nodeId round robin" );
        } else {
            logger.info("The queue named '" + event.getKey() + "' is already in the nodeId round robin" );
        }
    }

    @Override
    public void entryRemoved(EntryEvent<String, String> event) {
        // we're not going to remove nodeIds because it wouldn't improve performance
        // (notice that the poll() skips the wait time if the queue doesn't exist, anyway).
        // and it complicates things because of the two maps we're listening to.
        
    }

    @Override
    public void entryUpdated(EntryEvent<String, String> event) {
        // nothing to do here
        
    }

    @Override
    public void entryEvicted(EntryEvent<String, String> event) {
        // nothing to do here
        
    }
    

}
