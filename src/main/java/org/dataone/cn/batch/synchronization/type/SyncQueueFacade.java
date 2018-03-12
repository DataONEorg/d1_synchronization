package org.dataone.cn.batch.synchronization.type;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.dataone.cn.batch.synchronization.tasks.SyncObjectTask;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.synchronization.types.SyncObject;
import org.dataone.configuration.Settings;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;



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
public class SyncQueueFacade implements EntryListener<String, IQueue<Object>> {

    static final Logger logger = Logger.getLogger(SyncObjectTask.class);
    
    /* the list of nodeIds that are the keys of the queueMap(s) */
    /* for shared roundrobin, a "SyncQueueQueue" could be implemented instead... */
    protected Deque<String> nodeIdRoundRobin = new LinkedList<String>();  //LL implements Deque interface
    
    protected String synchronizationObjectQueue = Settings.getConfiguration().getString("dataone.hazelcast.synchronizationObjectQueue"); 
    
//    protected Set<String> nodeIdSet = new HashSet<String>();
        
//    protected Map<String,Integer> mapCountMap = new HashMap<>();

    public SyncQueueFacade() {
                
        HazelcastClient processingClient = HazelcastClientFactory.getProcessingClient();
        
        IMap<String,IQueue<Object>> qMap = processingClient.getMap("dataone.synchronization.queueMap");        
        qMap.addEntryListener(this, false);
        
        IMap<String,IQueue<Object>> pqMap = processingClient.getMap("dataone.synchronization.priority.queueMap");      
        pqMap.addEntryListener(this, false);
        
        // this adds the legacy all in one queue to the map
        // the listener should put this into the queue-name round robin
        qMap.put("legacy", processingClient.getQueue(synchronizationObjectQueue));
        
        
    }
    
    
    /**
     * Adds a task to the appropriate queue, creating the queue if one doesn't exist
     * @param syncObject
     */
    public void add(SyncObject syncObject) {
        
        String nodeId = syncObject.getNodeId() == null ? "generic" : syncObject.getNodeId();
        
        
        HazelcastClient processingClient = HazelcastClientFactory.getProcessingClient();
        IMap<String,IQueue<Object>> qMap = processingClient.getMap("dataone.synchronization.queueMap");
        if (!qMap.containsKey(nodeId)) {
            qMap.put(nodeId,processingClient.getQueue("dataone.synchronization.queue." + nodeId));
        }
        qMap.get(nodeId).add(syncObject);
    }
    
    /**
     * Adds a task to the appropriate priority queue, creating the queue if one doesn't exist.
     * this.poll() method tries the priority queue before trying the regular ones 

     * @param syncObject
     */
    public void addWithPriority(SyncObject syncObject) {
        
        String nodeId = syncObject.getNodeId() == null ? "generic" : syncObject.getNodeId();
        
        HazelcastClient processingClient = HazelcastClientFactory.getProcessingClient();
        IMap<String,IQueue<Object>> qMap = processingClient.getMap("dataone.synchronization.priority.queueMap");
        if (!qMap.containsKey(nodeId)) {
                qMap.put(nodeId,processingClient.getQueue("dataone.synchronization.priority.queue." + nodeId));
        }
        qMap.get(nodeId).add(syncObject);
    }

    
    
    

    /**
     * Returns the next SyncObject from one of the syncQueues, or null if none
     * exist in any of the SyncQueues.  This method iterates through all of the
     * queues at most one time.
     * 
     * Note that multiple queues may be polled, so the method may block for 
     * multiple timeouts.
     * The strategy for pulling from the queues is to check the priority queue for
     * the first node, then the regular queue if nothing is returned, and then
     * to repeat for the next node until an item is found, or all of the queues have
     * been polled one time.
     * 
     * @param perQueueTimeout
     * @param unit
     * @return
     * @throws InterruptedException
     */
    public SyncObject poll(long perQueueTimeout, TimeUnit unit) throws InterruptedException {
        HazelcastClient processingClient = HazelcastClientFactory.getProcessingClient();

        SyncObject item = null;            
        
        //  go through the round robin no more than one time, or until you find an object
        for (int i=0; i< nodeIdRoundRobin.size(); i++) {
            String nextQueue = getNextNodeId();
        
            IMap<String,IQueue<Object>> priorityQueueMap = processingClient.getMap("dataone.synchronization.priority.queueMap");

            if (priorityQueueMap.containsKey(nextQueue)) {
                item = (SyncObject)priorityQueueMap.get(nextQueue).poll(100,TimeUnit.MICROSECONDS);
            }
            if (item == null) {
                IMap<String,IQueue<Object>> qMap = processingClient.getMap("dataone.synchronization.queueMap");
                if (qMap.containsKey(nextQueue)) {
                    item = (SyncObject) qMap.get(nextQueue).poll(perQueueTimeout, unit);
                }
            }
            if (item != null) 
                break;
        }
        return item;
    }

    
    /**
     * Returns the current set of keys of the SyncQueueMap
     * @return
     */
//    public Set<String> keySet() {
//        HazelcastClient processingClient = HazelcastClientFactory.getProcessingClient();
//        IMap<String,IQueue<Object>> qMap = processingClient.getMap("dataone.synchronization.queueMap");
//        return qMap.keySet();
//        
//    }
 

    /**
     * Returns the total number of items in the sync queues
     * @return
     */
    public int size() {
        HazelcastClient processingClient = HazelcastClientFactory.getProcessingClient();
        
        int size = 0;
        for (int i=0; i < nodeIdRoundRobin.size(); i++) {
            String nextQueue = getNextNodeId();
            
            IMap<String,IQueue<Object>> priorityQueueMap = processingClient.getMap("dataone.synchronization.priority.queueMap");

            if (priorityQueueMap.containsKey(nextQueue)) {
                size += priorityQueueMap.get(nextQueue).size();
            }
            IMap<String,IQueue<Object>> qMap = processingClient.getMap("dataone.synchronization.queueMap");
            if (qMap.containsKey(nextQueue)) {
                size += qMap.get(nextQueue).size();
            }
        }
        return size;
    }
    
    
    public int size(String nodeId) {
        HazelcastClient processingClient = HazelcastClientFactory.getProcessingClient();
        int size = 0;
        IMap<String,IQueue<Object>> priorityQueueMap = processingClient.getMap("dataone.synchronization.priority.queueMap");
        IQueue<Object> queue = priorityQueueMap.get(nodeId);
        size += queue == null ? 0 : queue.size();
        
        IMap<String,IQueue<Object>> qMap = processingClient.getMap("dataone.synchronization.queueMap");
        queue = qMap.get(nodeId);
        size += queue == null ? 0 : queue.size();
        
        return size;
    }

    
    public String[] getQueueNames() {
        return (String[]) this.nodeIdRoundRobin.toArray();
    }
    
    
    public IQueue<Object> getLegacyQueue() {
        HazelcastClient processingClient = HazelcastClientFactory.getProcessingClient();
        IMap<String,IQueue<Object>> qMap = processingClient.getMap("dataone.synchronization.queueMap");
        return qMap.get("legacy");
    }
    
    /**
     * Returns the map of non-prioritized synchronization queues
     * @return
     */
    protected IMap<String, IQueue> getSyncQueueMap() {
        HazelcastClient processingClient = HazelcastClientFactory.getProcessingClient();
        return processingClient.getMap("dataone.synchronization.queueMap");        
    }
    
    /**
     * Returns the map of prioritized synchronization queues
     * @return
     */
    protected IMap<String, IQueue> getPrioritySyncQueueMap() {
        HazelcastClient processingClient = HazelcastClientFactory.getProcessingClient();
        return processingClient.getMap("dataone.synchronization.priority.queueMap");        
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
     * listens to map entries so that the round robin can be expanded
     * to include the new node entry.
     * 
     * Listens to both the regular and prioritized maps, and simply stores
     * the nodeId the first time it is encountered, not twice, so that equity
     * is maintained.
     */
    @Override
    public synchronized void entryAdded(EntryEvent<String, IQueue<Object>> event) {
        if (!nodeIdRoundRobin.contains(event.getKey())) {
            nodeIdRoundRobin.add(event.getKey());
            logger.info("Added queue named '" + event.getKey() + "' to the nodeId round robin" );
        } else {
            logger.info("The queue named '" + event.getKey() + "' is already in the nodeId round robin" );
        }
    }

    @Override
    public void entryRemoved(EntryEvent<String, IQueue<Object>> event) {
        // we're not going to remove nodeIds because it wouldn't improve performance
        // (notice that the poll() skips the wait time if the queue doesn't exist, anyway).
        // and it complicates things because of the two maps we're listening to.
        
    }

    @Override
    public void entryUpdated(EntryEvent<String, IQueue<Object>> event) {
        // nothing to do here
        
    }

    @Override
    public void entryEvicted(EntryEvent<String, IQueue<Object>> event) {
        // nothing to do here
        
    }
    

}
