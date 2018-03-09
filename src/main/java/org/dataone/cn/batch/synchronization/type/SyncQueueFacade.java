package org.dataone.cn.batch.synchronization.type;

import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.dataone.client.exception.ClientSideException;
import org.dataone.client.v1.CNode;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.synchronization.types.SyncObject;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;



/**
 * A class to manage the particulars of adding and removing SyncObjects to the set of backing 
 * synchronization queues.
 * 
 * The basic usage pattern would be
 * 
 *  Harvester:
 *       syncQMan.add(syncObject);
 * 
 *  SyncObjectTask:
 *       syncObject = syncQMan.poll(10, TimeUnit.MILLISECONDS)
 *       process(syncObject)
 *       
 * @author rnahf
 *
 */
public class SyncQueueFacade implements EntryListener<String, IQueue<Object>> {

  
    /* the list of nodeIds that are the keys of the queueMap(s) */
    protected Deque<String> nodeIdRoundRobin = new LinkedList<String>();  //LL implements Deque interface

//    protected Set<String> nodeIdSet = new HashSet<String>();
        
    protected Map<String,Integer> mapCountMap = new HashMap<>();

    public SyncQueueFacade() {
        
        
        HazelcastClient processingClient = HazelcastClientFactory.getProcessingClient();
        IMap<String,IQueue<Object>> qMap = processingClient.getMap("dataone.synchronization.queueMap");
        
        qMap.addEntryListener(this, false);
        
        IMap<String,IQueue<Object>> pqMap = processingClient.getMap("dataone.synchronization.priority.queueMap");
        
        qMap.addEntryListener(this, false);
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
     * Adds a task to the appropriate priority queue, creating the queue if one doesn't exist 

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
     * Returns the next SyncObject from the appropriate queue, or null if none exist within the given time period
     * Note that if no queue exists for the , null will be returned without waiting.
     * The priority queue for that node is polled first, adding a 100 microsecond overhead if the priority queue exists.
     * 
     * @param timeout
     * @param unit
     * @return
     * @throws InterruptedException
     */
    public SyncObject poll(long timeout, TimeUnit unit) throws InterruptedException {
        HazelcastClient processingClient = HazelcastClientFactory.getProcessingClient();

        SyncObject item = null;            
        
        String nextQueue = getNextNodeId();
        
        IMap<String,IQueue<Object>> priorityQueueMap = processingClient.getMap("dataone.synchronization.priority.queueMap");

        if (priorityQueueMap.containsKey(nextQueue)) {
            item = (SyncObject)priorityQueueMap.get(nextQueue).poll(100,TimeUnit.MICROSECONDS);
        }
        if (item == null) {
            IMap<String,IQueue<Object>> qMap = processingClient.getMap("dataone.synchronization.queueMap");
            if (qMap.containsKey(nextQueue)) {
                item = (SyncObject) qMap.get(nextQueue).poll(timeout, unit);
            }
        }
        return item;
    }

    
    /**
     * Returns the current set of keys of the SyncQueueMap
     * @return
     */
    public Set<String> keySet() {
        HazelcastClient processingClient = HazelcastClientFactory.getProcessingClient();
        IMap<String,IQueue<Object>> qMap = processingClient.getMap("dataone.synchronization.queueMap");
        return qMap.keySet();
        
    }
    
    
    
    /**
     * Returns the map of all synchronization queues
     * @return
     */
    protected IMap<String, IQueue> getSyncQueueMap() {
        HazelcastClient processingClient = HazelcastClientFactory.getProcessingClient();
        return processingClient.getMap("dataone.synchronization.queueMap");        
    }
    
    protected IMap<String, IQueue> getPrioritySyncQueueMap() {
        HazelcastClient processingClient = HazelcastClientFactory.getProcessingClient();
        return processingClient.getMap("dataone.synchronization.priority.queueMap");        
    }
    
    
    
    
    /**
     * implements the RoundRobin approach to reading from multiple queues
     * It pops the first item off the deque, pushes it onto the end,
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
    

    @Override
    public void entryAdded(EntryEvent<String, IQueue<Object>> event) {
        if (!nodeIdRoundRobin.contains(event.getKey()))
            nodeIdRoundRobin.add(event.getKey());
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
