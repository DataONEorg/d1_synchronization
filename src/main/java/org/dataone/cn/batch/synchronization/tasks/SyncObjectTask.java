/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.synchronization.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.synchronization.NodeCommD1ClientFactory;
import org.dataone.cn.batch.synchronization.NodeCommFactory;
import org.dataone.cn.batch.type.NodeComm;
import org.dataone.cn.batch.type.MemberNodeReaderState;
import org.dataone.cn.batch.type.SimpleNode;
import org.dataone.cn.batch.type.SyncObject;
import org.springframework.core.task.AsyncTaskExecutor;

/**
 * Reads from the syncTaskQueue synchronization tasks that need to be performed.
 *
 * Keeps track of the number of connections per membernode
 * and does not allow more than maxNumberOfClientsPerMemberNode
 * threads to execute per MN
 *
 * It runs as a daemon thread by the SyncObjectExecutor, and
 * is run as an eternal loop unless an exception is thrown.
 *
 *
 * @author waltz
 */
public class SyncObjectTask implements Callable<String> {

    Logger logger = Logger.getLogger(TransferObjectTask.class.getName());
    private AsyncTaskExecutor taskExecutor;
    private HazelcastInstance hazelcast;
    private Integer maxNumberOfClientsPerMemberNode;
    private NodeCommFactory nodeCommunicationsFactory;

    @Override
    public String call() {
        logger.debug("Starting SyncObjectTask");
        Map<String, List<NodeComm>> initializedMemberNodes = new HashMap<String, List<NodeComm>>();
        BlockingQueue<SyncObject> syncTaskQueue = hazelcast.getQueue("syncTaskQueue");
        IMap<String, SimpleNode> d1NodesMap = hazelcast.getMap("d1NodesMap");
//        ArrayList<Future> futuresList = new ArrayList<Future>();
        SyncObject task = null;
        try {
            do {
              
                task = syncTaskQueue.take();
                logger.info("found task " + task.getPid());
                if (task != null) {
                    NodeComm nodeCommunications = null;
                    // first check all the futures of past tasks to see if any have finished
                    // XXX is this code doing anything useful?
       /*             if (!futuresList.isEmpty()) {
                        ArrayList<Future> removalList = new ArrayList<Future>();
                        for (Future future : futuresList) {
                            try {
                                 future.get(1L, TimeUnit.MILLISECONDS);
                                // the future is now, reset the state of the NodeCommunication object
                                // so that it will be re-used (oh, maybe this should be done immediately
                                // before task ends
                                if (future.isDone()) {
                                    // print something about it returning ok
                                } 

                                removalList.add(future);
                                
                            } catch (ExecutionException ex) {

                                // this is a problem because we don't know which of the tasks
                                // threw an exception! 
                                // should we do anything special?

                                removalList.add(future);
                            } catch (TimeoutException ex) {
                                // not ready yet, ignore
                            }

                        }
                        if (!removalList.isEmpty()) {
                        futuresList.removeAll(removalList);
                        }
                    }
        *
        */
                    // investigate the task for membernode
                    String memberNodeId = task.getNodeId();
                    // grab a membernode client off of the stack of initialized clients
                    if (initializedMemberNodes.containsKey(memberNodeId)) {
                        List<NodeComm> mnReaderList = initializedMemberNodes.get(memberNodeId);
                        // find a mn reader that is not currently in use
                        for (NodeComm memberNodeReader : mnReaderList) {
                            if (memberNodeReader.getState().equals(MemberNodeReaderState.AVAILABLE)) {
                                nodeCommunications = memberNodeReader;
                                nodeCommunications.setState(MemberNodeReaderState.RUNNING);
                                break;
                            }
                        }
                        if (nodeCommunications == null) {
                            // no memberNodeReader is available, see if we can create a new one
                            if (mnReaderList.size() < maxNumberOfClientsPerMemberNode) {
                                // create and add a new one
                                nodeCommunications = nodeCommunicationsFactory.getNodeComm(d1NodesMap.get(memberNodeId).getBaseUrl());
                                nodeCommunications.setState(MemberNodeReaderState.RUNNING);
                                mnReaderList.add(nodeCommunications);
                            }
                        }
                    } else {
                        // The memberNode hash does not contain an array
                        // that is assigned to this MemberNode
                        // create it, get a reader, and put it in the hash
                        List<NodeComm> mnReaderList = new ArrayList<NodeComm>();
                        nodeCommunications = nodeCommunicationsFactory.getNodeComm(d1NodesMap.get(memberNodeId).getBaseUrl());
                        nodeCommunications.setState(MemberNodeReaderState.RUNNING);
                        mnReaderList.add(nodeCommunications);
                        initializedMemberNodes.put(memberNodeId, mnReaderList);
                    }
                    if (nodeCommunications != null) {
                        // finally, execute the new task!
                        TransferObjectTask transferObject = new TransferObjectTask(nodeCommunications,hazelcast, task);
                        FutureTask futureTask = new FutureTask(transferObject);
                        taskExecutor.execute(new FutureTask(transferObject));
       //                 futuresList.add(futureTask); // removed because futures are not doing anything
                    } else {
                        // Membernode Reader is unavailable.  place the task back on the queue
                        // and sleep for a second in case this is the only
                        // task left on the queue, maybe another node will have
                        // capacity to pick it up
                        logger.warn("No MN communication threads available at this time");
                        syncTaskQueue.put(task);
                        Thread.sleep(1000L);
                    }
                    // if an initialized client is unavailable, put task back on the stack,
                    // then sleep for a second and try for another task
                    // then try the same Task again.
                    //  taskExecutor.submit();
                }
            } while (true);
        } catch (InterruptedException ex) {
            return "Interrupted";
        }
    }

    // how do we or what actions  call shutdown on the executor such that no more tasks are created
    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    public void setHazelcast(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
    }

    public AsyncTaskExecutor getThreadPoolTaskExecutor() {
        return taskExecutor;
    }

    public void setThreadPoolTaskExecutor(AsyncTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public Integer getMaxNumberOfClientsPerMemberNode() {
        return maxNumberOfClientsPerMemberNode;
    }

    public void setMaxNumberOfClientsPerMemberNode(Integer maxNumberOfClientsPerMemberNode) {
        this.maxNumberOfClientsPerMemberNode = maxNumberOfClientsPerMemberNode;
    }

    public NodeCommFactory getNodeCommunicationsFactory() {
        return nodeCommunicationsFactory;
    }

    public void setNodeCommunicationsFactory(NodeCommFactory nodeCommunicationsFactory) {
        this.nodeCommunicationsFactory = nodeCommunicationsFactory;
    }
   
}
