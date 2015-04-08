/**
 * This work was created by participants in the DataONE project, and is jointly copyrighted by participating
 * institutions in DataONE. For more information on DataONE, see our web site at http://dataone.org.
 *
 * Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * $Id$
 */
package org.dataone.cn.batch.synchronization.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.exceptions.ExecutionDisabledException;
import org.dataone.cn.batch.exceptions.NodeCommUnavailable;
import org.dataone.cn.batch.synchronization.NodeCommFactory;
import org.dataone.cn.batch.synchronization.NodeCommSyncObjectFactory;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.batch.synchronization.type.NodeCommState;
import org.dataone.cn.batch.synchronization.type.SyncObject;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.util.DateTimeMarshaller;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Regulates the processing of the hzSyncObjectQueue by assigning SyncObjects
 * to available resources (NodeComms), submitting FutureTasks to (SpringFramework)
 * task executor, and managing task-related processing failures.
 * 
 * It regulates execution scheduling by throttling the number of connections per 
 * MemberNode to maxNumberOfClientsPerMemberNode (a bean-property), and also 
 * canceling tasks that are taking too long to complete.
 *
 * Instances of this class that are call()'ed run as a daemon thread by the 
 * SyncObjectExecutor, and the call() method uses an infinite loop that gracefully
 * exits upon exception.
 *
 * @author waltz
 */
public class SyncObjectTask implements Callable<String> {

    Logger logger = Logger.getLogger(TransferObjectTask.class.getName());
    private ThreadPoolTaskExecutor taskExecutor;
    private HazelcastInstance hazelcast;
    private Integer maxNumberOfClientsPerMemberNode;
    private NodeCommFactory nodeCommunicationsFactory;
    private static final String nodecommName = "NODECOMM";
    private static final String taskName = "SYNCOBJECT";
    private static final long threadTimeout = Settings.getConfiguration()
            .getLong("Synchronization.SyncObjectTask.threadTimeout", 900000L); //900000L represents fifteen minutes

    /**
     * 
     * Method to be called in a separately executing thread.
     *
     * @return String
     * @throws Exception
     */
    @Override
    public String call() throws Exception {

        logger.info("Starting SyncObjectTask");

        String hzNodesName = Settings.getConfiguration().getString("dataone.hazelcast.nodes");
        String syncObjectQueue = Settings.getConfiguration().getString("dataone.hazelcast.synchronizationObjectQueue");
        BlockingQueue<SyncObject> hzSyncObjectQueue = hazelcast.getQueue(syncObjectQueue);
        IMap<NodeReference, Node> hzNodes = hazelcast.getMap(hzNodesName);
        
        NodeCommFactory nodeCommFactory = NodeCommSyncObjectFactory.getInstance();
        
        HashMap<FutureTask, HashMap<String, Object>> futuresMap = new HashMap<FutureTask, HashMap<String, Object>>();
        // the futures map is helpful in tracking the state of a TransferObjectTask
        // submitted to the task executor as a FutureTask
        // The HashMap contains the NodeComm and the SyncObject 
        // 
        // NodeComm is used to determine how long the comm channels have 
        //   been running and unavailable for use by any other task.
        //   - If the task has been running for over an hour, it is considered 
        //     blocking and will be terminated.  
        //   - Once the task is terminated, the membernode will need to be
        //     informed of the synchronization failure. 
        // Hence, the SyncObject is needed to create and schedule a SyncFailedTask

        SyncObject task = null;
        try {
            do {  // forever, (unless exception thrown)
                
                // Settings gets refreshed periodically 
                boolean activateJob = Boolean.parseBoolean(Settings.getConfiguration().getString("Synchronization.active"));
                if (activateJob) {
                    task = hzSyncObjectQueue.poll(90L, TimeUnit.SECONDS);
                } else {
                    // do not listen to Sync Object queue, just finish up with any active tasks
                    // clearing out the futures map
                    // to allow for eventual shutdown
                    if (futuresMap.isEmpty()) {
                        // ok futures Map is empty, no need to keep spinning here, shut this thread down
                        logger.info("All Tasks are complete. Shutting down\n");
                        throw new ExecutionDisabledException();
                    }
                    Thread.sleep(10000L); // ten seconds
                    task = null;
                }
                
                reapFutures(futuresMap);
                
                
                // If task does need to be processed
                // Try to get a communications object for it
                // if no comm objects are in the comm object buffer, then stick
                // the task back on the queue
                if (task != null) {
                    try {
                        // Found a task now see if there is a comm object available
                        // if so, then run it
                        logger.info(task.taskLabel() + " received");
                        // investigate the task for membernode
                        NodeReference nodeReference = new NodeReference();
                        nodeReference.setValue(task.getNodeId());
                        Node mnNode = hzNodes.get(nodeReference);
                        NodeComm nodeCommunications = nodeCommFactory.getNodeComm(mnNode.getIdentifier());

                        // finally, execute the new task!
                        try {
                            TransferObjectTask transferObject = new TransferObjectTask(nodeCommunications, task);
                            FutureTask futureTask = new FutureTask(transferObject);
                            taskExecutor.execute(futureTask);

                            HashMap<String, Object> futureHash = new HashMap<String, Object>();
                            futureHash.put(nodecommName, nodeCommunications);
                            futureHash.put(taskName, task);
                            futuresMap.put(futureTask, futureHash);
                            logger.info(task.taskLabel() + " submitted for execution");
                        } catch (TaskRejectedException ex) {
                            // Tasks maybe rejected because of the pool has filled up 
                            // and no more tasks may be executed.
                            // TODO: Q. Is it the responsibility of this class to avoid this situation?
                            // This situation is dire since it should never be allowed to occur!
                            logger.error(task.taskLabel() + " Rejected");
                            logger.error("ActiveCount: " + taskExecutor.getActiveCount() + " Pool size " 
                                    + taskExecutor.getPoolSize() + " Max Pool Size " + taskExecutor.getMaxPoolSize());
                            nodeCommunications.setState(NodeCommState.AVAILABLE);
                            hzSyncObjectQueue.put(task);
                            try {
                                Thread.sleep(5000L); // millisecs
                            } catch (InterruptedException iex) {
                                logger.debug("sleep interrupted");
                            }
                        }
                    } catch (NodeCommUnavailable ex) {
                        // Communication object is unavailable.  place the task back on the queue
                        // and sleep for 10 seconds
                        // maybe another node will have
                        // capacity to pick it up
                        logger.warn("No MN communication threads available at this time");
                        hzSyncObjectQueue.put(task);
                        try {
                            Thread.sleep(5000L); // ten seconds
                        } catch (InterruptedException ex1) {
                            logger.debug("sleep interrupted");
                        }
                    }
                }
                logger.debug("ActiveCount: " + taskExecutor.getActiveCount() + " Pool size " 
                        + taskExecutor.getPoolSize() + " Max Pool Size " + taskExecutor.getMaxPoolSize());
                if ((taskExecutor.getActiveCount()) >= taskExecutor.getPoolSize()) {
                    if ((taskExecutor.getPoolSize() == taskExecutor.getMaxPoolSize()) && futuresMap.isEmpty()) {
                        BlockingQueue<Runnable> blockingTaskQueue = taskExecutor.getThreadPoolExecutor().getQueue();
                        Runnable[] taskArray = {};
                        taskArray = blockingTaskQueue.toArray(taskArray);
                        for (int j = 0; j < taskArray.length; ++j) {
                            taskExecutor.getThreadPoolExecutor().remove(taskArray[j]);
                        }
                    }
                    taskExecutor.getThreadPoolExecutor().purge();
                }
            } while (true);
        } catch (InterruptedException ex) {
            // XXX
            // determine which exceptions are acceptable for
            // restart and which ones are truly fatal
            // otherwise could cause an infinite loop of continuous failures
            //
            ex.printStackTrace();
            logger.error("Interrupted! by something " + ex.getMessage() + "\n");
            return "Interrupted";
        }
    }
    
    
    /**
     * try to reap futures:
     * they may have completed successfully
     * they may have been cancelled by something
     * they may have thrown an exception and failed
     * or they may be stuck/blocking/hanging and need to be killed
     * 
     * when a TransferObjectTask Future is cancelled due to taking too long, 
     * it executes a SyncFailedTask Future
     * 
     * @param futuresMap
     * @throws InterruptedException
     */
    private void reapFutures(HashMap<FutureTask, HashMap<String, Object>> futuresMap) throws InterruptedException {

        // first check all the futures of past tasks to see if any have finished
        // XXX is this code doing anything useful?
        if (futuresMap.size() > 0) {
            logger.info("waiting on " + futuresMap.size() + " futures");
        } else {
            logger.debug("Polling empty hzSyncObjectQueue");
        }
        if (!futuresMap.isEmpty()) {
            ArrayList<Future> removalList = new ArrayList<Future>();

            for (FutureTask future : futuresMap.keySet()) {
                HashMap<String, Object> futureHash = futuresMap.get(future);
                SyncObject futureTask = (SyncObject) futureHash.get(taskName);
                NodeComm futureNodeComm = (NodeComm) futureHash.get(nodecommName);
                logger.debug("trying future " + futureTask.taskLabel());
                try {
                    
                    future.get(250L, TimeUnit.MILLISECONDS);
                    // the future is now, reset the state of the NodeCommunication object
                    // so that it will be re-used
                    logger.debug("futureMap is done? " + future.isDone());
                    logger.debug(futureTask.taskLabel() + " Returned from the Future "
                            + ":(" + futureNodeComm.getNumber() + "):");

                    futureNodeComm.setState(NodeCommState.AVAILABLE);
                    removalList.add(future);
                    
                } catch (CancellationException ex) {
                    
                    logger.debug(futureTask.taskLabel() + " The Future has been cancelled "
                            + ":(" + futureNodeComm.getNumber() + "):");
                    
                    futureNodeComm.setState(NodeCommState.AVAILABLE);
                    removalList.add(future);

                } catch (ExecutionException ex) {

                    // this is a problem because we don't know which of the tasks 
                    // threw an exception!  Should we do anything special?
                    
                    logger.error(futureTask.taskLabel() + "An Exception is reported FROM the Future "
                            + ":(" + futureNodeComm.getNumber() + "):");
                    logger.error(futureTask.taskLabel() + ex.getMessage());
                    ex.printStackTrace();
                    
                    futureNodeComm.setState(NodeCommState.AVAILABLE);
                    removalList.add(future);
                    
                } catch (TimeoutException ex) {

                    logger.debug(futureTask.taskLabel() + " Waiting for the future " 
                            + ":(" + futureNodeComm.getNumber() + "):" + " since " 
                            + DateTimeMarshaller.serializeDateToUTC(futureNodeComm.getRunningStartDate()));
                    Date now = new Date();
                    // if the thread is running longer than an hour, kill it
                    if ((now.getTime() - futureNodeComm.getRunningStartDate().getTime()) > threadTimeout) {
                        logger.warn(futureTask.taskLabel() + " Cancelling. " 
                                + ":(" + futureNodeComm.getNumber() + "):" + " Waiting since " 
                                + DateTimeMarshaller.serializeDateToUTC(futureNodeComm.getRunningStartDate()));
                        if (future.cancel(true)) {
                            // this is a task we don't care about tracking.
                            // it is a 'special' task because we do not limit the number of NodeComms. they are not
                            // maintained by the nodeCommList. if the submit sync failed task fails
                            // we do not resubmit
                            NodeReference nodeReference = new NodeReference();
                            nodeReference.setValue(futureTask.getNodeId());
                            futureNodeComm.setState(NodeCommState.AVAILABLE);
                            submitSynchronizationFailed(futureTask, nodeReference);
                        } else {
                            logger.warn(futureTask.taskLabel() + " Unable to cancel the task");
                        }
                        //force removal from the thread pool. 
                        taskExecutor.getThreadPoolExecutor().remove(future);
                    }
                }
            }
            if (!removalList.isEmpty()) {
                for (Future key : removalList) {
                    futuresMap.remove(key);
                }
            }
        }
    }

    private void submitSynchronizationFailed(SyncObject task, NodeReference mnNodeId) {
        // we do not care about if this returns, because if this
        // submission fails, then it would be somewhat futile to
        // repeatedly try to submit failing submisssions...
        try {
            logger.info(task.taskLabel() + " Submit SyncFailed");
            NodeComm nodeCommunications = nodeCommunicationsFactory.getNodeComm(mnNodeId);
            SyncFailedTask syncFailedTask = new SyncFailedTask(nodeCommunications, task);

            FutureTask futureTask = new FutureTask(syncFailedTask);
            taskExecutor.execute(futureTask);
        } catch (TaskRejectedException ex) {
            // Tasks maybe rejected because of the pool has filled up and no
            // more tasks may be executed.
            // This situation is dire since it should never be allowed to occur!
            logger.error(task.taskLabel() + " Submit SyncFailed Rejected from MN");
            logger.error("ActiveCount: " + taskExecutor.getActiveCount() + " Pool size " 
                    + taskExecutor.getPoolSize() + " Max Pool Size " + taskExecutor.getMaxPoolSize());

        } catch (ServiceFailure ex) {
            ex.printStackTrace();
            logger.error(ex.getDescription());
        } catch (NodeCommUnavailable ex) {
            ex.printStackTrace();
            logger.error(ex.getMessage());
        }
    }

    // how do we or what actions  call shutdown on the executor such that no more tasks are created?
    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    public void setHazelcast(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
    }

    public ThreadPoolTaskExecutor getThreadPoolTaskExecutor() {
        return taskExecutor;
    }

    public void setThreadPoolTaskExecutor(ThreadPoolTaskExecutor taskExecutor) {
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
