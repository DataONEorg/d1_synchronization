/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.batch.synchronization.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.exceptions.ExecutionDisabledException;
import org.dataone.cn.batch.synchronization.NodeCommFactory;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.batch.synchronization.type.NodeCommState;
import org.dataone.cn.batch.synchronization.type.SyncObject;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.util.DateTimeMarshaller;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Reads from the hzSyncObjectQueue synchronization tasks that need to be performed.
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
    private ThreadPoolTaskExecutor taskExecutor;
    private HazelcastInstance hazelcast;
    private Integer maxNumberOfClientsPerMemberNode;
    private NodeCommFactory nodeCommunicationsFactory;
    private static final String nodecommName = "NODECOMM";
    private static final String taskName = "SYNCOBJECT";
    private static final long threadTimeout = Settings.getConfiguration().getLong("Synchronization.SyncObjectTask.threadTimeout", 900000L); //900000L represents fifteen minutes

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
        BlockingQueue<SyncObject> hzSyncObjectQueue = hazelcast.getQueue("hzSyncObjectQueue");
        IMap<NodeReference, Node> hzNodes = hazelcast.getMap("hzNodes");

        // maintain a List of NodeComms for each membernode that will not
        // exceed the maxNumberOfClientsPerMemberNode
        // each NodeComm will have a state, NodeCommState, that
        // will indicate if it is available for use by a future task
        Map<String, List<NodeComm>> initializedMemberNodes = new HashMap<String, List<NodeComm>>();

        // the futures map is helpful in tracking the state of a TransferObjectTask
        // nodecomm is need in order determine how long the comm channels have been running
        // and unavailable for use by any other task.
        // If the task has been running for over an hour, it is considered blocking and will
        // be terminated.  Once the task is terminated, the membernode will need to be
        // informed of the synchronization failure. Hence, the
        // futures map will also hold the SyncObject submitted to the TransferObjectTask
        HashMap<FutureTask, HashMap<String, Object>> futuresMap = new HashMap<FutureTask, HashMap<String, Object>>();

        SyncObject task = null;
        try {
            do {
                boolean activateJob = Boolean.parseBoolean(Settings.getConfiguration().getString("Synchronization.active"));
                if (activateJob) {
                    task = hzSyncObjectQueue.poll(90L, TimeUnit.SECONDS);
                } else {
                    // do not listen to Sync Object queue, just finish up with any active tasks
                    // clearning out the futures map
                    // to allow for eventual shutdown
                    if (futuresMap.isEmpty()) {
                        // ok futures Map is empty, no need to keep spinning here, shut this thread down
                        logger.info("All Tasks are complete. Shutting down\n");
                        throw new ExecutionDisabledException();
                    }
                    Thread.sleep(10000L); // ten seconds
                    task = null;
                }
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
                        logger.debug("trying future Task-" + futureTask.getNodeId() + "-" + futureTask.getPid());
                        try {
                            future.get(250L, TimeUnit.MILLISECONDS);
                            // the future is now, reset the state of the NodeCommunication object
                            // so that it will be re-used
                            logger.debug("futureMap is done? " + future.isDone());

                            logger.debug("Task-" + futureTask.getNodeId() + "-" + futureTask.getPid() + " Returned from the Future :"  + futureNodeComm.getNumber() + ":");

                            futureNodeComm.setState(NodeCommState.AVAILABLE);
                            removalList.add(future);
                        } catch (CancellationException ex) {
                            
                            logger.debug("Task-" + futureTask.getNodeId() + "-" + futureTask.getPid() + "The Future has been cancelled  " + ":(" + futureNodeComm.getNumber() + "):");
                            futureNodeComm.setState(NodeCommState.AVAILABLE);
                            removalList.add(future);

                        } catch (ExecutionException ex) {

                            // this is a problem because we don't know which of the tasks
                            // threw an exception!
                            // should we do anything special?
                            ex.printStackTrace();
                            logger.error(ex.getMessage());

                            futureNodeComm.setState(NodeCommState.AVAILABLE);
                            logger.error("Task-" + futureTask.getNodeId() + "-" + futureTask.getPid() + "An Exception is reported FROM the Future " + ":(" + futureNodeComm.getNumber() + "):");
                            futureNodeComm.setState(NodeCommState.AVAILABLE);
                            removalList.add(future);
                        } catch (TimeoutException ex) {
                            
                            logger.debug("Task-" + futureTask.getNodeId() + "-" + futureTask.getPid() + "Waiting for the future " + ":(" + futureNodeComm.getNumber() + "):" + " since " + DateTimeMarshaller.serializeDateToUTC(futureNodeComm.getRunningStartDate()));
                            Date now = new Date();
                            // if the thread is running longer than an hour, kill it
                            if ((now.getTime() - futureNodeComm.getRunningStartDate().getTime()) > threadTimeout) {
                                logger.warn("Task-" + futureTask.getNodeId() + "-" + futureTask.getPid() + " Cancelling. " + ":(" + futureNodeComm.getNumber() + "):" + " Waiting since " + DateTimeMarshaller.serializeDateToUTC(futureNodeComm.getRunningStartDate()));
                                if (future.cancel(true)) {
                                    // this is a task we don't care about tracking.
                                    // it is a 'special' task because we do not limit the number of NodeComms. they are not
                                    // maintained by the nodeCommList. if the submit sync failed task fails
                                    // we do not resubmit
                                    NodeReference nodeReference = new NodeReference();
                                    nodeReference.setValue(futureTask.getNodeId());
                                    submitSynchronizationFailed(futureTask, hzNodes.get(nodeReference).getBaseURL());
                                } else {
                                    logger.warn("Task-" + futureTask.getNodeId() + "-" + futureTask.getPid() + "Unable to cancel the task");
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

                if (task != null) {
                    // Found a task now see if there is a comm object available
                    // if so, then run it
                    logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " received");
                    NodeComm nodeCommunications = null;
                    // investigate the task for membernode
                    NodeReference nodeReference = new NodeReference();
                    nodeReference.setValue(task.getNodeId());
                    String memberNodeId = task.getNodeId();
                    // grab a membernode client off of the stack of initialized clients
                    if (initializedMemberNodes.containsKey(memberNodeId)) {
                        List<NodeComm> nodeCommList = initializedMemberNodes.get(memberNodeId);
                        // find a node comm that is not currently in use
                        for (NodeComm nodeComm : nodeCommList) {
                            if (nodeComm.getState().equals(NodeCommState.AVAILABLE)) {
                                nodeCommunications = nodeComm;
                                nodeCommunications.setState(NodeCommState.RUNNING);
                                nodeCommunications.setRunningStartDate(new Date());
                                break;
                            }
                        }
                        if (nodeCommunications == null) {
                            // no node Communications is available, see if we can create a new one
                            if (nodeCommList.size() <= maxNumberOfClientsPerMemberNode) {
                                try {
                                    // create and add a new one
                                    nodeCommunications = nodeCommunicationsFactory.getNodeComm(hzNodes.get(nodeReference).getBaseURL());

                                    nodeCommunications.setState(NodeCommState.RUNNING);
                                    nodeCommunications.setNumber(nodeCommList.size() + 1);
                                    nodeCommunications.setRunningStartDate(new Date());
                                    nodeCommList.add(nodeCommunications);
                                } catch (ServiceFailure ex) {
                                    ex.printStackTrace();
                                    logger.error(ex.getDescription());
                                }
                            }
                        }
                    } else {
                        // The memberNode hash does not contain an array
                        // that is assigned to this MemberNode
                        // create it, get a node comm, and put it in the hash
                        try {
                            List<NodeComm> nodeCommList = new ArrayList<NodeComm>();
                            nodeCommunications = nodeCommunicationsFactory.getNodeComm(hzNodes.get(nodeReference).getBaseURL());
                            nodeCommunications.setState(NodeCommState.RUNNING);
                            nodeCommunications.setNumber(nodeCommList.size() + 1);
                            nodeCommunications.setRunningStartDate(new Date());
                            nodeCommList.add(nodeCommunications);
                            initializedMemberNodes.put(memberNodeId, nodeCommList);
                        } catch (ServiceFailure ex) {
                            ex.printStackTrace();
                            logger.error(ex.getDescription());
                        }
                    }
                    if (nodeCommunications != null) {
                        // finally, execute the new task!
                        try {
                            TransferObjectTask transferObject = new TransferObjectTask(nodeCommunications, task);
                            FutureTask futureTask = new FutureTask(transferObject);
                            taskExecutor.execute(futureTask);
                            
                            HashMap<String, Object> futureHash = new HashMap<String, Object>();
                            futureHash.put(nodecommName, nodeCommunications);
                            futureHash.put(taskName, task);
                            futuresMap.put(futureTask, futureHash);
                            logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " submitted for execution");
                        } catch (TaskRejectedException ex) {
                            // Tasks maybe rejected because of the pool has filled up and no
                            // more tasks may be executed.
                            // This situation is dire since it should never be allowed to occur!
                            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + " Rejected");
                            logger.error("ActiveCount: " + taskExecutor.getActiveCount() + " Pool size " + taskExecutor.getPoolSize() + " Max Pool Size " + taskExecutor.getMaxPoolSize());
                            nodeCommunications.setState(NodeCommState.AVAILABLE);
                            hzSyncObjectQueue.put(task);
                            try {
                                Thread.sleep(10000L); // ten seconds
                            } catch (InterruptedException iex) {
                                logger.debug("sleep interrupted");
                            }
                        }
                    } else {
                        // Communication object is unavailable.  place the task back on the queue
                        // and sleep for 10 seconds
                        // maybe another node will have
                        // capacity to pick it up
                        logger.warn("No MN communication threads available at this time");
                        hzSyncObjectQueue.put(task);
                        try {
                            Thread.sleep(10000L); // ten seconds
                        } catch (InterruptedException ex) {
                            logger.debug("sleep interrupted");
                        }
                    }
                }
                logger.debug("ActiveCount: " + taskExecutor.getActiveCount() + " Pool size " + taskExecutor.getPoolSize() + " Max Pool Size " + taskExecutor.getMaxPoolSize());
                if ((taskExecutor.getPoolSize() + 10) > taskExecutor.getMaxPoolSize()) {
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

    private void submitSynchronizationFailed(SyncObject task, String mnBaseUrl) {
        // we do not care about if this returns, because if this
        // submission fails, then it would be somewhat futile to
        // repeatedly try to submit failing submisssions...
        try {
            logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Submit SyncFailed");
            NodeComm nodeCommunications = nodeCommunicationsFactory.getNodeComm(mnBaseUrl);
            SyncFailedTask syncFailedTask = new SyncFailedTask(nodeCommunications, task);

            FutureTask futureTask = new FutureTask(syncFailedTask);
            taskExecutor.execute(futureTask);
        } catch (TaskRejectedException ex) {
            // Tasks maybe rejected because of the pool has filled up and no
            // more tasks may be executed.
            // This situation is dire since it should never be allowed to occur!
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + " Submit SyncFailed Rejected from MN");
            logger.error("ActiveCount: " + taskExecutor.getActiveCount() + " Pool size " + taskExecutor.getPoolSize() + " Max Pool Size " + taskExecutor.getMaxPoolSize());

        } catch (ServiceFailure ex) {
           ex.printStackTrace();
           logger.error(ex.getDescription());
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
