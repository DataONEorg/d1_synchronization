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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.exceptions.ExecutionDisabledException;
import org.dataone.cn.batch.exceptions.NodeCommUnavailable;
import org.dataone.cn.batch.synchronization.NodeCommFactory;
import org.dataone.cn.batch.synchronization.NodeCommSyncObjectFactory;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.batch.synchronization.type.NodeCommState;
import org.dataone.cn.batch.synchronization.type.SyncObjectState;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.synchronization.types.SyncObject;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.util.DateTimeMarshaller;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.hazelcast.client.HazelcastClient;
import org.dataone.cn.ComponentActivationUtility;

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
 * SyncObjectTaskManager, and the call() method uses an infinite loop that gracefully
 * exits upon exception.
 * 
 * It is also designed/needs to inter-operate with other SyncObjectTasks from other
 * CN cluster members.
 *
 * @author waltz
 */
public class SyncObjectTask implements Callable<String> {

    static final Logger logger = Logger.getLogger(SyncObjectTask.class);
    private ThreadPoolTaskExecutor taskExecutor;

    private Integer maxNumberOfClientsPerMemberNode;
    private NodeCommFactory nodeCommunicationsFactory;
    private static final String nodecommName = "NODECOMM";
    private static final String taskName = "SYNCOBJECT";
    private static final long threadTimeout = Settings.getConfiguration()
            .getLong("Synchronization.SyncObjectTask.threadTimeout", 900000L); //900000L represents fifteen minutes

    private static long FUTURE_REAP_WAIT = Settings.getConfiguration().getLong("Synchronization.SyncObjectTask.reapFutureWait", 250L);
    private static CircularFifoQueue<SyncObjectState> latestResults = new CircularFifoQueue<>(50);
    private static long delayUntil = -1;
    /**
     * 
     * Method to be called in a separately executing thread.
     *
     * @return String
     * @throws ExecutionDisabledException 
     * @throws Exception
     */
    @Override
    public String call() throws ExecutionDisabledException {
        HazelcastClient hazelcast = HazelcastClientFactory.getProcessingClient();
        logger.info("Starting SyncObjectTask");

        String syncObjectQueue = Settings.getConfiguration().getString("dataone.hazelcast.synchronizationObjectQueue");
        BlockingQueue<SyncObject> hzSyncObjectQueue = hazelcast.getQueue(syncObjectQueue);

        HashMap<FutureTask<SyncObjectState>, HashMap<String, Object>> futuresMap = new HashMap<>();
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

        
        try {
            do {  // forever, (unless exception thrown)

                SyncObject task = null;
                //
                // (A). pull a task from the queue if synchronization is active and there is one
                //
                // Settings gets refreshed periodically           
                if (ComponentActivationUtility.synchronizationIsActive()) {
                    // get next item off the SyncObject queue
                    
                    task = hzSyncObjectQueue.poll(60L, TimeUnit.SECONDS);
                    
                    
                    
// XXX: this delayUntil was never fully implemented, (note that delayUntil value was never changed from the default -1)
// so commenting out for now.  The idea was to use the
// task.getSleepUntil() value to induce a sleep assuming that 
// any future tasks in the queue would have later task.getSleepUntils
//                    Long nowTime = (new Date()).getTime();
//                    if (nowTime > delayUntil) {
//                        task = hzSyncObjectQueue.poll(90L, TimeUnit.SECONDS);
//                    } 
//                    else if (nowTime + 500 > delayUntil) {
//                        // sleep instead of requeue
//                        interruptableSleep(delayUntil-nowTime);
//                        task = hzSyncObjectQueue.poll(90L, TimeUnit.SECONDS);
//                    } 
                    
                } else {
                                        
                    //
                    // (B).  Try to shutdown gracefully - exit if all tasks are complete,
                    //          and slow the infinite loop down, because there is not
                    //          a wait for tasks happening
                    //
                    // see if we can shutdown
                    if (futuresMap.isEmpty()) {
                        // ok futures Map is empty, no need to keep spinning here, shut this thread down
                        logger.info("All Tasks are complete. Shutting down\n");
                        throw new ExecutionDisabledException();
                    }
                    // slow down the rate of future-reaping while sync is not active
                    //
                    // we really don't want to slow down things when sync becomes 
                    // inactive, because it means that we wish to stop the process
                    // fairly quickly. -rpw
                    // 
                    // true, but since each task takes about 3-4 seconds to complete, we can avoid 
                    // unnecessary loops and many log statements while waiting.
                    interruptableSleep(1000L);
                }
                
                
                // (C).  try to free up executor threads and nodeComms
                
                // reapFutures serially waits FUTURE_REAP_WAIT ms per future, so with the number of futures limited
                // by number of nodeComms (defaluts to 5) * number of member nodes (limited to 100), there can be up to
                // 100 * FUTURE_REAP_WAIT ms imposed blocking time.
                // (although, unless a job is stuck, the upper limit is the processing time of a single task - since they
                // are all being processed in concurrently)

                reapFutures(futuresMap);

                // (D). try to get a NodeComm for the task and assign it to the executor
                
                if (task != null) {
                    boolean success = executeTransferObjectTask(task, futuresMap);
                    if (!success) {
                        logger.info(task.taskLabel() + " - requeueing task.");
                        hzSyncObjectQueue.put(task);
                        // if queue is short, try to pass off this task to another
                        // CN by sleeping a bit
                        if (hzSyncObjectQueue.size() < 3) {
                            interruptableSleep(5000L);
                        }
                    }
                }
                
                if (logger.isDebugEnabled()) {
                    logger.debug("ActiveCount: " + taskExecutor.getActiveCount() + "  Pool size: " 
                            + taskExecutor.getPoolSize() + "  Max Pool Size: " + taskExecutor.getMaxPoolSize());
                }
                
                
                // (E).  an orthogonal method to manage the taskExecutor (beyond the TaskRejectedException in executteTransferObjectTask method)
                //  
                // 
                
                if ((taskExecutor.getActiveCount()) >= taskExecutor.getPoolSize()) {
                    // removes tasks when the futuresMap is empty, and the executor is full
                    // (how effective is this?)
                    if ((taskExecutor.getPoolSize() == taskExecutor.getMaxPoolSize()) && futuresMap.isEmpty()) {
                        BlockingQueue<Runnable> blockingTaskQueue = taskExecutor.getThreadPoolExecutor().getQueue();
                        Runnable[] taskArray = {};
                        taskArray = blockingTaskQueue.toArray(taskArray);
                        for (int j = 0; j < taskArray.length; ++j) {
                            taskExecutor.getThreadPoolExecutor().remove(taskArray[j]);
                        }
                    }
                    // try to remove cancelled tasks from the work queue
                    taskExecutor.getThreadPoolExecutor().purge();
                }
            } while (true);
        } 
        catch (InterruptedException ex) {
            // XXX determine which exceptions are acceptable for restart and which ones 
            // are truly fatal. otherwise could cause an infinite loop of continuous failures

            logger.error("Interrupted! by something " + ex.getMessage() + "\n", ex);
            return "Interrupted";
        }
    }
  
    
    /**
     * (Straight from the ExecutorService javadoc)
     * The following method shuts down an ExecutorService in two phases, first by calling 
     * shutdown to reject incoming tasks, and then calling shutdownNow, if necessary, 
     * to cancel any lingering tasks
     * @param pool
     */
    private void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }



    
    /**
     * sleeps but wakes upon interruption (without failing)
     * @param millis
     */
    private void interruptableSleep(Long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException iex) {
            logger.debug("sleep interrupted");
        }
    }

    /**
     * Gathers resources for the task, if available, and gives it to the taskExecutor
     * to run. If accepted, adds the Future to the futuresMap.
     * @param task
     * @param futuresMap
     * @return true if a future is created and added to the map.
     */
    private boolean executeTransferObjectTask(SyncObject task, HashMap<FutureTask<SyncObjectState>, HashMap<String, Object>> futuresMap ) {
        
        if (task == null) {
            return true;
        }
        
        boolean isSuccess = false;
        try {
            // Found a task now see if there is a comm object available
            // if so, then run it
            logger.info(task.taskLabel() + " received");
            // investigate the task for membernode
            NodeReference nodeReference = new NodeReference();
            nodeReference.setValue(task.getNodeId());
            
            NodeComm nodeCommunications = NodeCommSyncObjectFactory.getInstance().getNodeComm(nodeReference);

            // finally, execute the new task!
            try {
                V2TransferObjectTask transferObject = new V2TransferObjectTask(nodeCommunications, task);
                FutureTask<SyncObjectState> futureTask = new FutureTask<SyncObjectState>(transferObject);
                taskExecutor.execute(futureTask);

                HashMap<String, Object> futureHash = new HashMap<String, Object>();
                futureHash.put(nodecommName, nodeCommunications);
                futureHash.put(taskName, task);
                futuresMap.put(futureTask, futureHash);
                logger.info(task.taskLabel() + " submitted for execution");
                isSuccess = true;
            } catch (TaskRejectedException ex) {
                // Tasks maybe rejected because of the executor's work queue has filled up 
                // and no more tasks may be executed.
                // TODO: Q. Is it the responsibility of this class to avoid this situation?
                // This situation is dire since it should never be allowed to occur!
                logger.error(task.taskLabel() + " Executor rejected the task");
                logger.error("ActiveCount: " + taskExecutor.getActiveCount() + " Pool size " 
                        + taskExecutor.getPoolSize() + " Max Pool Size " + taskExecutor.getMaxPoolSize());
                nodeCommunications.setState(NodeCommState.AVAILABLE);
            }
        } catch (NodeCommUnavailable | ServiceFailure ex) {
            logger.warn("No MN communication threads available at this time");
        }
        return isSuccess;
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
    private void reapFutures(HashMap<FutureTask<SyncObjectState>, HashMap<String, Object>> futuresMap) throws InterruptedException {

        // first check all the futures of past tasks to see if any have finished
        // XXX is this code doing anything useful?
        // the if statement below is to provide debugging messages to the log file
        if (futuresMap.size() > 0) {
            logger.info("waiting on " + futuresMap.size() + " futures");
        } else {
            logger.debug("Polling empty hzSyncObjectQueue");
        }
        if (!futuresMap.isEmpty()) {
            // XXX why make a list to do individual removes later, instead of removing them directly?
            // removing an object from the hash will cause an exception ConcurrentModificationException
            // to be thrown during  the for loop iteration
            // So, keep track of the items to remove, and remove them after iteration
            //
            // to remove in a loop, an listIterator could be used
            // http://stackoverflow.com/questions/10431981/remove-elements-from-collection-while-iterating
            //
            ArrayList<Future<SyncObjectState>> removalList = new ArrayList<>();

            for (FutureTask<SyncObjectState> future : futuresMap.keySet()) {
                HashMap<String, Object> futureHash = futuresMap.get(future);
                SyncObject futureTask = (SyncObject) futureHash.get(taskName);
                NodeComm futureNodeComm = (NodeComm) futureHash.get(nodecommName);
                logger.debug("trying future " + futureTask.taskLabel());
                try {
                    
                    // don't wait long, because we are in a loop.  
                    SyncObjectState futureOutcome = future.get(FUTURE_REAP_WAIT, TimeUnit.MILLISECONDS);
                    logger.info(futureTask.taskLabel() + " SyncObjectState: " + futureOutcome);
                    // retries are already requeued in (V2)TransferObjectTask, so nothing to do here
                    latestResults.add(futureOutcome);
                    // the future is now, reset the state of the NodeCommunication object
                    // so that it will be re-used
                    
                    if (logger.isDebugEnabled()) {
                        logger.debug("futureMap is done? " + future.isDone());
                    
                        logger.debug(futureTask.taskLabel() + " Returned from the Future "
                            + ":(" + futureNodeComm.getNumber() + "):");
                    }
                    futureNodeComm.setState(NodeCommState.AVAILABLE);
                    removalList.add(future);
                    
                    
                } catch (CancellationException ex) {
                    // XXX does canceling the future interrupt the processing task, and 
                    // do we need to requeue the task?
                    logger.debug(futureTask.taskLabel() + " The Future has been canceled "
                            + ":(" + futureNodeComm.getNumber() + "):");
                    
                    futureNodeComm.setState(NodeCommState.AVAILABLE);
                    removalList.add(future);

                } catch (ExecutionException ex) {

                    // this is a problem because we don't know which of the tasks 
                    // threw an exception!  Should we do anything special?
                    
                    logger.error(futureTask.taskLabel() + "An Exception is reported FROM the Future "
                            + ":(" + futureNodeComm.getNumber() + "):");
                    logger.error(futureTask.taskLabel() + ex.getMessage(), ex);

                    
                    futureNodeComm.setState(NodeCommState.AVAILABLE);
                    removalList.add(future);
                    
                } catch (TimeoutException ex) {

                    logger.debug(futureTask.taskLabel() + " Waiting for the future " 
                            + ":(" + futureNodeComm.getNumber() + "):" + " since " 
                            + DateTimeMarshaller.serializeDateToUTC(futureNodeComm.getRunningStartDate()));
                    Date now = new Date();
                    // if the thread is running longer than a specified time, kill it
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
                for (Future<SyncObjectState> key : removalList) {
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

            FutureTask<String> futureTask = new FutureTask<String>(syncFailedTask);
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
