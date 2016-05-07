package org.dataone.cn.batch.synchronization.tasks;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.exceptions.ExecutionDisabledException;
import org.dataone.cn.batch.exceptions.RetryableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * A Callable for executing a Queue processing loop using a ThreadPoolTaskExecutor.
 * It is designed to run continuously until interrupted or is inactivated.  The 
 * class is responsible for smooth execution of tasks associated with the object 
 * Queue, including handling Queue size limits and ThreadPool capacity.
 * <p/>
 * Assumptions: - read-only Queue, other QueueProcessors, task failures, try-agains (w/ delay),
 * limited thread pool.

 * @author rnahf
 *
 * @param <E> The type of object on the queue
 * @param <V> The type of task to execute
 */
public abstract class QueueProcessorCallable<E,V> implements Callable<String> {

    Logger logger = Logger.getLogger(QueueProcessorCallable.class.getName());
    
    protected boolean inactivate = false;
    protected Queue<E> queue;
    protected DelayQueue<DelayWrapper<E>> pendingQueueItem = new DelayQueue<>();
    protected CircularFifoQueue<V> latestResults = new CircularFifoQueue<>(50);

    
//    @Autowired
    private static ThreadPoolTaskExecutor taskExecutor;
    
    private static final long EXECUTION_THREAD_TIMEOUT = 900000L; 
    

    
    public void setQueue(Queue<E> queue) {
        this.queue = queue;
    }
    
    public void setThreadPoolTaskExecutor(ThreadPoolTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }
    /**
     * runs the queue processor until interrupted.
     */
    @Override
    public String call() throws Exception {
        
        HashMap<FutureTask<V>,FutureStat> activeFutures = new HashMap<>();

        try {
            do {
                // first try to reap futures to free up executor resources
                reapFutures(activeFutures);

                if (isInactivated()) {
                    if (activeFutures.isEmpty() & pendingQueueItem.isEmpty()) {
                        // there is nothing else to do, so shut down the thread
                        logger.info("All Tasks are complete. Shutting down\n");
                        throw new ExecutionDisabledException();
                    }

                    if (pendingQueueItem.isEmpty())
                        // with no new items to process, we don't have to 
                        // reap futures as often.
                        interruptableSleep(3000L);
                }


                // look for new queue items to execute
                E queueItem = getNextItem();
                if (queueItem == null)
                    continue;
            
                // map the queue item to a Callable task
                Callable<V> callable = prepareTask(queueItem);

                // try to execute the callable (task)
                // if the executor is full, keep hold of the task for the
                // next iteration of our infinite loop.
                try {
                    FutureTask<V> scheduledFuture = scheduleExecution(callable);
                    
                    // we need to track executing futures
                    activeFutures.put(scheduledFuture, new FutureStat(new Date(),queueItem));
                    
                } catch (TaskRejectedException e)  {
                    // executor pool is full
                    // hold onto it for next loop-through
                    pendingQueueItem.add(new DelayWrapper(queueItem));

                    // sleep a bit because the thread pool is busy
                    interruptableSleep(2000L);
                }
                
                cancelStuckTasks(activeFutures);

            } while (true);


        } catch (InterruptedException ex) {
            // XXX determine which exceptions are acceptable for restart and which ones 
            // are truly fatal. otherwise could cause an infinite loop of continuous failures
            //
            ex.printStackTrace();
//            logger.error("Interrupted! by something " + ex.getMessage() + "\n");
            return "Interrupted";
        }
    }


    /**
     * retrieves the next item available to process, prioritizing available pending
     * items first.  Can return null.
     * @return
     * @throws InterruptedException
     */
    private E getNextItem() throws InterruptedException {

        // 
        DelayWrapper<E> delayWrapper =  pendingQueueItem.poll();
        if (delayWrapper != null) {
            // there's an item whose delay is over
            return (E) delayWrapper.getWrappedObject();
        }
        
        if (isInactivated()) {
            return null;
        
        } else if (queue instanceof BlockingQueue<?>) {
            return ((BlockingQueue<E>)queue).poll(250L, TimeUnit.MILLISECONDS);
        
        } else {
            return queue.poll();
        }
    }



    /**
     * 
     * @param callable
     * @return
     */
    private  FutureTask<V> scheduleExecution(Callable<V> callable) throws TaskRejectedException {

        FutureTask<V> futureTask = new FutureTask<V>(callable);
        taskExecutor.execute(futureTask);
        return futureTask;
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
    
    private void cancelStuckTasks(HashMap<FutureTask<V>,FutureStat> activeFutures) {

        if ((taskExecutor.getActiveCount()) >= taskExecutor.getPoolSize()) {
            if ((taskExecutor.getPoolSize() == taskExecutor.getMaxPoolSize()) && activeFutures.isEmpty()) {
                BlockingQueue<Runnable> blockingTaskQueue = taskExecutor.getThreadPoolExecutor().getQueue();
                Runnable[] taskArray = {};
                taskArray = blockingTaskQueue.toArray(taskArray);
                for (int j = 0; j < taskArray.length; ++j) {
                    taskExecutor.getThreadPoolExecutor().remove(taskArray[j]);
                    // XXX: should these be requeued?
                }
            }
            // try to remove cancelled tasks from the work queue
            taskExecutor.getThreadPoolExecutor().purge();

        }
        
    }
    
    
    /**
     * builds the repeatable task
     */
    protected abstract Callable<V> prepareTask(E queueItem); 
    
    protected abstract void cleanupTask(E queueItem);



    /**
     * Change the processing state of this instance
     * @param inactivate - true to inactivate, false to keep active.
     */
    protected void setIsInactivated(boolean inactivate) {
        logger.warn("Setting processor inactivation: " + inactivate);
        this.inactivate = inactivate;
    }
    
    /**
     * override this method (and possibly setIsInactivated) to set the 
     * activation status by properties
     * @return true if the processor has been inactivated 
     */
    protected boolean isInactivated() {
        return this.inactivate;
    }
    
    
    private void reapFutures(HashMap<FutureTask<V>,FutureStat> activeFutures) throws InterruptedException {


        // first check all the futures of past tasks to see if any have finished
        // XXX is this code doing anything useful?
        if (activeFutures.size() > 0) {
            logger.info("waiting on " + activeFutures.size() + " futures");
        } else {
            logger.debug("Polling empty queue");
        }
        if (!activeFutures.isEmpty()) {
            // XXX why make a list to do individual removes later, instead of removing them directly?
            ArrayList<Future<V>> removalList = new ArrayList<>();

            Iterator<FutureTask<V>> it = activeFutures.keySet().iterator();
            while (it.hasNext()) {
                FutureTask<V> future = it.next();
                try {
                    V outcome = future.get(250L, TimeUnit.MILLISECONDS);
                    latestResults.add(outcome);
                    cleanupTask(activeFutures.get(future).queueItem);
                    removalList.add(future);
                    
                }
                // handle situations where the task was canceled
                catch (CancellationException ex) {
                    cleanupTask(activeFutures.get(future).queueItem);
                    pendingQueueItem.add(new DelayWrapper<E>(activeFutures.get(future).queueItem, 0));
                    removalList.add(future);
                    
                } 
                // handle exceptions thrown from the Callable call method
                catch (ExecutionException ex) {
                    if (ex.getCause() != null && ex.getCause() instanceof RetryableException) {
                        logger.debug("Adding item to pendingQueue...");
                        pendingQueueItem.add(new DelayWrapper<E>(
                                activeFutures.get(future).queueItem,
                                ((RetryableException)ex.getCause()).getDelay()));
                    }
                    cleanupTask(activeFutures.get(future).queueItem);
                    removalList.add(future);
                    // XXX what else to do if there's an exception?  Do we want to support special exception handling
                }
                catch (TimeoutException ex) {
                    // no value returned from the Future
                    // this is ok, unless it's been processing for too long of a time
                    if ((new Date()).getTime() - activeFutures.get(future).start.getTime() > EXECUTION_THREAD_TIMEOUT) {
                        // cancel the future
                        if (future.cancel(true /* may interrupt */)) {
                            cleanupTask(activeFutures.get(future).queueItem);
                            removalList.add(future);
                        } else {
                            logger.warn("unable to cancel this future task!");
                        }
                        //force removal from the thread pool. 
                        if (taskExecutor.getThreadPoolExecutor().remove(future) ) {
                            // XXX:  do we need to do anything here?  Is this the same as future.cancel?
                        }
                    }
                }
            }
            if (!removalList.isEmpty()) {
                for (Future<V> key : removalList) {
                    activeFutures.remove(key);
                }
            }
        }
    }
 
    class FutureStat {
        Date start;
        E queueItem;
        
        FutureStat(Date start, E queueItem) {
            this.start = start;
            this.queueItem = queueItem;
        }
    }

}
