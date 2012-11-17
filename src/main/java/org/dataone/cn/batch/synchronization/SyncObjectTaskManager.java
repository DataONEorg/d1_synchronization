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
package org.dataone.cn.batch.synchronization;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.exceptions.ExecutionDisabledException;
import org.dataone.cn.batch.synchronization.tasks.SyncObjectTask;
import org.dataone.configuration.Settings;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

/**
 * Manages the SyncObjectTask runnable class as a single daemon threaded executable. 
 * If the SyncObjectTask should return from execution,
 * evaluate the conditions by which it returned and determine
 * if the task may be executed again.
 *
 * TODO: If the SyncObjectTaskManager  dies, the entire synchronization process should
 * be shutdown and a report sent to someone about its failure!
 * 
 * @author waltz
 */
public class SyncObjectTaskManager implements Runnable {

    Logger logger = Logger.getLogger(SyncObjectTaskManager.class.getName());
    SyncObjectTask syncObjectTask;
    private SimpleAsyncTaskExecutor taskExecutor;
    // syncObjectTaskManagerFuture future probabaly is not needed,
    // but maybe it will force the executor to remove the thread (???)
    Future syncObjectTaskManagerFuture = null;

    public void init() {
        syncObjectTaskManagerFuture = taskExecutor.submit(this);
    }

    /**
     * Method to be called in a separately executing thread.
     *
     */
    @Override
    public void run() {
        // shouldContinueRunning decides if an exception is fatal to the 
        // running of this thread
        boolean shouldContinueRunning = true;
        // activateJob decides if the task should be submitted
        // therefore, this thread may be running, but not waiting on a
        // task
       
        do {
             boolean activateJob = Boolean.parseBoolean(Settings.getConfiguration().getString("Synchronization.active"));
            if (activateJob) {
                
                logger.debug("Starting SyncObjectTaskManager");
                FutureTask futureTask = new FutureTask(syncObjectTask);
                taskExecutor.execute(futureTask);

                try {
                    futureTask.get();
                } catch (InterruptedException ex) {
                    logger.warn(ex.getMessage());
                } catch (ExecutionException ex) {
                    if (ex.getCause() instanceof ExecutionDisabledException) {
                    logger.warn("Excecution Disabled continue polling until shutdown");
                    shouldContinueRunning = true; 
                    } else {
                    ex.printStackTrace();
                    shouldContinueRunning = false;
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    shouldContinueRunning = false;
                }
                if (futureTask.isCancelled()) {
                    logger.warn("SyncObjectTask was cancelled");
                    shouldContinueRunning = false;
                } else {
                    // An InterruptedException occurred that was not
                    // a task cancellation, so cancel the task and
                    // resubmitt
                    futureTask.cancel(true);
                }
            } else {
                logger.debug("SyncObjectTaskManager is disabled");
                try {
                    Thread.sleep(30000L);
                } catch (InterruptedException ex1) {
                    logger.warn(ex1.getMessage());
                    shouldContinueRunning = false;
                }
            }
        } while (shouldContinueRunning);
        syncObjectTaskManagerFuture.cancel(true);
    }

    public SyncObjectTask getSyncObjectTask() {
        return syncObjectTask;
    }

    public void setSyncObjectTask(SyncObjectTask syncObjectTask) {
        this.syncObjectTask = syncObjectTask;
    }

    public SimpleAsyncTaskExecutor getTaskExecutor() {
        return taskExecutor;
    }

    public void setTaskExecutor(SimpleAsyncTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }
}
