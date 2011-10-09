/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.synchronization;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.synchronization.tasks.SyncObjectTask;

/**
 * Runs the SyncObjectTask runnable class as a single daemon threaded
 * executable.
 * 
 * @author waltz
 */
public class SyncObjectExecutor {
    Logger logger = Logger.getLogger(SyncObjectExecutor.class.getName());
    SyncObjectTask syncObjectTask;

    public void init() {
        ExecutorService executor = Executors.newSingleThreadExecutor(new DaemonFactory());
        boolean shouldContinueRunning = true;
        do {
            logger.debug("Starting SyncObjectExecutor");
            Future future = executor.submit(new FutureTask(syncObjectTask));
            try {
                future.get();
            } catch (InterruptedException ex) {
                logger.warn( ex.getMessage());
            } catch (ExecutionException ex) {
                shouldContinueRunning = false;
            } catch (Exception ex) {
                ex.printStackTrace();
                shouldContinueRunning = false;
            }
            if (future.isCancelled()) {
                shouldContinueRunning = false;
            }
        } while (shouldContinueRunning);
        executor.shutdown();

    }

    public SyncObjectTask getSyncObjectTask() {
        return syncObjectTask;
    }

    public void setSyncObjectTask(SyncObjectTask syncObjectTask) {
        this.syncObjectTask = syncObjectTask;
    }

    private class DaemonFactory implements ThreadFactory {

        public Thread newThread(Runnable task) {
            Thread thread = new Thread(task);
            thread.setDaemon(true);
            return thread;
        }
    }
}
