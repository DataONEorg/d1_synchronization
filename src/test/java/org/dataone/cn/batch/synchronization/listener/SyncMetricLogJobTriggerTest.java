/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.synchronization.listener;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.log4j.Logger;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author waltz
 */
public class SyncMetricLogJobTriggerTest {
    static final Logger logger = Logger.getLogger(SyncMetricLogJobTriggerTest.class);
    
    @Test
    public void testLocking() throws InterruptedException {
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
        SyncMetricLogJobTriggerListener syncMetricLogJobTrigger = new SyncMetricLogJobTriggerListener();
        assertTrue(syncMetricLogJobTrigger.vetoJobExecution(null, null));
        boolean locked = true;
        TestUnlockTask testUnlockTask = new TestUnlockTask();
        Future futureUnlockTask = singleThreadExecutor.submit(testUnlockTask);
        while (locked) {
            try {
                futureUnlockTask.get(425, TimeUnit.MILLISECONDS);
                locked = false;
            } catch (ExecutionException ex) {
                fail(ex.getMessage());
            } catch (TimeoutException ex) {
                assertFalse(syncMetricLogJobTrigger.vetoJobExecution(null, null));
                logger.info(ex);
            }
        }
        assertTrue(syncMetricLogJobTrigger.vetoJobExecution(null, null));
        SyncMetricLogJobTriggerListener.releaseJob();
        singleThreadExecutor.shutdown();
    }
    
    private class TestUnlockTask implements Runnable {


        @Override
        public void run() {
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException ex) {
                logger.info(ex,ex);
            }
            SyncMetricLogJobTriggerListener.releaseJob();
        }
    }
}
