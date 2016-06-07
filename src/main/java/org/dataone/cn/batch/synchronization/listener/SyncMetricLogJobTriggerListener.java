/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.synchronization.listener;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.quartz.TriggerListener;

/**
 * SyncMetricLogJob does not allow concurrent execution of jobs.
 * 
 * But instead of rejecting the a job when the @DisallowConcurrentExecution
 * annotation is set, quartz just queues them up
 * for later execution
 * 
 * The condition leads to a massive job backup queue, If the job is
 * long running.
 * 
 * So, If a SyncMetricLogJob is running, disallow the trigger from
 * any further processing by vetoJobExecution.  
 * vetoJobExecution is called by the Scheduler when a Trigger has fired, 
 * and it's associated JobDetail is about to be executed. 
 * If the implementation vetos the execution 
 * (via returning true), the job's execute method will not be called. 
 * http://www.quartz-scheduler.org/api/2.2.1/org/quartz/TriggerListener.html
 * 
 * @author waltz
 */
public class SyncMetricLogJobTriggerListener implements TriggerListener{
    private static final String name = "SyncMetricLogJobTriggerListener";
    private static BlockingQueue<Object> lockJobQueue = new ArrayBlockingQueue<>(1);
    private static final Object lock = new Object();
    private static Logger logger = Logger.getLogger(SyncMetricLogJobTriggerListener.class);


    public String getName() {
        return name;
    }

    /* according to QuartzSchedulerThread and JobRunShell in Quartz 2.17
      triggerComplete will not fire if the Trigger has been vetoed
    */
    public void triggerComplete(Trigger trigger, JobExecutionContext context,
            Trigger.CompletedExecutionInstruction triggerInstructionCode) {
        // The trigger will create a lock when evaluating if 
        // the job should be vetoed useing vetoJobExecution
        // vetoJobExecution will return true if it can obtain
        // the lock or false if lock is already held
        //
        // If the Job completes and is not vetoed then
        // This method releases the lock so that other
        // trigger threads might obtain the lock again so
        // as to call this job.
        releaseJob();
        logger.debug("triggersComplete " );

    }

    public void triggerFired(Trigger trigger, JobExecutionContext context) {
                // do something with the event
          logger.debug("triggerFired");

    }

    public void triggerMisfired(Trigger trigger) {
        logger.debug("triggerMisFired");
    }
        // This trigger will create a lock when evaluating if 
        // the job should be vetoed useing vetoJobExecution
        // vetoJobExecution will return true if it can obtain
        // the lock or false if lock is already held
        // When SyncMetricLogJob completes its execution,
        // it releases the lock so that other
        // trigger threads might obtain the lock again so
        // as to call this job.
    public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {
        //
        // if the job is not locked, then try to lock it
        logger.debug("vetoJobExecution");
        return lockJob();
    }

        
    
    // this is to be called when SyncMetricLogJob has completed
    // it's run
    private void  releaseJob() {
        synchronized(lock){
            logger.debug("releaseJob");
            lockJobQueue.poll();
        }
    }
    // this is to be called to evaluate whether a trigger
    // should veto its job execution
    private  boolean  lockJob() {
        boolean locked;
        synchronized(lock){
            locked =  lockJobQueue.offer(lock);
        }
        logger.debug("lockJob is " + locked + " queue size is " + lockJobQueue.size());
        return locked;
    }
}
