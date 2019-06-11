/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.synchronization.jobs;

import com.hazelcast.client.HazelcastClient;
import java.util.concurrent.BlockingQueue;
import org.apache.log4j.Logger;
import org.dataone.cn.ComponentActivationUtility;
import org.dataone.cn.batch.synchronization.tasks.SyncMetricLogReport;
import org.dataone.cn.batch.synchronization.type.SyncQueueFacade;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.synchronization.types.SyncObject;
import org.dataone.configuration.Settings;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Periodically report on synchronization statistics that can be calculated independently of other running job
 * threads/tasks
 *
 * Runs as a Quartz Job
 *
 * @author waltz
 */
@DisallowConcurrentExecution
public class SyncMetricLogJob implements Job {

    static final Logger logger = Logger.getLogger(SyncMetricLogJob.class);
//    static final String syncObjectQueue = Settings.getConfiguration().getString("dataone.hazelcast.synchronizationObjectQueue");
    
    static final SyncMetricLogReport syncMetricLogReport = new SyncMetricLogReport();
    @Override
    public void execute(JobExecutionContext jobContext) throws JobExecutionException {

        JobExecutionException jex = null;

        try {
logger.debug("start");            
            if (ComponentActivationUtility.synchronizationIsActive()) {
//                HazelcastClient hazelcast = HazelcastClientFactory.getProcessingClient();
//                BlockingQueue<SyncObject> hzSyncObjectQueue = hazelcast.getQueue(syncObjectQueue);
                SyncQueueFacade hzSyncObjectQueue = new SyncQueueFacade();
                syncMetricLogReport.reportSyncMetrics(hzSyncObjectQueue);
            } else {
                logger.warn("SyncMetricLogJob Disabled");
            }
logger.debug("end");
        } catch (Exception ex) {
            logger.error(jobContext.getJobDetail().getKey().getName() + " died: " + ex.getMessage(), ex);
            // log this message, someone else has the lock (and they probably shouldn't)
            jex = new JobExecutionException();
            jex.unscheduleFiringTrigger();
            jex.setStackTrace(ex.getStackTrace());
        }

        if (jex != null) {
            throw jex;
        }

    }
}
