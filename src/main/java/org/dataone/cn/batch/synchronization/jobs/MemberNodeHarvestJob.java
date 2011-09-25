/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.synchronization.jobs;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.batch.synchronization.tasks.ObjectListHarvestTask;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Quartz Job that starts off the hazelcast distributed execution of harvesting for a nodeList
 * from a Membernode
 * It executes only for a given membernode, and while executing excludes via a lock
 * any other execution of a job on that membernode
 * It also sets and persists the LastHarvested date on a node after completion
 * before releasing the lock
 *
 * @author waltz
 */
@DisallowConcurrentExecution
public class MemberNodeHarvestJob implements Job {

    @Override
    public void execute(JobExecutionContext jobContext) throws JobExecutionException {
        Log logger = LogFactory.getLog(MemberNodeHarvestJob.class);
        String mnIdentifier = jobContext.getMergedJobDataMap().getString("mnIdentifier");
        try {
            NodeReference nodeReference = new NodeReference();
            nodeReference.setValue(mnIdentifier);
            Integer batchSize = Settings.getConfiguration().getInt("Synchronization.mn_listobjects_batch_size");

            logger.debug("executing for " + mnIdentifier + " with batch size " + batchSize);
            HazelcastInstance hazelcast = Hazelcast.getDefaultInstance();

            IMap<NodeReference, Node> hzNodes = hazelcast.getMap("hzNodes");

            Node mnNode = hzNodes.tryLockAndGet(nodeReference, 5L, TimeUnit.SECONDS);

            ObjectListHarvestTask harvestTask = new ObjectListHarvestTask(nodeReference, batchSize);
            ExecutorService executor = Hazelcast.getExecutorService();
            DistributedTask dtask = new DistributedTask((Callable<Date>) harvestTask);
            Future future = executor.submit(dtask);
            Date lastUpdateDate = null;
            try {
                lastUpdateDate = (Date) future.get();
            } catch (InterruptedException ex) {
                logger.error(ex.getMessage());
            } catch (ExecutionException ex) {
                logger.error(ex.getMessage());
            }

            hzNodes.unlock(nodeReference);

            // if the lastUpdateDate has changed then it should be persisted

        } catch (TimeoutException ex) {
            logger.warn(jobContext.getJobDetail().getDescription() + " is locked from running " + mnIdentifier);
            // log this message, someone else has the lock (and they probably shouldn't)
        } catch (Exception ex) {
            logger.error(jobContext.getJobDetail().getDescription() + " died: " + ex.getMessage());
            // log this message, someone else has the lock (and they probably shouldn't)
            JobExecutionException jex = new JobExecutionException();
            jex.unscheduleFiringTrigger();
            jex.setStackTrace(ex.getStackTrace());
            throw jex;
        }

    }
}
