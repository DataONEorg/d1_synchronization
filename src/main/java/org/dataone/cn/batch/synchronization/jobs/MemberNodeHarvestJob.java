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
import java.text.SimpleDateFormat;
import java.text.ParseException;
import org.dataone.cn.batch.exceptions.ExecutionDisabledException;
import org.dataone.cn.hazelcast.HazelcastInstanceFactory;

/**
 * Quartz Job that starts off the hazelcast distributed execution of harvesting for a nodeList from a Membernode It
 * executes only for a given membernode, and while executing excludes via a lock any other execution of a job on that
 * membernode It also sets and persists the LastHarvested date on a node after completion before releasing the lock
 *
 * @author waltz
 */
@DisallowConcurrentExecution
public class MemberNodeHarvestJob implements Job {

    @Override
    public void execute(JobExecutionContext jobContext) throws JobExecutionException {

        Log logger = LogFactory.getLog(MemberNodeHarvestJob.class);
        JobExecutionException jex = null;
        NodeReference nodeReference = new NodeReference();
        SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss zzz");
        boolean nodeLocked = false;
        IMap<NodeReference, Node> hzNodes = null;

        try {
            boolean activateJob = Boolean.parseBoolean(Settings.getConfiguration().getString("Synchronization.active"));
            if (activateJob) {
                String mnIdentifier = jobContext.getMergedJobDataMap().getString("mnIdentifier");
                logger.info(mnIdentifier + "- ObjectListHarvestTask Start");
                HazelcastInstance hazelcast = HazelcastInstanceFactory.getProcessingInstance();

                hzNodes = hazelcast.getMap("hzNodes");

                nodeReference.setValue(mnIdentifier);

                Integer batchSize = Settings.getConfiguration().getInt("Synchronization.mn_listobjects_batch_size");

                nodeLocked = hzNodes.tryLock(nodeReference, 5L, TimeUnit.SECONDS);
                if (nodeLocked) {

                    ObjectListHarvestTask harvestTask = new ObjectListHarvestTask(nodeReference, batchSize);
                    ExecutorService executor = Hazelcast.getExecutorService();
                    DistributedTask dtask = new DistributedTask((Callable<Date>) harvestTask);
                    Future future = executor.submit(dtask);
                    Date lastProcessingCompletedDate = null;
                    try {
                        lastProcessingCompletedDate = (Date) future.get();
                    } catch (ExecutionDisabledException ex) {
                        logger.error("ExecutionDisabledException: " + ex.getMessage() + "\n\t\tExecutionDisabledException: Will fire Job again\n");
                        jex = new JobExecutionException();
                        jex.setRefireImmediately(true);
                        Thread.sleep(5000L);
                    } catch (InterruptedException ex) {
                        logger.error("InterruptedException: " + ex.getMessage());
                    } catch (ExecutionException ex) {
                        if (ex.getCause() instanceof ExecutionDisabledException) {
                            logger.error("ExecutionDisabledException: " + ex.getMessage() + "\n\tExecutionDisabledException: Will fire Job again\n");
                            jex = new JobExecutionException();
                            jex.setStackTrace(ex.getStackTrace());
                            jex.setRefireImmediately(true);
                            Thread.sleep(5000L);
                        } else {
                            logger.error("ExecutionException: " + ex.getMessage());
                        }
                    }

                    // if the lastProcessingCompletedDate has changed then it should be persisted, but where?
                    // Does not need to be stored, maybe just printed?
                    if (lastProcessingCompletedDate == null) {
                        logger.info(mnIdentifier + "- ObjectListHarvestTask did not finish.");
                    } else {
                        logger.info(mnIdentifier + "- ObjectListHarvestTask End at " + format.format(lastProcessingCompletedDate));
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error(jobContext.getJobDetail().getKey().getName() + " died: " + ex.getMessage());
            // log this message, someone else has the lock (and they probably shouldn't)
            jex = new JobExecutionException();
            jex.unscheduleFiringTrigger();
            jex.setStackTrace(ex.getStackTrace());
        } finally {
            if (nodeLocked) {
                hzNodes.unlock(nodeReference);
            }
        }
        if (jex != null) {
            throw jex;
        }

    }
}
