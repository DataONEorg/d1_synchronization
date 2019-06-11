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

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.dataone.cn.ComponentActivationUtility;
import org.dataone.cn.batch.synchronization.tasks.ObjectListHarvestTask;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.NodeReference;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Quartz Job that starts off the hazelcast distributed execution of harvesting for a nodeList from a Membernode It
 * executes only for a given membernode, and while executing excludes via a lock any other execution of a job on that
 * membernode It also sets and persists the LastHarvested date on a node after completion before releasing the lock
 *
 * @author waltz
 */
@DisallowConcurrentExecution
public class MemberNodeHarvestJob implements Job {

    static final Logger logger = Logger.getLogger(MemberNodeHarvestJob.class);
    @Override
    public void execute(JobExecutionContext jobContext) throws JobExecutionException {

        
        JobExecutionException jex = null;
        NodeReference nodeReference = new NodeReference();
        SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss zzz");

        String mnIdentifier = null;
        try {
            if (ComponentActivationUtility.synchronizationIsActive()) {
                mnIdentifier = jobContext.getMergedJobDataMap().getString("mnIdentifier");
                logger.info(mnIdentifier + " - ObjectListHarvestTask Start");

                nodeReference.setValue(mnIdentifier);

                Integer pageSize = Settings.getConfiguration().getInt("Synchronization.mn_listobjects_page_size");

                ObjectListHarvestTask harvestTask = new ObjectListHarvestTask(nodeReference, pageSize);

                Date lastProcessingCompletedDate = harvestTask.call();

                    // if the lastProcessingCompletedDate has changed then it should be persisted, but where?
                // Does not need to be stored, maybe just printed?
                if (lastProcessingCompletedDate == null) {
                    logger.info(mnIdentifier + " - ObjectListHarvestTask did not finish.");
                } else {
                    logger.info(mnIdentifier + " - ObjectListHarvestTask Completed at "
                            + format.format(lastProcessingCompletedDate));
                }
            } else {
                logger.warn(mnIdentifier + "-  ObjectListHarvestTask Disabled");
            }

        } catch (Exception ex) {
            logger.error(mnIdentifier + " - " + jobContext.getJobDetail().getKey().getName() + " died: " + ex.getMessage(), ex);
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
