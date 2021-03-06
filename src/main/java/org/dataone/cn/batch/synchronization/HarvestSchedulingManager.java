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

import com.hazelcast.client.HazelcastClient;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.dataone.cn.batch.synchronization.jobs.MemberNodeHarvestJob;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeState;
import org.dataone.service.types.v1.NodeType;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.io.File;

import org.dataone.client.auth.CertificateManager;
import org.dataone.cn.batch.service.v2.NodeRegistrySyncService;
import org.dataone.cn.batch.service.v2.impl.NodeRegistrySyncServiceImpl;
import org.dataone.cn.batch.synchronization.jobs.SyncMetricLogJob;
import org.dataone.cn.batch.synchronization.listener.SyncMetricLogJobTriggerListener;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v2.NodeList;
import org.joda.time.DateTime;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import org.quartz.TriggerListener;
import org.quartz.impl.matchers.KeyMatcher;

/**
 * this bean must be managed by Spring upon startup of spring it will execute via init method
 *
 * evaluate whether the NodeList contains nodes that should be executed on the executing coordinating node. it will add
 * or remove triggers for jobs based on events, such as startup, nightly refactoring, more CNs coming online
 *
 * XXX todo: add in nightly job that re-calcuates jobs evaluate if EntryListener and MigrationListeners create potential
 * for race conditions
 *
 * @author waltz
 */
public class HarvestSchedulingManager implements ApplicationContextAware {

    static final Logger logger = Logger.getLogger(HarvestSchedulingManager.class);
    // Quartz GroupName for Jobs and Triggers, should be unique for a set of jobs that are related
    private static String groupName = "MemberNodeHarvesting";
    private static String metricLogGroupName = "MetricLogJobReporting";
    private static String metricLogTriggerName = "MetricLogJobTrigger";
    private static String metricLogJobName = "job-MetricLog";

    private Scheduler scheduler;
    ApplicationContext applicationContext;

    private NodeRegistrySyncService nodeRegistryService = new NodeRegistrySyncServiceImpl();
    private static String clientCertificateLocation
            = Settings.getConfiguration().getString("D1Client.certificate.directory")
            + File.separator + Settings.getConfiguration().getString("D1Client.certificate.filename");

    /*
     /* 
     * Called by Spring to bootstrap Synchronization
     * 
     * it will initialize Quartz
     * it will schedule membernodes for harvesting
     * 
     * it also adds a listener for changes in the hazelcast Nodes map and hz partitioner
     * Change in hzNodes or migration of partitions may entail rebalancing of quartz jobs

     * it populates the hzNodes map
     *
     *
     */
    public void init() {
        try {
            HazelcastClient hazelcast = HazelcastClientFactory.getProcessingClient();
            logger.info("HarvestingScheduler starting up");
            CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);

            Properties properties = new Properties();
            properties.load(this.getClass().getResourceAsStream("/org/dataone/configuration/synchQuartz.properties"));
            StdSchedulerFactory schedulerFactory = new StdSchedulerFactory(properties);
            scheduler = schedulerFactory.getScheduler();

            this.manageHarvest();
            this.scheduleSyncMetricLogJob();
        } catch (IOException ex) {
            throw new IllegalStateException("Loading properties file failedUnable to initialize jobs for scheduling: " + ex.getMessage());
        } catch (SchedulerException ex) {
            throw new IllegalStateException("Unable to initialize jobs for scheduling: " + ex.getMessage());
        }
    }

    public void halt() throws SchedulerException {
        scheduler.shutdown(true);
    }

    /**
     * will perform the recalculation of the scheduler. if scheduler is running, it will be disabled All jobs will be
     * deleted for this node All nodes that are considered 'local' by hazelcast will be scheduled with synchronization
     * jobs
     *
     * Seems that the listeners could call this in parallel, and it should be an atomic operation, so it is synchronized
     */
    public synchronized void manageHarvest() throws SchedulerException {
        if (!scheduler.isShutdown()) {
            try {

                // halt all operations
                if (scheduler.isStarted()) {
                    scheduler.standby();
                    while (!(scheduler.getCurrentlyExecutingJobs().isEmpty())) {
                        try {
                            Thread.sleep(2000L);
                        } catch (InterruptedException ex) {
                            logger.warn("Sleep interrupted. check again!");
                        }
                    }
                    // remove any existing jobs
                    GroupMatcher<JobKey> groupMatcher = GroupMatcher.groupEquals(groupName);
                    Set<JobKey> jobsInGroup = scheduler.getJobKeys(groupMatcher);

                    for (JobKey jobKey : jobsInGroup) {
                        logger.info("deleting job " + jobKey.getGroup() + " " + jobKey.getName());
                        scheduler.deleteJob(jobKey);
                    }
                }
                // populate the nodeList

                NodeList nodeList = nodeRegistryService.listNodes();
                logger.info("Node map has " + nodeList.sizeNodeList() + " entries");
                // construct new jobs and triggers based on ownership of nodes in the nodeList
                for (Node node : nodeList.getNodeList()) {
                    // exclude from the set any CNs or membernodes that are down or do not
                    // want to be synchronized

                    addHarvest(node.getIdentifier(), node);
                }
                scheduler.start();

                if (scheduler.isStarted()) {
                    logger.info("Scheduler is started");
                }
            } catch (NotImplemented | ServiceFailure ex) {
                logger.error(ex, ex);
                throw new IllegalStateException("Unable to initialize jobs for scheduling: " + ex.getMessage());
            }
        } else {
            logger.warn("Scheduler has been shutdown. Synchronization is not running");
        }
    }

    /*
     *
     * Convert the values in the nodes' sychronization schedule into a valid crontab entry to be used by Quartz
     *
     * @param Node @return String of crontab
     *
     */
    private String getCrontabEntry(Node node) {
        String seconds = node.getSynchronization().getSchedule().getSec();
        seconds = seconds.replace(" ", "");
        String minutes = node.getSynchronization().getSchedule().getMin();
        minutes = minutes.replace(" ", "");
        String hours = node.getSynchronization().getSchedule().getHour();
        hours = hours.replace(" ", "");
        String days = node.getSynchronization().getSchedule().getMday();
        days = days.replace(" ", "");
        String months = node.getSynchronization().getSchedule().getMon();
        months = months.replace(" ", "");
        String weekdays = node.getSynchronization().getSchedule().getWday();
        weekdays = weekdays.replace(" ", "");
        String years = node.getSynchronization().getSchedule().getYear();
        years = years.replace(" ", "");
        if (days.equalsIgnoreCase("?") && weekdays.equalsIgnoreCase("?")) {
            // both can not be ?
            days = "*";
        } else if (!(days.equalsIgnoreCase("?")) && !(weekdays.equalsIgnoreCase("?"))) {
            // if both of them are set to something other than ?
            // then one of them needs to be ?
            // if one of them is set as * while the other is not
            if (days.equalsIgnoreCase("*") && weekdays.equalsIgnoreCase("*")) {
                weekdays = "?";
            } else if (days.equalsIgnoreCase("*")) {
                days = "?";
            } else {
                weekdays = "?";
            }
        }
        String crontab = seconds + " " + minutes + " " + hours + " " + days + " " + months + " " + weekdays + " " + years;
        return crontab;
    }

    /*
     * Create the specific Trigger and Job that should be executed by Quartz
     * only if the MN is UP and available for synchronization
     * 
     * @param NodeReference 
     * @param Node
     *
     */
    private void addHarvest(NodeReference key, Node node) {
        if (node.getState().equals(NodeState.UP)
                && node.isSynchronize() && node.getType().equals(NodeType.MN)) {

            String crontabEntry = this.getCrontabEntry(node);

            // the current mn node is owned by this hazelcast cn node member
            // so schedule a job based on the settings of the node
            JobKey jobKey = new JobKey("job-" + key.getValue(), groupName);
            try {
                if (!scheduler.checkExists(jobKey)) {
                    JobDetail job = newJob(MemberNodeHarvestJob.class).withIdentity(jobKey).usingJobData("mnIdentifier", key.getValue()).build();
                    TriggerKey triggerKey = new TriggerKey("trigger-" + key.getValue(), groupName);
                    Trigger trigger = newTrigger().withIdentity(triggerKey).startNow().withSchedule(cronSchedule(crontabEntry)).build();
                    logger.info("scheduling  key " + key.getValue() + " with schedule " + crontabEntry);
                    scheduler.scheduleJob(job, trigger);
                } else {
                    logger.error("job-" + key.getValue() + " exists!");
                }
            } catch (SchedulerException ex) {
                logger.error("Unable to initialize job key " + key.getValue() + " with schedule " + crontabEntry + "for scheduling: ", ex);
            }

        }
    }

    private void scheduleSyncMetricLogJob() {
        JobKey jobKey = new JobKey(metricLogJobName, metricLogGroupName);
        try {
            if (!scheduler.checkExists(jobKey)) {
                DateTime startTime = new DateTime().plusSeconds(30);
                JobDetail job = newJob(SyncMetricLogJob.class).withIdentity(jobKey).build();
                TriggerKey triggerKey = new TriggerKey(metricLogTriggerName, metricLogGroupName);

               
                Trigger trigger = newTrigger().withIdentity(triggerKey)
                        .startAt(startTime.toDate()) // if a start time is not given (if this line were omitted), "now" is implied
                        .withSchedule(simpleSchedule()
                                .withIntervalInSeconds(60)
                                .repeatForever()
                                .withMisfireHandlingInstructionIgnoreMisfires()) // note that 10 repeats will give a total of 11 firings
                        .forJob(job) // identify job with handle to its JobDetail itself                   
                        .build();
                logger.info("scheduling  key " + jobKey + " with schedule ");
                scheduler.scheduleJob(job, trigger);
                TriggerListener triggerListener = new SyncMetricLogJobTriggerListener();
                scheduler.getListenerManager().addTriggerListener(triggerListener, KeyMatcher.keyEquals(triggerKey));
            } else {
                logger.error(metricLogJobName + " exists!");
            }
        } catch (SchedulerException ex) {
            logger.error("Unable to initialize job " + metricLogJobName + " for scheduling: ", ex);
        }
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

}
