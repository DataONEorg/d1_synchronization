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

import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.core.Hazelcast;
import java.util.List;
import java.util.ArrayList;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.core.Member;
import org.dataone.service.types.v1.NodeReference;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.NodeState;
import org.dataone.service.types.v1.Node;
import org.quartz.impl.StdSchedulerFactory;
import java.io.IOException;
import java.util.Properties;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import com.hazelcast.core.IMap;
import org.quartz.JobDetail;
import org.dataone.cn.batch.synchronization.jobs.MemberNodeHarvestJob;
import com.hazelcast.core.HazelcastInstance;
import java.text.ParseException;
import java.util.Set;
import org.dataone.cn.hazelcast.HazelcastLdapStore;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import static org.quartz.TriggerBuilder.*;
import static org.quartz.CronScheduleBuilder.*;

import static org.quartz.JobBuilder.*;

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
public class HarvestSchedulingManager implements ApplicationContextAware, EntryListener<NodeReference, Node>, MigrationListener {

    public static Log logger = LogFactory.getLog(HarvestSchedulingManager.class);
    // Quartz GroupName for Jobs and Triggers, should be unique for a set of jobs that are related
    private static String groupName = "MemberNodeHarvesting";
    private HazelcastInstance hazelcast;
    private HazelcastLdapStore hazelcastLdapStore;
    private Scheduler scheduler;
    ApplicationContext applicationContext;
    PartitionService partitionService;
    Member localMember;

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
            logger.info("HarvestingScheduler starting up");
            partitionService = hazelcast.getPartitionService();

            localMember = hazelcast.getCluster().getLocalMember();
            hazelcastLdapStore.loadAllKeys();

            Properties properties = new Properties();
            properties.load(this.getClass().getResourceAsStream("/org/dataone/configuration/synchQuartz.properties"));
            StdSchedulerFactory schedulerFactory = new StdSchedulerFactory(properties);
            scheduler = schedulerFactory.getScheduler();


            this.manageHarvest();
            partitionService.addMigrationListener(this);
            IMap<NodeReference, Node> hzNodes = hazelcast.getMap("hzNodes");
            hzNodes.addEntryListener(this, true);
        } catch (IOException ex) {
            throw new IllegalStateException("Loading properties file failedUnable to initialize jobs for scheduling: " + ex.getMessage());
        } catch (SchedulerException ex) {
            throw new IllegalStateException("Unable to initialize jobs for scheduling: " + ex.getMessage());
        }
    }

    /**
     * will perform the recalculation of the scheduler. 
     * if scheduler is running, it will be disabled All jobs will be
     * deleted for this node All nodes that are considered 'local' by hazelcast 
     * will be scheduled with synchronization jobs
     *
     * Seems that the listeners could call this in parallel, and it should
     * be an atomic operation, so it is synchronized
     */
    public synchronized void manageHarvest() throws SchedulerException {

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
        IMap<NodeReference, Node> hzNodes = hazelcast.getMap("hzNodes");

        logger.info("Node map has " + hzNodes.size() + " entries");
        // construct new jobs and triggers based on ownership of nodes in the nodeList
        for (NodeReference key : hzNodes.localKeySet()) {
            // exclude from the set any CNs or membernodes that are down or do not
            // want to be synchronized
            Node node = hzNodes.get(key);
            addHarvest(key, node);
        }
        scheduler.start();

        if (scheduler.isStarted()) {
            logger.info("Scheduler is started");
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

    /* 
     * monitor node additions
     * 
     * if a node is added to nodelist, this will be triggered
     * does nothing now
     * 
     * @param EntryEvent<NodeReference, Node>
     * 
     */
    @Override
    public void entryAdded(EntryEvent<NodeReference, Node> event) {
        logger.info("Node Entry added key=" + event.getKey().getValue());
        /*
         * try { Thread.sleep(2000L); } catch (InterruptedException ex) {
         * Logger.getLogger(HarvestSchedulingManager.class.getName()).log(Level.SEVERE, null, ex); }
         *
         * Partition partition = partitionService.getPartition(event.getKey()); Member ownerMember =
         * partition.getOwner();
         *
         * if (localMember.equals(ownerMember)) { try { this.manageHarvest(); } catch (SchedulerException ex) { throw
         * new IllegalStateException("Unable to initialize jobs for scheduling: " + ex.getMessage()); } }
         */
    }

    /*
     * monitor node removals
     * 
     * if a node is removed from nodelist, this will be triggered
     * does nothing now
     * 
     * @param EntryEvent<NodeReference, Node>
     * 
     */
    @Override
    public void entryRemoved(EntryEvent<NodeReference, Node> event) {
        logger.error("Entry removed key=" + event.getKey().getValue());
    }
    /*
     * monitor node changes/updates to hazelcast 
     * 
     * should only be noted if updateNodeCapabilities is called on the CN, or if
     * the nodeApproval tool is run by administrator
     * 
     * may result in recalculation of Quartz job scheduling
     * if any nodes are determined to be locally owned
     * 
     * @param EntryEvent<NodeReference, Node>
     */
    @Override
    public void entryUpdated(EntryEvent<NodeReference, Node> event) {
        logger.info("Node Entry updated key=" + event.getKey().getValue());
        synchronized(this) {
            Partition partition = partitionService.getPartition(event.getKey());
            Member ownerMember = partition.getOwner();

            if (localMember.equals(ownerMember)) {
                try {
                    this.manageHarvest();
                } catch (SchedulerException ex) {
                    throw new IllegalStateException("Unable to initialize jobs for scheduling: " + ex.getMessage());
                }
            }
        }
    }

    @Override
    public void entryEvicted(EntryEvent<NodeReference, Node> event) {
        logger.warn("Entry evicted key=" + event.getKey().getValue());
    }

    //
    // http://code.google.com/p/hazelcast/source/browse/trunk/hazelcast/src/main/java/com/hazelcast/impl/TestUtil.java?r=1824
    // http://groups.google.com/group/hazelcast/browse_thread/thread/3856d5829e26f81c?fwc=1
    //
    public void migrationCompleted(MigrationEvent migrationEvent) {
        logger.debug("migrationCompleted " + migrationEvent.getPartitionId());
        // this is the partition that was moved from 
        // one node to the other
        // try to determine if a Node has migrated home servers
        if (localMember.equals(migrationEvent.getNewOwner()) || localMember.equals(migrationEvent.getOldOwner())) {
            Integer partitionId = migrationEvent.getPartitionId();

            IMap<NodeReference, Node> hzNodes = hazelcast.getMap("hzNodes");

            List<Integer> nodePartitions = new ArrayList<Integer>();
            for (NodeReference key : hzNodes.keySet()) {
                nodePartitions.add(partitionService.getPartition(key).getPartitionId());
            }

            if (nodePartitions.contains(partitionId)) {
                logger.info("Node Partions migrated ");
                try {
                    this.manageHarvest();
                } catch (SchedulerException ex) {
                    throw new IllegalStateException("Unable to initialize jobs for scheduling: " + ex.getMessage());
                }
            }
        }


    }

    public void migrationStarted(MigrationEvent migrationEvent) {
        logger.debug("migrationStarted " + migrationEvent.getPartitionId());
    }

    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    public void setHazelcast(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
    }

    public HazelcastLdapStore getHazelcastLdapStore() {
        return hazelcastLdapStore;
    }

    public void setHazelcastLdapStore(HazelcastLdapStore hazelcastLdapStore) {
        this.hazelcastLdapStore = hazelcastLdapStore;
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

    @Override
    public void migrationFailed(MigrationEvent migrationEvent) {
         logger.warn("migrationFailed " + migrationEvent.getPartitionId());
    }
}
