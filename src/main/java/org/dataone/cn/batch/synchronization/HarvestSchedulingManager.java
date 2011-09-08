/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.synchronization;

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
import org.dataone.cn.batch.ldap.impl.HazelcastLdapStore;
import org.dataone.cn.batch.type.SimpleNode;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import static org.quartz.TriggerBuilder.*;
import static org.quartz.CronScheduleBuilder.*;

import static org.quartz.JobBuilder.*;

/**
 * this bean must be managed by Spring
 * upon startup of spring it will execute via init method
 * 
 * evaluate whether the NodeList contains nodes that should be executed on the
 * executing coordinating node. it will add  or remove triggers for jobs based on
 * events, such as startup, nightly refactoring, more CNs coming online
 *
 * todo: add in nightly job that re-calcuates jobs
 *       add in listeners that will,under certain conditions, add a job to call manager
 *       added jobs that call the manager should retrieve the manager from spring context
 * 
 * @author waltz
 */
public class HarvestSchedulingManager implements ApplicationContextAware {

    public static Log logger = LogFactory.getLog(HarvestSchedulingManager.class);
    private static String groupName = "MemberNodeHarvesting";
    private HazelcastInstance hazelcast;
    private HazelcastLdapStore hazelcastLdapStore;
    private Scheduler scheduler;
    ApplicationContext applicationContext;

    public void init() {
        try {
            logger.debug("HarvestingScheduler starting up");
            hazelcastLdapStore.loadAllKeys();
            Properties properties = new Properties();
            properties.load(this.getClass().getResourceAsStream("/org/dataone/configuration/quartz.properties"));
            StdSchedulerFactory schedulerFactory = new StdSchedulerFactory(properties);
            scheduler = schedulerFactory.getScheduler();


            this.manageHarvest();
        } catch (IOException ex) {
            throw new IllegalStateException("Loading properties file failedUnable to initialize jobs for scheduling: " + ex.getMessage());
        } catch (ParseException ex) {
            throw new IllegalStateException("Unable to initialize jobs for scheduling due to parsing of node cron entry: " + ex.getMessage());
        } catch (SchedulerException ex) {
            throw new IllegalStateException("Unable to initialize jobs for scheduling: " + ex.getMessage());
        }
    }

    public void manageHarvest() throws SchedulerException, ParseException {

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
            Set<JobKey> jobsInGroup = scheduler.getJobKeys(GroupMatcher.groupEquals(groupName));
            for (JobKey jobKey : jobsInGroup) {
                scheduler.deleteJob(jobKey);
            }
        }
        // populate the nodeList
        IMap<String, SimpleNode> d1NodesMap = hazelcast.getMap("d1NodesMap");

        logger.debug("Node map has " + d1NodesMap.size() + " entries");
        // construct new jobs and triggers based on ownership of nodes in the nodeList
        for (String key : d1NodesMap.localKeySet()) {

            // the current d1 node is owned by this hazelcast cn node member
            // so schedule a job based on the settings of the node
            JobDetail job = newJob(MemberNodeHarvestJob.class).withIdentity("job-" + key, groupName) // name "myJob", group "group1"
                    .usingJobData("mnIdentifier", d1NodesMap.get(key).getNodeId()).build();
            String crontabEntry = d1NodesMap.get(key).getCrontab();
            logger.debug("scheduling  key " + key + " for schedule " + crontabEntry);

            Trigger trigger = newTrigger().withIdentity("trigger-" + key, groupName).startNow().withSchedule(cronSchedule(crontabEntry)).build();
            scheduler.scheduleJob(job, trigger);

        }
        scheduler.start();

        if (scheduler.isStarted()) {
            logger.debug("Scheduler is started");
        }

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
}
