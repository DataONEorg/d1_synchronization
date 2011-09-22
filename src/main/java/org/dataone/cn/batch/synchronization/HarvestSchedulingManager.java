/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.synchronization;

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
import org.dataone.cn.hazelcast.ldap.HazelcastLdapStore;
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
            logger.info("HarvestingScheduler starting up");
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
        IMap<String, Node> hzNodes = hazelcast.getMap("hzNodes");

        logger.info("Node map has " + hzNodes.size() + " entries");
        // construct new jobs and triggers based on ownership of nodes in the nodeList
        for (String key : hzNodes.localKeySet()) {
            // exclude from the set any CNs or membernodes that are down or do not
            // want to be synchronized
            Node node = hzNodes.get(key);
            if (node.getState().equals(NodeState.UP)
                    && node.isSynchronize() && node.getType().equals(NodeType.MN)) {
                String crontabEntry = this.getCrontabEntry(node);
                logger.info("scheduling  key " + key + " with schedule " + crontabEntry);
                // the current mn node is owned by this hazelcast cn node member
                // so schedule a job based on the settings of the node
                JobDetail job = newJob(MemberNodeHarvestJob.class).withIdentity("job-" + key, groupName) // name "myJob", group "group1"
                        .usingJobData("mnIdentifier", key).build();

                Trigger trigger = newTrigger().withIdentity("trigger-" + key, groupName).startNow().withSchedule(cronSchedule(crontabEntry)).build();
                try {
                    scheduler.scheduleJob(job, trigger);
                } catch (SchedulerException ex) {
                    logger.error("Unable to initialize job key " + key + " with schedule " + crontabEntry + "for scheduling: ", ex);
                }
            }
        }
        scheduler.start();

        if (scheduler.isStarted()) {
            logger.debug("Scheduler is started");
        }

    }

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
