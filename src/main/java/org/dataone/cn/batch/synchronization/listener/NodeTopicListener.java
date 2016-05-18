/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.synchronization.listener;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.NodeReference;
import org.apache.log4j.Logger;
import org.dataone.cn.ComponentActivationUtility;
import org.dataone.cn.batch.synchronization.HarvestSchedulingManager;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.quartz.SchedulerException;

/**
 * Listen to the nodeTopic for any messages. if a message is received, then call back to manage the harvest jobs in
 * HarvestSchedulingManager
 *
 * @author waltz
 */
public class NodeTopicListener implements MessageListener<NodeReference> {

    // The BlockingQueue indexLogEntryQueue is a threadsafe, non-distributed queue shared with LogEntryQueueTask
    // It is injected via Spring
    static final Logger logger = Logger.getLogger(NodeTopicListener.class);

    static final String hzNodeTopicName = Settings.getConfiguration().getString("dataone.hazelcast.nodeTopic");

    HarvestSchedulingManager harvestSchedulingManager;

    public void addListener() {
        logger.info("NodeTopicListener Start");
        HazelcastClient hazelcast = HazelcastClientFactory.getProcessingClient();
        ITopic topic = hazelcast.getTopic(hzNodeTopicName);
        topic.addMessageListener(this);
 
    }

    private void removeListener() {
        HazelcastClient hazelcast = HazelcastClientFactory.getProcessingClient();
        ITopic topic = hazelcast.getTopic(hzNodeTopicName);
        topic.removeMessageListener(this);
    }
    public HarvestSchedulingManager getHarvestSchedulingManager() {
        return harvestSchedulingManager;
    }

    public void setHarvestSchedulingManager(HarvestSchedulingManager harvestSchedulingManager) {
        this.harvestSchedulingManager = harvestSchedulingManager;
    }

    @Override
    public void onMessage(Message<NodeReference> message) {
        if (ComponentActivationUtility.synchronizationIsActive()) {
            try {
                harvestSchedulingManager.manageHarvest();
            } catch (SchedulerException ex) {
                ex.printStackTrace();
                logger.error("unable to reschedule jobs due to harvestSchedulingManager.manageHarvest failure");
            }
        } else { 
            this.removeListener();
            logger.warn(message.getMessageObject().getValue() + "-  NodeTopicListener Disabled");
        }
    }

}
