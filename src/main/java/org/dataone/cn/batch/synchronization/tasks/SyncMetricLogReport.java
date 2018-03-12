/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.synchronization.tasks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.synchronization.type.SyncQueueFacade;
import org.dataone.cn.log.MetricEvent;
import org.dataone.cn.log.MetricLogClientFactory;
import org.dataone.cn.log.MetricLogEntry;
import org.dataone.cn.synchronization.types.SyncObject;
import org.dataone.service.types.v1.NodeReference;

/**
 * Create a Report of the number of tasks queued for processing per MemberNode and a total of tasks queued for the
 * processor
 *
 * @author waltz
 */
public class SyncMetricLogReport {
    static final Logger logger = Logger.getLogger(SyncMetricLogReport.class);
    public void reportSyncMetrics(SyncQueueFacade hzSyncObjectQueue) {
logger.debug("start");
        Date metricLogReportDate = new Date(System.currentTimeMillis());
        Map<String, MutableCounter> legacySyncObjectPerNodeMap = new HashMap<>();

        MetricLogEntry metricLogEntry = new MetricLogEntry(MetricEvent.SYNCHRONIZATION_QUEUED);
        metricLogEntry.setMessage("Total Sync Objects Queued: " + hzSyncObjectQueue.size());
        metricLogEntry.setDateLogged(metricLogReportDate);
        MetricLogClientFactory.getMetricLogClient().logMetricEvent(metricLogEntry);
        
        
        
        SyncObject[] syncObjectArray = hzSyncObjectQueue.getLegacyQueue().toArray(new SyncObject[0]);
        
        for (int i = 0; i < syncObjectArray.length; ++i) {
            String nodeId = syncObjectArray[i].getNodeId();
            
            MutableCounter nodeCount = legacySyncObjectPerNodeMap.get(nodeId);
            if (nodeCount == null) {
                nodeCount = new MutableCounter();
                legacySyncObjectPerNodeMap.put(nodeId, nodeCount);
            } 
            nodeCount.increment();

        }
        
        
        for (String qName : hzSyncObjectQueue.getQueueNames()) {
            hzSyncObjectQueue.size(qName);
            legacySyncObjectPerNodeMap.get(qName);
        }
        
        
        // create a superset of nodes to report on
        Set<String> nodesToReport = legacySyncObjectPerNodeMap.keySet();
        for (String qName : hzSyncObjectQueue.getQueueNames()) {
            nodesToReport.add(qName);
        }
        
        
        for (String nodeId : nodesToReport) {
            NodeReference nodeReference = new NodeReference();
            nodeReference.setValue(nodeId);
            metricLogEntry = new MetricLogEntry(MetricEvent.SYNCHRONIZATION_QUEUED, nodeReference);
            
            int sum = hzSyncObjectQueue.size(nodeId) 
                    + (legacySyncObjectPerNodeMap.get(nodeId) == null ? 0 : legacySyncObjectPerNodeMap.get(nodeId).getInt()); 
                     
            metricLogEntry.setMessage("Sync Objects Queued: " + sum );
            metricLogEntry.setDateLogged(metricLogReportDate);
             MetricLogClientFactory.getMetricLogClient().logMetricEvent(metricLogEntry);
        }
logger.debug("end");
    }

    /**
     * Simple object that increments an integer based on reports from 2014 benchmarks of different methods using a
     * MutableCounter appears the most efficient unless we want to use a 3rd party library
     *
     * http://stackoverflow.com/questions/81346/most-efficient-way-to-increment-a-map-value-in-java
     */
    private class MutableCounter {

        private int value = 0;

        public void increment() {
            ++value;
        }

        public int getInt() {
            return value;
        }
    }
}
