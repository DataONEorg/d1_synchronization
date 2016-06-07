/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.synchronization.tasks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.log4j.Logger;
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
    public void reportSyncMetrics(BlockingQueue<SyncObject> hzSyncObjectQueue) {
logger.debug("start");
        Map<String, MutableCounter> countSyncObjectPerNodeMap = new HashMap<>();
        SyncObject[] syncObjectArray = hzSyncObjectQueue.toArray(new SyncObject[0]);
        MetricLogEntry metricLogEntry = new MetricLogEntry(MetricEvent.SYNCHRONIZATION_QUEUED);
        metricLogEntry.setMessage("Total Sync Objects Queued: " + syncObjectArray.length);
        MetricLogClientFactory.getMetricLogClient().logMetricEvent(metricLogEntry);
        
        
        
        for (int i = 0; i < syncObjectArray.length; ++i) {
            String nodeId = syncObjectArray[i].getNodeId();
            
            MutableCounter nodeCount = countSyncObjectPerNodeMap.get(nodeId);
            if (nodeCount == null) {
                nodeCount = new MutableCounter();
                countSyncObjectPerNodeMap.put(nodeId, nodeCount);
            } 
            nodeCount.increment();

        }
        for (String nodeId : countSyncObjectPerNodeMap.keySet()) {
            NodeReference nodeReference = new NodeReference();
            nodeReference.setValue(nodeId);
            metricLogEntry = new MetricLogEntry(MetricEvent.SYNCHRONIZATION_QUEUED, nodeReference);
            metricLogEntry.setMessage("Sync Objects Queued: " + countSyncObjectPerNodeMap.get(nodeId).getInt());
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
