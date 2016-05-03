package org.dataone.cn.batch.synchronization.tasks;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;

public class MockQueueProcessor extends QueueProcessorCallable<String, String> {

    HashMap<Object,List<String>> processingLog = new HashMap<>();

    @Override
    public Callable<String> prepareTask(Object queueItem) {
        logEvent(queueItem, "task Prepared");
        String[] elements = StringUtils.split(queueItem.toString(), '-');
        long duration = 2000L;
        Exception exception = null;
        if (elements.length > 1) {
            duration = Long.getLong(elements[1],2000L);
        }
        if (elements.length > 2) {
            try {
                Object o = Class.forName(elements[2],true,null);
                if (o instanceof Exception) {
                    exception = (Exception)o; 
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return new MockQueueTask(queueItem.toString(), duration, exception);
    }
    
    
    @Override
    public void cleanupTask(Object queueItem) {
        logEvent(queueItem, "task CleanedUp");
        System.out.println("cleaning up task: " + queueItem);
    }

    void logEvent(Object queueItem, String message) {
        String logMessage = String.format("[%s]: %s",new Date(), message);
        if (processingLog.containsKey(queueItem)) {
            processingLog.get(queueItem).add(logMessage);
        } else {
            processingLog.put(queueItem, new LinkedList<String>());
            processingLog.get(queueItem).add(logMessage);
        }
    }
    
    public HashMap<Object,List<String>> getProcessingLog() {
        return processingLog;
    }
}
