package org.dataone.cn.batch.synchronization.tasks;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.dataone.cn.batch.exceptions.RetryableException;

public class MockQueueProcessor extends QueueProcessorCallable<MockQueueItem, String> {

    HashMap<MockQueueItem,List<String>> processingLog = new HashMap<>();

    @Override
    public Callable<String> prepareTask(MockQueueItem queueItem) {
        logEvent(queueItem, "task Prepared from: " + queueItem.name);
        return new MockQueueTask(queueItem.name, queueItem.duration, queueItem.exception);
    }
    
    
    @Override
    public void cleanupTask(MockQueueItem queueItem) {
        logEvent(queueItem, "task CleanedUp");
        System.out.println("cleaning up task: " + queueItem.name);
    }

    void logEvent(MockQueueItem queueItem, String message) {
        String logMessage = String.format("[%s]: %s",
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()),
                message);
        if (processingLog.containsKey(queueItem)) {
            processingLog.get(queueItem).add(logMessage);
        } else {
            processingLog.put(queueItem, new LinkedList<String>());
            processingLog.get(queueItem).add(logMessage);
        }
    }
    
    public HashMap<MockQueueItem,List<String>> getProcessingLog() {
        return processingLog;
    }
}
