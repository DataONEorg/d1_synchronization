package org.dataone.cn.batch.synchronization.tasks;

public class MockQueueItem {

    final String name;
    final long duration;
    final Exception exception;
    
    MockQueueItem(String name, long duration, Exception exception) {
        this.name = name;
        this.duration = duration;
        this.exception = exception;
    }
}
