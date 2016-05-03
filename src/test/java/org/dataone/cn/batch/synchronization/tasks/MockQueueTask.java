package org.dataone.cn.batch.synchronization.tasks;

import java.util.concurrent.Callable;

public class MockQueueTask implements Callable<String> {

    private String name;
    private long executeTime;
    private Exception e;
    
    MockQueueTask(String name, long executionTime) {
        this.name = name;
        this.executeTime = executionTime;
    }
    
    MockQueueTask(String name, long executionTime, Exception e) {
        this.name = name;
        this.executeTime = executionTime;
        this.e = e;
    }
    
    @Override
    public String call() throws Exception {
        Thread.sleep(this.executeTime);
        if (this.e != null)
            throw this.e;
        return String.format("%s: done", name);
    }

}
