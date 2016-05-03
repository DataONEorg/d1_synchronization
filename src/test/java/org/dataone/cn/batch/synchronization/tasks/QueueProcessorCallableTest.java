package org.dataone.cn.batch.synchronization.tasks;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/org/dataone/configuration/applicationContext.xml" })
public class QueueProcessorCallableTest {

    static Queue<String> queue;
    static String queueItemPrefix = "item";
    static int queueItemSequence = 0;
    
    class QueueTask implements Callable<String> {

        String queueItemName;
        
        QueueTask(String queueItemName) {
            this.queueItemName = queueItemName;
        }
        
        @Override
        public String call() throws Exception {
            return String.format("%s: ", this.queueItemName);
        }
    }
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        queue = new ArrayBlockingQueue<String>(5);
        queue.add(queueItemPrefix + queueItemSequence++);
        queue.add(queueItemPrefix + queueItemSequence++);
        queue.add(queueItemPrefix + queueItemSequence++);
        queue.add(queueItemPrefix + queueItemSequence++);
        queue.add(queueItemPrefix + queueItemSequence++);
    }

    @Test
    public void shouldExecuteCallables() throws InterruptedException {
        
        Executor x = Executors.newSingleThreadExecutor();
        MockQueueProcessor qp = new MockQueueProcessor();
        qp.setQueue(queue);
        
        FutureTask<String> futureTask = null;
        try {
            futureTask = new FutureTask<>(qp);
            x.execute(futureTask);
            if (futureTask != null) {
                Thread.sleep(10000);
                futureTask.cancel(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        for (Entry<Object,List<String>> n : qp.getProcessingLog().entrySet()) {
            for (String message : n.getValue()) {
                System.out.printf("%s  : %s\n", n.getKey(), message);
            }
            assertEquals("Should be 2 log entries per queueItem",2,n.getValue().size());
        }
    }

    @Test 
    public void shouldBeInactivatable() {
        Executor x = Executors.newSingleThreadExecutor();
        MockQueueProcessor qp = new MockQueueProcessor();
        qp.setQueue(queue);
        
        FutureTask<String> futureTask = null;
        String nextAdditionalItem = null;
        try {
            futureTask = new FutureTask<>(qp);
            x.execute(futureTask);
            Thread.sleep(3000L);
            // the key statement to this test
            qp.setIsInactivated(true);
            
            nextAdditionalItem = queueItemPrefix + queueItemSequence++;
            queue.add(nextAdditionalItem);
            if (futureTask != null) {
                Thread.sleep(10000);
                futureTask.cancel(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // look at all of the queue processor tasks
        assertFalse("Should not have any logging of the item added after inactivation",
                qp.getProcessingLog().containsKey(nextAdditionalItem));
    }

    @Ignore("TO DO")
    @Test 
    public void shouldHoldOntoTaskIfExecutorFull() {
        // TODO
    }
    
    @Ignore ("TO DO")
    @Test
    public void shouldCancelStuckTasks() {
        
    }
}
