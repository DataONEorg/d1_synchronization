package org.dataone.cn.batch.synchronization.tasks;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.dataone.cn.batch.exceptions.RetryableException;
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

    Queue<MockQueueItem> queue5 = new ArrayBlockingQueue<>(5);
    static String queueItemPrefix = "item";
    static int queueItemSequence = 0;
    
    MockQueueProcessor qProcessor;
    
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        qProcessor = new MockQueueProcessor();
    }

    private MockQueueItem buildQueueItem(long duration, Exception ex) {
        return new MockQueueItem(queueItemPrefix + queueItemSequence++, duration, ex);
    }
    
    @Test
    public void shouldExecuteCallables() throws InterruptedException {
        
        ExecutorService x = Executors.newSingleThreadExecutor();
        qProcessor.setQueue(queue5);
        queue5.add(buildQueueItem(0, null));
        queue5.add(buildQueueItem(0, null));
        queue5.add(buildQueueItem(0, null));
        queue5.add(buildQueueItem(0, null));
        queue5.add(buildQueueItem(0, null));
        
        FutureTask<String> futureTask = null;
        try {
            futureTask = new FutureTask<>(qProcessor);
            x.execute(futureTask);
            if (futureTask != null) {
                Thread.sleep(10000);
                futureTask.cancel(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            System.out.println("Remaining on the queue: "+ queue5.size());
            for (Entry<MockQueueItem,List<String>> n : qProcessor.getProcessingLog().entrySet()) {
                for (String le: n.getValue()) {
                    System.out.printf("%s : %s\n", n.getKey().name, le);
                }
                assertEquals("Should be 2 log entries per queueItem",2,n.getValue().size());
            }
            
            x.shutdownNow();
        }
        assertEquals("The processor should clear the queue",0, queue5.size());
    }

    @Test 
    public void shouldBeInactivatable() {
        ExecutorService x = Executors.newSingleThreadExecutor();
        qProcessor.setQueue(queue5);
        queue5.add(buildQueueItem(1000, null));
        queue5.add(buildQueueItem(2000, null));
        queue5.add(buildQueueItem(3000, null));
        queue5.add(buildQueueItem(4000, null));
        queue5.add(buildQueueItem(5000, null));
        
        FutureTask<String> processorTask = null;
        MockQueueItem itemAfterInactivation = null;
        try {
            processorTask = new FutureTask<>(qProcessor);
            x.execute(processorTask);
            Thread.sleep(3000L);
            // the key statement to this test
            qProcessor.setIsInactivated(true);
            Thread.sleep(100L);
            itemAfterInactivation = buildQueueItem(1000, null);
            System.out.println(itemAfterInactivation.name);
            queue5.add(itemAfterInactivation);
            if (processorTask != null) {
                Thread.sleep(10000);
                processorTask.cancel(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            System.out.println("Remaining on the queue: "+ queue5.size());
            for (Entry<MockQueueItem,List<String>> n : qProcessor.getProcessingLog().entrySet()) {
                for (String le: n.getValue()) {
                    System.out.printf("%s : %s\n", n.getKey().name, le);
                }
            }
            x.shutdownNow();
        }
        // look at all of the queue processor tasks
        assertFalse("Should not have any logging of the item added after inactivation",
                qProcessor.getProcessingLog().containsKey(itemAfterInactivation));
    }
 
    
    @Test
    public void shouldDelayRetryableTasks() {
        ExecutorService x = Executors.newSingleThreadExecutor();
        queue5.add(buildQueueItem(4000, new RetryableException("oops1",null,1000)));
        queue5.add(buildQueueItem(4000, new RetryableException("oops2",null,1000)));
        queue5.add(buildQueueItem(4000, new RetryableException("oops3",null,1000)));
        queue5.add(buildQueueItem(4000, new RetryableException("oops4",null,1000)));
        queue5.add(buildQueueItem(4000, new RetryableException("oops5",null,1000)));
        qProcessor.setQueue(queue5);
        
        FutureTask<String> processorTask = null;
        MockQueueItem nextAdditionalItem = null;
        try {
            processorTask = new FutureTask<>(qProcessor);
            x.execute(processorTask);
            Thread.sleep(1000L);
            qProcessor.setIsInactivated(true);
            Thread.sleep(10000L);

        } catch (InterruptedException e) {

        } catch (Exception e) {
            e.printStackTrace();
            fail("Threw some other exception:");
            
        }
        finally {
            System.out.println("Remaining on the queue: "+ queue5.size());
            for (Entry<MockQueueItem,List<String>> n : qProcessor.getProcessingLog().entrySet()) {
                for (String le: n.getValue()) {
                    System.out.printf("%s : %s\n", n.getKey().name, le);
                }
            }
            x.shutdownNow();
        }

    }
    
    @Ignore("TO DO")
    @Test
    public void shouldRequeueCanceledTasks() {
        
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
