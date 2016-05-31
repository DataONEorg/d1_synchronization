/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.synchronization.tasks;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.log4j.Logger;
import org.dataone.cn.log.MockArrayWriterAppender;
import org.dataone.cn.synchronization.types.SyncObject;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author waltz
 */
public class SyncMetricLogReportTest {

    static Logger logger = Logger.getLogger(SyncMetricLogReportTest.class);
    SyncMetricLogReport syncMetricLogReport = new SyncMetricLogReport();
    static private final int ARRAY_SIZE = 100000;
    private static final BlockingQueue<SyncObject> largeRandomTestQueue = new ArrayBlockingQueue<>(ARRAY_SIZE);
    private static final Random generator = new Random();
    private static final BlockingQueue<SyncObject> preciseTestQueue = new ArrayBlockingQueue<>(10);

    static private final String TEST_1_NODE = "urn:node:test1";
    static private final String TEST_2_NODE = "urn:node:test2";
    static private final String TEST_3_NODE = "urn:node:test3";
    static private final String TEST_4_NODE = "urn:node:test4";
    static private final String TEST_5_NODE = "urn:node:test5";
    static private final String TEST_6_NODE = "urn:node:test6";
    static private final String TEST_7_NODE = "urn:node:test7";

    @BeforeClass
    static public void populateTestQueue() {
        String[] allNodes = {TEST_1_NODE, TEST_2_NODE, TEST_3_NODE, TEST_4_NODE,
            TEST_5_NODE, TEST_6_NODE, TEST_7_NODE};

        for (int i = 0; i < ARRAY_SIZE; ++i) {
            int randomIndex = generator.nextInt(allNodes.length);
            largeRandomTestQueue.add(new SyncObject(allNodes[randomIndex], UUID.randomUUID().toString()));
        }
        
        // 1 TEST_1_NODEs
        // 2 TEST_2_NODEs
        // 3 TEST_3_NODEs
        // 4 TEST_4_NODEs
        // 10 Test Nodes ha ha ha ha!
        
        preciseTestQueue.add(new SyncObject(TEST_1_NODE, UUID.randomUUID().toString()));
        preciseTestQueue.add(new SyncObject(TEST_2_NODE, UUID.randomUUID().toString()));
        preciseTestQueue.add(new SyncObject(TEST_2_NODE, UUID.randomUUID().toString()));
        preciseTestQueue.add(new SyncObject(TEST_3_NODE, UUID.randomUUID().toString()));
        preciseTestQueue.add(new SyncObject(TEST_3_NODE, UUID.randomUUID().toString()));
        preciseTestQueue.add(new SyncObject(TEST_3_NODE, UUID.randomUUID().toString()));
        preciseTestQueue.add(new SyncObject(TEST_4_NODE, UUID.randomUUID().toString()));
        preciseTestQueue.add(new SyncObject(TEST_4_NODE, UUID.randomUUID().toString()));
        preciseTestQueue.add(new SyncObject(TEST_4_NODE, UUID.randomUUID().toString()));
        preciseTestQueue.add(new SyncObject(TEST_4_NODE, UUID.randomUUID().toString()));
    }
    @Before
    public void initTest() {
        MockArrayWriterAppender.logReset();
    }
    @Test
    public void testLargeRandomArray() {
        
        long startTime = System.currentTimeMillis();
        syncMetricLogReport.reportSyncMetrics(largeRandomTestQueue);
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime);
        String[] mockLoggingOutputArray = MockArrayWriterAppender.getOutputArray();
        logger.info("Time to Execute in Milliseconds: "+ duration);
        logger.info(mockLoggingOutputArray.length);
        assertTrue(mockLoggingOutputArray.length == 8);
/*        for (int i = 0; i < mockLoggingOutputArray.length; ++i) {
            System.out.println(mockLoggingOutputArray[i]);
        } */
        
    }
    @Test
    public void testPreciseArray() {

        syncMetricLogReport.reportSyncMetrics(preciseTestQueue);
        String[] mockLoggingOutputArray = MockArrayWriterAppender.getOutputArray();
        for (int i = 0 ; i < mockLoggingOutputArray.length; ++i) {
            System.out.println(mockLoggingOutputArray[i]);
            if (mockLoggingOutputArray[i].contains(TEST_1_NODE) ) {
                assertTrue(mockLoggingOutputArray[i].contains("Sync Objects Queued: 1"));
            } else if (mockLoggingOutputArray[i].contains(TEST_2_NODE)) {
                assertTrue(mockLoggingOutputArray[i].contains("Sync Objects Queued: 2"));
            } else if (mockLoggingOutputArray[i].contains(TEST_3_NODE)) {
                assertTrue(mockLoggingOutputArray[i].contains("Sync Objects Queued: 3"));
            } else if (mockLoggingOutputArray[i].contains(TEST_4_NODE)) {   
                assertTrue(mockLoggingOutputArray[i].contains("Sync Objects Queued: 4"));
            } else if (mockLoggingOutputArray[i].contains("Total Sync Objects")) {
                assertTrue(mockLoggingOutputArray[i].contains("Queued: 10"));
            } else {
                fail("Invalid message " + mockLoggingOutputArray[i]);
            }
        }
    }
    

}
