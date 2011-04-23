/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.packager;

import java.io.File;
import java.util.Map;
import javax.annotation.Resource;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
/*
 *
 * @author rwaltz
 */

/**
 *
 * @author rwaltz
 *
 * , "classpath:/org/dataone/cn/rest/proxy/tests/webapp/mercury-beans.xml"
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/org/dataone/cn/batch/proto/packager/LogReaderTestCase-context.xml"})
public class LogReaderTestCase {

    EventLogReader eventLogReader;
    String testPackageHarvestDirectoryString;
    String testLogFilePersistDataName;
    String testEvent2;
    String testEvent3;
    DataPersistence eventPersistence;
    @Resource
    public void setTestLogFilePersistDataName(String testLogFilePersistDataName) {
        this.testLogFilePersistDataName = testLogFilePersistDataName;
    }

    @Resource
    public void setTestPackageHarvestDirectoryString(String testPackageHarvestDirectoryString) {
        this.testPackageHarvestDirectoryString = testPackageHarvestDirectoryString;
    }

    @Resource
    public void setLogReader(EventLogReader eventLogReader) {
        this.eventLogReader = eventLogReader;
    }

    @Resource
    public void setTestEvent2(String testEvent2) {
        this.testEvent2 = testEvent2;
    }

    @Resource
    public void setTestEvent3(String testEvent3) {
        this.testEvent3 = testEvent3;
    }
    @Resource
    public void setEventPersistence(DataPersistence eventPersistence) {
        this.eventPersistence = eventPersistence;
    }

    @Test
    public void testLogReader() throws Exception {
        eventLogReader.readLogfile();
        Map<String, Map<String, String>> results = eventLogReader.getMergeMap();
        eventPersistence.writePersistentData();
/*        System.out.println("sucess");
        for (String key : results.keySet()) {
            System.out.println("found GUID " + key);
            Map<String, String> mergeFiles = results.get(key);
            for (String keyMerge : mergeFiles.keySet()) {
                System.out.println("\tfound " + keyMerge + " for " + mergeFiles.get(keyMerge));
            }
        } */
    }

    @Test
    public void testLogReaderWithAdditionalLogging() throws Exception {

        eventLogReader.setLogFileName(testEvent2);
        eventLogReader.readLogfile();
        Map<String, Map<String, String>> results = eventLogReader.getMergeMap();
/*        for (String key : results.keySet()) {
            System.out.println("found GUID " + key);
            Map<String, String> mergeFiles = results.get(key);
            for (String keyMerge : mergeFiles.keySet()) {
                System.out.println("\tfound " +keyMerge + " for " + mergeFiles.get(keyMerge));
            }
        } */
        File testLogFilePersistDataNameFile = new File(this.testPackageHarvestDirectoryString + File.separator + this.testLogFilePersistDataName);
        testLogFilePersistDataNameFile.delete();
    }

    @Test
    public void testLogReaderWithDosNewlines() throws Exception {
        eventLogReader.newline = "\r\n";
        eventPersistence.init();
        eventLogReader.setLogFileName(testEvent3);
        eventLogReader.readLogfile();
        Map<String, Map<String, String>> results = eventLogReader.getMergeMap();
/*        for (String key : results.keySet()) {
            System.out.println("found GUID " + key);
            Map<String, String> mergeFiles = results.get(key);
            for (String keyMerge : mergeFiles.keySet()) {
                System.out.println("\tfound " + keyMerge + " for " + mergeFiles.get(keyMerge));
            }
        } */
        File testLogFilePersistDataNameFile = new File(this.testPackageHarvestDirectoryString + File.separator + this.testLogFilePersistDataName);
        testLogFilePersistDataNameFile.delete();
    }
}
