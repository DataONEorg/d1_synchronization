/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.packager;

import java.io.File;
import java.util.Map;
import javax.annotation.Resource;
import org.dataone.cn.batch.utils.MetadataPackageAccess;
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

    LogReader logReader;
    String testPackageHarvestDirectoryString;
    String testLogFilePersistDataName;
    String testEvent2;
    String testEvent3;
    MetadataPackageAccess metadataPackageAccess;
    @Resource
    public void setTestLogFilePersistDataName(String testLogFilePersistDataName) {
        this.testLogFilePersistDataName = testLogFilePersistDataName;
    }

    @Resource
    public void setTestPackageHarvestDirectoryString(String testPackageHarvestDirectoryString) {
        this.testPackageHarvestDirectoryString = testPackageHarvestDirectoryString;
    }

    @Resource
    public void setLogReader(LogReader logReader) {
        this.logReader = logReader;
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
    public void setMetadataPackageAccess(MetadataPackageAccess metadataPackageAccess) {
        this.metadataPackageAccess = metadataPackageAccess;
    }

    @Test
    public void testLogReader() throws Exception {
        logReader.readEventLogfile();
        Map<String, Map<String, String>> results = logReader.getMergeQueue();
        metadataPackageAccess.writePersistentData();
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

        logReader.setEventLogFileName(testEvent2);
        logReader.readEventLogfile();
        Map<String, Map<String, String>> results = logReader.getMergeQueue();
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
        logReader.newline = "\r\n";
        metadataPackageAccess.init();
        logReader.setEventLogFileName(testEvent3);
        logReader.readEventLogfile();
        Map<String, Map<String, String>> results = logReader.getMergeQueue();
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
