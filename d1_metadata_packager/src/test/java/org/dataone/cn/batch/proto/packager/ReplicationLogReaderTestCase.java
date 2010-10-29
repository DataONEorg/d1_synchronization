/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.packager;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Resource;
import org.dataone.cn.batch.utils.MetadataPackageAccess;
import org.junit.*;
import static org.junit.Assert.*;
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
@ContextConfiguration(locations = {"classpath:/org/dataone/cn/batch/proto/packager/ReplicationLogReaderTestCase-context.xml"})
public class ReplicationLogReaderTestCase {

    ReplicationLogReader replicationLogReader;
    String testPackageHarvestDirectoryString;
    String testLogFilePersistDataName;
    String testEvent2;
    String testEvent3;
    MetadataPackageAccess metadataPackageAccess;
    StringBuffer results1Buffer = new StringBuffer();
    StringBuffer results2Buffer = new StringBuffer();
    @Resource      
    public void setTestLogFilePersistDataName(String testLogFilePersistDataName) {
        this.testLogFilePersistDataName = testLogFilePersistDataName;
    }

    @Resource
    public void setTestPackageHarvestDirectoryString(String testPackageHarvestDirectoryString) {
        this.testPackageHarvestDirectoryString = testPackageHarvestDirectoryString;
    }

    @Resource
    public void setReplicationLogReader(ReplicationLogReader logReader) {
        this.replicationLogReader = logReader;
    }

    @Resource
    public void setMetadataPackageAccess(MetadataPackageAccess metadataPackageAccess) {
        this.metadataPackageAccess = metadataPackageAccess;
    }

    @Test
    public void testLogReader() throws Exception {
        replicationLogReader.readLogfile();
        Map<String, Map<String, String>> results = replicationLogReader.getMergeQueue();
        metadataPackageAccess.writePersistentData();
        System.out.println("sucess with " + results.size()  +" entries");
        results1Buffer.append(results.size());
        ArrayList<String> keyset = new ArrayList<String>(results.keySet());
        Collections.sort(keyset);
        for (String key : keyset) {
            System.out.println("found GUID " + key);
            results1Buffer.append(key);
            Map<String, String> mergeFiles = results.get(key);
            for (String keyMerge : mergeFiles.keySet()) {
                results1Buffer.append(keyMerge);
                results1Buffer.append(mergeFiles.get(keyMerge));
                System.out.println("\tfound " + keyMerge + " for " + mergeFiles.get(keyMerge));
            }
        }
    }
    @Test
    public void testLogPersistence() throws Exception {

        Map<String, Map<String, String>> results =metadataPackageAccess.getPendingDataQueue();
        results2Buffer.append(results.size());
        ArrayList<String> keyset = new ArrayList<String>(results.keySet());
        Collections.sort(keyset);
        for (String key : keyset) {
            results2Buffer.append(key);
            Map<String, String> mergeFiles = results.get(key);
            for (String keyMerge : mergeFiles.keySet()) {
                results2Buffer.append(keyMerge);
                results2Buffer.append(mergeFiles.get(keyMerge));
            }
        }
         assertTrue(results2Buffer.toString().contentEquals(results2Buffer.toString()));
    }
//    @Test
    public void testLogReaderWithAdditionalLogging() throws Exception {

        replicationLogReader.setEventLogFileName(testEvent2);
        replicationLogReader.readLogfile();
        Map<String, Map<String, String>> results = replicationLogReader.getMergeQueue();
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

//    @Test
    public void testLogReaderWithDosNewlines() throws Exception {
        replicationLogReader.newline = "\r\n";
        metadataPackageAccess.init();
        replicationLogReader.setEventLogFileName(testEvent3);
        replicationLogReader.readLogfile();
        Map<String, Map<String, String>> results = replicationLogReader.getMergeQueue();
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
