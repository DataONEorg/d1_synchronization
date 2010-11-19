/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.packager;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Resource;
import org.dataone.cn.batch.proto.packager.types.MergeMap;
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
    String testReplicate2;
    String testReplicate3;
    String testReplicationLogFile2;
    ReplicationPersistence replicationPersistence;
    static StringBuffer results1Buffer = new StringBuffer();
    static StringBuffer results2Buffer = new StringBuffer();

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
    public void setMetadataReplicationPersistence(ReplicationPersistence replicationPersistence) {
        this.replicationPersistence = replicationPersistence;
    }

    @Resource
    public void setTestReplicationLogFile2(String testReplicationLogFile2) {
        this.testReplicationLogFile2 = testReplicationLogFile2;
    }

    @Test
    public void testLogReader() throws Exception {
        replicationLogReader.readLogfile();
        MergeMap results = replicationLogReader.getMergeMap();
        replicationPersistence.writePersistentData();
        System.out.println("testLogReader success with " + results.size()  +" entries");
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

        MergeMap results = replicationPersistence.getPersistMergeMap();
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
        System.out.println("results1Buffer:" + results1Buffer.toString() + "\nresults2Buffer:" + results2Buffer.toString());
        assertTrue(results1Buffer.toString().contentEquals(results2Buffer.toString()));
    }

    @Test
    public void testReplicationPersistenceWithDuplicateLogEntries() throws Exception {
        MergeMap replicationMap = replicationPersistence.getPersistMergeMap();
        Map<String, Map<String, String>> duplicateMap = replicationMap.getMap();
        System.out.println("\treplicate Size: " + duplicateMap.keySet().size());
        replicationMap = new MergeMap();
        
        replicationPersistence.setPersistMergeMap(replicationMap);
        replicationLogReader.setLogFileName(this.testReplicationLogFile2);

        replicationLogReader.readLogfile();
        MergeMap results = replicationLogReader.getMergeMap();
        System.out.println("\tresults size: " + results.keySet().size() + " replicate Size: " + duplicateMap.keySet().size());
        assertTrue(results.keySet().size() == duplicateMap.keySet().size());

        for (String key : (Set<String>)results.keySet()) {
            System.out.println("found GUID " + key);
            Map<String, String> mergeFiles = results.get(key);
            for (String keyMerge : mergeFiles.keySet()) {
                System.out.println("\tfound " +keyMerge + " for " + mergeFiles.get(keyMerge));
                assertTrue(results.get(key).get(keyMerge).contentEquals(duplicateMap.get(key).get(keyMerge)));
            }
        } 
        if (replicationPersistence.getPersistentDataFile().delete()) {
            System.out.println("deleted persistent Data File " + replicationPersistence.getPersistentDataFileName());
        }
    }

//    @Test
    public void testLogReaderWithDosNewlines() throws Exception {
        replicationLogReader.newline = "\r\n";
        replicationPersistence.init();
        replicationLogReader.setLogFileName(testReplicate3);
        replicationLogReader.readLogfile();
        Map<String, Map<String, String>> results = replicationLogReader.getMergeMap();
/*        for (String key : results.keySet()) {
            System.out.println("found GUID " + key);
            Map<String, String> mergeFiles = results.get(key);
            for (String keyMerge : mergeFiles.keySet()) {
                System.out.println("\tfound " + keyMerge + " for " + mergeFiles.get(keyMerge));
            }
        } */
//        File testLogFilePersistDataNameFile = new File(this.testPackageHarvestDirectoryString + File.separator + this.testLogFilePersistDataName);
        if (replicationPersistence.getPersistentDataFile().delete()) {
            System.out.println("deleted persistent Data File " + replicationPersistence.getPersistentDataFileName());
        }
    }
}
