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
import org.dataone.cn.batch.proto.packager.types.DataPersistenceKeys;
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
    }

    @Test
    public void testAdditonalLogReader() throws Exception {
        replicationLogReader.setLogFileName(this.testReplicationLogFile2);
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
            ArrayList<String> mergedkeyset =  new ArrayList<String>(mergeFiles.keySet());
            Collections.sort(mergedkeyset);
            for (String keyMerge : mergedkeyset) {
                results1Buffer.append(keyMerge);
                results1Buffer.append(mergeFiles.get(keyMerge));
                System.out.println("\tfound " + keyMerge + " for " + mergeFiles.get(keyMerge));
            }
        }
    }
    @Test
    public void testLogPersistence() throws Exception {
        replicationPersistence.init();
        MergeMap results = replicationPersistence.getPersistMergeMap();
        ArrayList<String> keyset = new ArrayList<String>(results.keySet());
        Collections.sort(keyset);
        for (String key : keyset) {
            results2Buffer.append(key);
            Map<String, String> mergeFiles = results.get(key);
            ArrayList<String> mergedkeyset =  new ArrayList<String>(mergeFiles.keySet());
            Collections.sort(mergedkeyset);
            for (String keyMerge : mergedkeyset) {
                results2Buffer.append(keyMerge);
                results2Buffer.append(mergeFiles.get(keyMerge));
            }
        }
        System.out.println("results1Buffer:" + results1Buffer.toString() + "\nresults2Buffer:" + results2Buffer.toString());
        assertTrue(results1Buffer.toString().contentEquals(results2Buffer.toString()));
        File testLogFilePersistDataNameFile = new File(this.testPackageHarvestDirectoryString + File.separator + this.testLogFilePersistDataName);
        testLogFilePersistDataNameFile.delete();
    }
    @Test
    public void testReplicationPersistenceWithEmpty() throws Exception {
        replicationPersistence.init();
        long x = 100;
        long y = 100;
        replicationPersistence.getPersistEventMap().put(DataPersistenceKeys.SKIP_IN_LOG_FIELD.toString(), new Long(x));
        replicationPersistence.getPersistEventMap().put(DataPersistenceKeys.DATE_TIME_LAST_ACCESSED_FIELD.toString(), new Long(y));
         System.out.println("\ntestReplicationPersistenceWithEmpty");
//        MergeMap replicationMap = replicationPersistence.getPersistMergeMap();
//        replicationMap.clear();
        replicationPersistence.writePersistentData();

    }
    @Test
    public void testReplicationPersistenceFailWithEmpty() throws Exception {
        try {
            System.out.println("\ntestReplicationPersistenceFailWithEmpty");
            replicationPersistence.init();
            MergeMap replicationMap = replicationPersistence.getPersistMergeMap();
            for (String key: replicationMap.keySet()) {
                System.out.println(key);
            }
        } catch (Exception e) {
            System.out.println("testReplicationPersistenceFailWithEmpty");
            e.printStackTrace();
        }
        if (replicationPersistence.getPersistentDataFile().delete()) {
            System.out.println("deleted persistent Data File " + replicationPersistence.getPersistentDataFileName());
        }
    }
}
