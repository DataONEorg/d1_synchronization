/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.proto.packager;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import javax.annotation.Resource;
import org.dataone.cn.batch.utils.MetadataPackageAccess;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 * @author rwaltz
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/org/dataone/cn/batch/proto/packager/MetadataPackageWriterTestCase-context.xml"})
public class MetadataPackageWriterTestCase {

    EventLogReader eventLogReader;
    MetadataPackageWriter packageWriter;
    String testPackageHarvestDirectoryString;
    String testEventLogFilePersistDataName;
    String testEventError;
    EventPersistence metadataPackageAccess;
    @Resource
    public void setTestEventLogFilePersistDataName(String testEventLogFilePersistDataName) {
        this.testEventLogFilePersistDataName = testEventLogFilePersistDataName;
    }

    @Resource
    public void setTestPackageHarvestDirectoryString(String testPackageHarvestDirectoryString) {
        this.testPackageHarvestDirectoryString = testPackageHarvestDirectoryString;
    }

    @Resource
    public void setTestEventLogReader(EventLogReader eventLogReader) {
        this.eventLogReader = eventLogReader;
    }
    @Resource
    public void setTestMetadataPackageWriter(MetadataPackageWriter packageWriter) {
        this.packageWriter = packageWriter;
    }
    @Resource
    public void setTestEventError(String testEventError) {
        this.testEventError = testEventError;
    }
    @Resource
    public void setMetadataPackageAccess(EventPersistence metadataPackageAccess) {
        this.metadataPackageAccess = metadataPackageAccess;
    }
    @Test
    public void testPackagingMetadataMergeQueue() throws Exception {
        eventLogReader.readLogfile();
        packageWriter.setReadQueue(eventLogReader.getMergeMap());
        packageWriter.writePackages();
/*      
        System.out.println("sucess");
        for (String key : eventLogReader.getMergeQueue().results.keySet()) {
            System.out.println("found GUID " + key);
            Map<String, String> mergeFiles = results.get(key);
            for (String keyMerge : mergeFiles.keySet()) {
                System.out.println("\tfound " + keyMerge + " for " + mergeFiles.get(keyMerge));
            }
        } */
        File testEventLogFilePersistDataNameFile = new File(this.testPackageHarvestDirectoryString + File.separator + this.testEventLogFilePersistDataName);
        testEventLogFilePersistDataNameFile.delete();
    }
    @Test
    public void testPackagingMetadataMergeQueueFailure() throws Exception {
        metadataPackageAccess.init();
        eventLogReader.setLogFileName(this.testEventError);
        eventLogReader.readLogfile();
        packageWriter.setReadQueue(eventLogReader.getMergeMap());
        try {
            packageWriter.writePackages();
        } catch (FileNotFoundException ex) {
            System.out.println("file was not found and process blew up");
        }
/*
        System.out.println("sucess");
        for (String key : eventLogReader.getMergeQueue().results.keySet()) {
            System.out.println("found GUID " + key);
            Map<String, String> mergeFiles = results.get(key);
            for (String keyMerge : mergeFiles.keySet()) {
                System.out.println("\tfound " + keyMerge + " for " + mergeFiles.get(keyMerge));
            }
        } */
        File testEventLogFilePersistDataNameFile = new File(this.testPackageHarvestDirectoryString + File.separator + this.testEventLogFilePersistDataName);
        testEventLogFilePersistDataNameFile.delete();
    }
}
