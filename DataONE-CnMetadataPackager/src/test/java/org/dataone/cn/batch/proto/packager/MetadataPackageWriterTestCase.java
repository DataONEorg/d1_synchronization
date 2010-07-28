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

    LogReader logReader;
    MetadataPackageWriter packageWriter;
    String testPackageHarvestDirectoryString;
    String testLogFilePersistDataName;
    String testEventError;
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
    public void setPackageWriter(MetadataPackageWriter packageWriter) {
        this.packageWriter = packageWriter;
    }
    @Resource
    public void setTestEventError(String testEventError) {
        this.testEventError = testEventError;
    }
    @Resource
    public void setMetadataPackageAccess(MetadataPackageAccess metadataPackageAccess) {
        this.metadataPackageAccess = metadataPackageAccess;
    }
    @Test
    public void testPackagingMetadataMergeQueue() throws Exception {
        logReader.readEventLogfile();
        packageWriter.setReadQueue(logReader.getMergeQueue());
        packageWriter.writePackages();
/*      
        System.out.println("sucess");
        for (String key : logReader.getMergeQueue().results.keySet()) {
            System.out.println("found GUID " + key);
            Map<String, String> mergeFiles = results.get(key);
            for (String keyMerge : mergeFiles.keySet()) {
                System.out.println("\tfound " + keyMerge + " for " + mergeFiles.get(keyMerge));
            }
        } */
        File testLogFilePersistDataNameFile = new File(this.testPackageHarvestDirectoryString + File.separator + this.testLogFilePersistDataName);
        testLogFilePersistDataNameFile.delete();
    }
    @Test
    public void testPackagingMetadataMergeQueueFailure() throws Exception {
        metadataPackageAccess.init();
        logReader.setEventLogFileName(this.testEventError);
        logReader.readEventLogfile();
        packageWriter.setReadQueue(logReader.getMergeQueue());
        try {
            packageWriter.writePackages();
        } catch (FileNotFoundException ex) {
            System.out.println("file was not found and process blew up");
        }
/*
        System.out.println("sucess");
        for (String key : logReader.getMergeQueue().results.keySet()) {
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
