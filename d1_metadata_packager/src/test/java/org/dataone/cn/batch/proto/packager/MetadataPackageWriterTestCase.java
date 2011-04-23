/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.packager;

import java.io.File;
import javax.annotation.Resource;
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
    DataPersistence metadataPackageAccess;

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
    public void setMetadataPackageAccess(DataPersistence metadataPackageAccess) {
        this.metadataPackageAccess = metadataPackageAccess;
    }

    @Test
    public void testPackagingMetadataMergeQueue() throws Exception {
        eventLogReader.readLogfile();
        packageWriter.setReadQueue(eventLogReader.getMergeMap());
        packageWriter.writePackages();

        File testEventLogFilePersistDataNameFile = new File(this.testPackageHarvestDirectoryString + File.separator + this.testEventLogFilePersistDataName);
        testEventLogFilePersistDataNameFile.delete();
    }

    @Test
    public void testPackagingMetadataMergeQueueFailure() throws Exception {
        metadataPackageAccess.init();
        eventLogReader.setLogFileName(this.testEventError);
        eventLogReader.readLogfile();
        packageWriter.setReadQueue(eventLogReader.getMergeMap());

        packageWriter.writePackages();

        File testEventLogFilePersistDataNameFile = new File(this.testPackageHarvestDirectoryString + File.separator + this.testEventLogFilePersistDataName);
        testEventLogFilePersistDataNameFile.delete();
    }
}
