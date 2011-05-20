/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.harvest;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PropertyPermission;
import java.util.TimeZone;
import javax.annotation.Resource;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.harvest.mock.MockMnReplication;
import org.dataone.cn.batch.proto.harvest.ObjectListQueueBuilder;
import org.dataone.cn.batch.proto.harvest.ObjectListQueueProcessor;
import org.dataone.cn.batch.proto.harvest.ObjectListQueueWriter;
import org.dataone.cn.batch.proto.harvest.persist.NodeMapPersistence;
import org.dataone.cn.batch.proto.harvest.types.NodeMap;
import org.dataone.service.mn.MemberNodeReplication;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.ObjectInfo;
import org.dataone.service.types.SystemMetadata;
import org.dataone.service.types.util.ServiceTypeUtil;
import org.junit.*;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * These Tests need a lot more work. Only the gross functionality is tested and no hedge cases, or failure cases.
 * Testing of side effects is also absent.
 * @author rwaltz
 *
 * 
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/org/dataone/cn/batch/harvest/config/harvesterTest-context.xml"})
public class HarvesterTestCase implements ApplicationContextAware {

    Logger logger = Logger.getLogger(HarvesterTestCase.class.getName());
    ObjectListQueueBuilder objectListQueueBuilder;
    ObjectListQueueProcessor objectListQueueProcessor;
    ObjectListQueueWriter objectListQueueWriter;
    ApplicationContext ac;
    static final String testListIds1 = "MD_ORNLDAAC_787_03032010095920:MD_ORNLDAAC_122_03032010095920:MD_ORNLDAAC_781_03032010095920";
    static final String testListFmt1 = "FGDC-STD-001.1-1999";
    static File cnObjectDirectory;
    static File cnMetaDirectory;
    static File mnSamplesDirectory;
    static String testTmpCacheDirectory;
    static String testSamplesDirectory;
    static NodeMapPersistence objectPersistence;
    MockMnReplication mockReplication;
    File testEmptyObjectListLocationFile;
    File testLyingCheatingObjectListLocationFile;
    
    @Before
    public void before() throws Exception {

        cnObjectDirectory = new File(testTmpCacheDirectory + File.separator + "cn" + File.separator + "object");
        if (!cnObjectDirectory.exists()) {
            cnObjectDirectory.mkdirs();
        }
        cnMetaDirectory = new File(testTmpCacheDirectory + File.separator + "cn" + File.separator + "meta");
        if (!cnMetaDirectory.exists()) {
            cnMetaDirectory.mkdirs();
        }
    }

    @AfterClass
    static public void cleanUp() {
        File[] objectFiles = cnObjectDirectory.listFiles();
        for (int i = 0; i < objectFiles.length; ++i) {
            objectFiles[i].delete();
        }
        cnObjectDirectory.delete();
        File[] metaFiles = cnMetaDirectory.listFiles();
        for (int i = 0; i < metaFiles.length; ++i) {
            metaFiles[i].delete();
        }
        cnMetaDirectory.delete();
        File cnDirectory = new File(testTmpCacheDirectory + File.separator + "cn");
        cnDirectory.delete();
        objectPersistence.getPersistentDataFile().delete();
    }

    @Resource
    public void setObjectPersistence(NodeMapPersistence objectPersistence) {
        this.objectPersistence = objectPersistence;
    }

    @Resource
    public void setTestSamplesDirectory(String testSamplesDirectory) {
        this.testSamplesDirectory = testSamplesDirectory;
    }

    @Resource
    public void setTestTmpCacheDirectory(String testTmpCacheDirectory) {
        this.testTmpCacheDirectory = testTmpCacheDirectory;
    }

    @Resource
    public void setObjectListQueueBuilder(ObjectListQueueBuilder objectListQueueBuilder) {
        this.objectListQueueBuilder = objectListQueueBuilder;
    }

    @Resource
    public void setObjectListQueueProcessor(ObjectListQueueProcessor objectListQueueProcessor) {
        this.objectListQueueProcessor = objectListQueueProcessor;
    }

    @Resource
    public void setObjectListQueueWriter(ObjectListQueueWriter objectListQueueWriter) {
        this.objectListQueueWriter = objectListQueueWriter;
    }
    @Resource
    public void setMnReplication(MockMnReplication mockReplication) {
        this.mockReplication = mockReplication;
    }
    @Resource
    public void setTestEmptyObjectListLocationFile(File testEmptyObjectListLocationFile) {
        this.testEmptyObjectListLocationFile = testEmptyObjectListLocationFile;
    }
    @Resource
    public void setTestLyingCheatingObjectListLocationFile(File testLyingCheatingObjectListLocationFile) {
        this.testLyingCheatingObjectListLocationFile = testLyingCheatingObjectListLocationFile;
    }
    @Test
    public void testQueueBuilder() throws Exception {

        List<ObjectInfo> writeQueue = new ArrayList<ObjectInfo>();
        objectListQueueBuilder.setWriteQueue(writeQueue);
        objectListQueueBuilder.buildQueue();
        for (ObjectInfo objectInfo : writeQueue) {
            assertTrue(testListIds1.contains(objectInfo.getIdentifier().getValue()));
        }
        assertTrue(writeQueue.size() == 3);
        System.out.println("Write Queue size is " + writeQueue.size() );
    }

    @Test
    public void testQueueProcessor() throws Exception {
        LinkedHashMap<Identifier, SystemMetadata> writeQueue = new LinkedHashMap<Identifier, SystemMetadata>();
        objectListQueueProcessor.setReadQueue(objectListQueueBuilder.getWriteQueue());
        objectListQueueProcessor.setWriteQueue(writeQueue);
        objectListQueueProcessor.processQueue();
        assertTrue(writeQueue.size() == 3);
        for (Identifier objectIdentifier : writeQueue.keySet()) {
            assertTrue(testListIds1.contains(objectIdentifier.getValue()));
        }
    }

    @Test
    public void testQueueWriter() throws Exception {
        File mnObjectDirectory = new File(testSamplesDirectory + File.separator + "mn" + File.separator + "object");
        File mnMetaDirectory = new File(testSamplesDirectory + File.separator + "mn" + File.separator + "meta");

        objectListQueueWriter.setReadQueue(objectListQueueProcessor.getWriteQueue());
        objectListQueueWriter.writeQueue();
        File[] objectFiles = mnObjectDirectory.listFiles();
        for (int i = 0; i < objectFiles.length; ++i) {
            File writtenObjectFile = new File(cnObjectDirectory.getAbsolutePath() + File.separator + objectFiles[i].getName());
            assertTrue(writtenObjectFile.exists());
            assertTrue(FileUtils.contentEquals(objectFiles[i], writtenObjectFile));
        }

        File[] metaFiles = mnMetaDirectory.listFiles();
        for (int i = 0; i < metaFiles.length; ++i) {
            File writtenMetaFile = new File(cnObjectDirectory.getAbsolutePath() + File.separator + metaFiles[i].getName());
            assertTrue(writtenMetaFile.exists());
        }
        assertTrue(objectPersistence.getPersistentDataFile().exists());
        assertTrue(!objectPersistence.getPersistMapping().isEmpty());
    }

    @Test
    public void testNodeMapPersistence() throws Exception {

        NodeMapPersistence testNodeMapPersistence = new NodeMapPersistence();
        testNodeMapPersistence.setPersistentDataFileName("tmpNodeMapPersistentStore");
        testNodeMapPersistence.setPersistentDataFileNamePath(testSamplesDirectory);
        testNodeMapPersistence.init();
        NodeMap persistNodeData = testNodeMapPersistence.getPersistMapping();
        assertTrue(!persistNodeData.isEmpty());
        Map<String, Date> persistMap = persistNodeData.getMap();
        for (String key : persistMap.keySet()) {
            Date timeValue = (Date) persistMap.get(key);
            logger.info("Persisted NodeId is " + key + "=" + ServiceTypeUtil.serializeDateToUTC(timeValue));
            assertTrue(key.contentEquals("r2d2"));
            assertTrue(timeValue.compareTo(ServiceTypeUtil.deserializeDateToUTC("2010-03-30T00:00:01.000+0000")) == 0);
        }

//        Map<String, Long> queueMap = objectListQueueBuilder.
//        eventPersistence.writePersistentData();
    }
    @Test
    public void testEmptyResultsQueueBuilder() throws Exception {
        mockReplication.setObjectListFile(testEmptyObjectListLocationFile);
        List<ObjectInfo> writeQueue = new ArrayList<ObjectInfo>();
        objectListQueueBuilder.setWriteQueue(writeQueue);
        objectListQueueBuilder.buildQueue();
        assertTrue(writeQueue.isEmpty());

    }
    @Test
    public void testLyingCheatingResultsQueueBuilder() throws Exception {
        mockReplication.setObjectListFile(testLyingCheatingObjectListLocationFile);
        List<ObjectInfo> writeQueue = new ArrayList<ObjectInfo>();
        objectListQueueBuilder.setWriteQueue(writeQueue);
        objectListQueueBuilder.buildQueue();
        assertTrue(writeQueue.isEmpty());

    }

    @Override
    public void setApplicationContext(ApplicationContext ac) throws BeansException {
        this.ac = ac;
    }
}
