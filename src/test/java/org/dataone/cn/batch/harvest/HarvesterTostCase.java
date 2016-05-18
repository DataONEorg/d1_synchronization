/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.batch.harvest;

import java.io.File;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.harvest.mock.MockMNRead;
import org.dataone.cn.batch.harvest.mock.MockCNCore;
//import org.junit.*;
//import static org.junit.Assert.*;
//import org.junit.runner.RunWith;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.test.context.ContextConfiguration;

/**
 *
 * These Tests need a lot more work. Only the gross functionality is tested and no hedge cases, or failure cases.
 * Testing of side effects is also absent.
 * @author rwaltz
 *
 * 
 *
 */
//@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/org/dataone/cn/batch/harvest/config/harvesterTest-context.xml"})
public class HarvesterTostCase implements ApplicationContextAware {

    static final Logger logger = Logger.getLogger(HarvesterTostCase.class);
    ApplicationContext ac;
    static final String testListIds1 = "MD_ORNLDAAC_787_03032010095920:MD_ORNLDAAC_122_03032010095920:MD_ORNLDAAC_781_03032010095920";
    static final String testListFmt1 = "FGDC-STD-001.1-1999";
    static File cnObjectDirectory;
    static File cnMetaDirectory;
    static File mnSamplesDirectory;
    static String testTmpCacheDirectory;
    static String testSamplesDirectory;
    MockCNCore mockCNCore;
    MockMNRead mockMNRead;
    File testEmptyObjectListLocationFile;
    File testLyingCheatingObjectListLocationFile;
 /*
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
    public void setObjectListQueueBuilder(ObjectListQueueReader objectListQueueBuilder) {
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
    public void setCNCore(MockCNCore mockCNCore) {
        this.mockCNCore = mockCNCore;
    }
    @Resource
    public void setMNRead(MockMNRead mockMNRead) {
        this.mockMNRead = mockMNRead;
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
        mockMNRead.setObjectListFile(testEmptyObjectListLocationFile);
        List<ObjectInfo> writeQueue = new ArrayList<ObjectInfo>();
        objectListQueueBuilder.setWriteQueue(writeQueue);
        objectListQueueBuilder.buildQueue();
        assertTrue(writeQueue.isEmpty());

    }
    @Test
    public void testLyingCheatingResultsQueueBuilder() throws Exception {
        mockMNRead.setObjectListFile(testLyingCheatingObjectListLocationFile);
        List<ObjectInfo> writeQueue = new ArrayList<ObjectInfo>();
        objectListQueueBuilder.setWriteQueue(writeQueue);
        objectListQueueBuilder.buildQueue();
        assertTrue(writeQueue.isEmpty());

    }
 *
  */
    @Override
    public void setApplicationContext(ApplicationContext ac) throws BeansException {
        this.ac = ac;
    }
 
}
