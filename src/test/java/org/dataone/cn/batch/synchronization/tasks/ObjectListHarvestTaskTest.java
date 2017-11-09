package org.dataone.cn.batch.synchronization.tasks;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.dataone.cn.batch.service.v2.NodeRegistrySyncService;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.synchronization.types.SyncObject;
import org.dataone.service.cn.v2.NodeRegistryService;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.SynchronizationFailed;
import org.dataone.service.mn.tier1.v2.MNRead;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.DescribeResponse;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectInfo;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.TypeFactory;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;
import org.dataone.service.types.v2.SystemMetadata;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ObjectListHarvestTaskTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    
    @Test
    public void testSoolToSyncQueue() throws Exception {
        ObjectListHarvestTask task = new ObjectListHarvestTask(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST"), 500);
       
         Calendar c = Calendar.getInstance();
        c.set(2017, 0, 1, 12, 0);
        long startTime = c.getTime().getTime();
        ObjectList ol = new ObjectList();
        for (int i=0; i < 3333; i++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("foo."+i));
            oi.setDateSysMetadataModified(new Date(startTime + 60000*i));
            ol.addObjectInfo(oi);
        }
        
        NodeComm nc = buildNodeComm(
                new Date(startTime -100 ), // lastHarvestDate just before the objectList contents
                ol,
                1000,  // page size - lower than batchSize
                true,  //  paging
                null,   // which exception to throw
                null);  // when to throw it
        
        SortedHarvestTimepointMap retrieved =task.getFullObjectList(nc,
                5000    // maxToHarvest
                );
        
        assertEquals("should have harvested all",3333,retrieved.getHarvestSize());
        assertEquals("earliest should match",new Date(startTime), retrieved.getEarliestTimePoint());
        assertEquals("latest should match",new Date(startTime + 3332*60000), retrieved.getLatestTimePoint());
        
        
        BlockingQueue<SyncObject> mockSyncQueue = new ArrayBlockingQueue<>(10000);
        task.spoolToSynchronizationQueue(retrieved, mockSyncQueue, nc.getNodeRegistryService(),60);
        assertEquals("latestHarvestDate should be updated",new Date(startTime + 60000*3332),
                nc.getNodeRegistryService().getDateLastHarvested(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST")));
        assertEquals("the syncQueue should have all of the harvest", 3333,mockSyncQueue.size());
    }
    
    
    @Test
    public void testAllInOneHarvest_normal() throws NotFound, ServiceFailure, InvalidRequest, InvalidToken, NotAuthorized, NotImplemented {
        ObjectListHarvestTask task = new ObjectListHarvestTask(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST"), 100);
        
        Calendar c = Calendar.getInstance();
        c.set(2017, 0, 1, 12, 0);
        long startTime = c.getTime().getTime();
        ObjectList ol = new ObjectList();
        for (int i=0; i < 3333; i++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("foo."+i));
            oi.setDateSysMetadataModified(new Date(startTime + 60000*i));
            ol.addObjectInfo(oi);
        }
        
        SortedHarvestTimepointMap retrieved = task.getFullObjectList(buildNodeComm(
                new Date(startTime -1000000), // last harvested
                ol,
                10000,  // page size - doesn't matter here, so set very high
                false,  // no paging
                null,   // which exception to throw
                null),  // when to throw it
                5000    // maxToHarvest
                );
        
        assertEquals("should have harvested all",3333,retrieved.getHarvestSize());
        assertEquals("earliest should match",new Date(startTime),retrieved.getEarliestTimePoint());
        assertEquals("latest should match",new Date(startTime + 60000*3332),retrieved.getLatestTimePoint());
 
        // test harvest limit
        SortedHarvestTimepointMap retrieved2 = task.getFullObjectList(buildNodeComm(
                new Date(startTime -1000000), // last harvested
                ol,
                10000,  // page size - doesn't matter here, so set very high
                false,  // no paging
                null,   // which exception to throw
                null),  // when to throw it
                1000    // maxToHarvest
                );
        
        assertEquals("should have harvested all",1000,retrieved2.getHarvestSize());
        assertEquals("latest should match",new Date(startTime + 60000*999),retrieved2.getLatestTimePoint());
    }

    @Test
    public void testAllInOneHarvest_incomplete() throws NotFound, ServiceFailure, InvalidToken, NotAuthorized, NotImplemented {
        ObjectListHarvestTask task = new ObjectListHarvestTask(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST"), 100);
        
        Calendar c = Calendar.getInstance();
        c.set(2017, 0, 1, 12, 0);
        long startTime = c.getTime().getTime();
        ObjectList ol = new ObjectList();
        for (int i=0; i < 3333; i++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("foo."+i));
            oi.setDateSysMetadataModified(new Date(startTime + 60000*i));
            ol.addObjectInfo(oi);
        }
        
        try {
            SortedHarvestTimepointMap retrieved = task.getFullObjectList(buildNodeComm(
                    new Date(startTime -1000000), // last harvested
                    ol,
                    1000,  // page size - set low to simulate incomplete harvest
                    false,  // no paging
                    null,   // which exception to throw
                    null),  // when to throw it
                    5000    // maxToHarvest
                    );
            fail("should not get a successful harvest");
        } catch (InvalidRequest e) {
            ; //expected outcome
        }
    }
    
    
    @Test(expected=ServiceFailure.class)
    public void testAllInOneHarvest_retrievalProblems() throws Exception {
        ObjectListHarvestTask task = new ObjectListHarvestTask(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST"), 10000);
    
    
        Calendar c = Calendar.getInstance();
        c.set(2017, 0, 1, 12, 0);
        long startTime = c.getTime().getTime();
        ObjectList ol = new ObjectList();
        for (int i=0; i < 3333; i++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("foo."+i));
            oi.setDateSysMetadataModified(new Date(startTime + 60000*i));
            ol.addObjectInfo(oi);
        }
        
        SortedHarvestTimepointMap retrieved = task.getFullObjectList(buildNodeComm(
                new Date(startTime -100 ), // lastHarvestDate just before the objectList contents
                ol,
                10000,  // page size - lower than batchSize
                false,  // paging
                new ServiceFailure("123","problem getting 3rd page"),   // which exception to throw
                0),  // when to throw it - the first and only call
                5000    // maxToHarvest
                );
        
        fail("harvest should fail");
    }
    
    
    
    @Test
    public void testAllInOneHarvest_empty() throws NotFound, ServiceFailure, InvalidToken, NotAuthorized, NotImplemented, InvalidRequest {
        ObjectListHarvestTask task = new ObjectListHarvestTask(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST"), 100);
        
        Calendar c = Calendar.getInstance();
        c.set(2017, 0, 1, 12, 0);
        long startTime = c.getTime().getTime();
        ObjectList ol = new ObjectList();
        for (int i=0; i < 3333; i++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("foo."+i));
            oi.setDateSysMetadataModified(new Date(startTime + 60000*i));
            ol.addObjectInfo(oi);
        }
        
        // this should return 0, because lastHarvestDate is set later than al of the objectInfos
      
        SortedHarvestTimepointMap retrieved = task.getFullObjectList(buildNodeComm(
                new Date(startTime + 60000*4000 ), // last harvested is set to after the latest in the objectList
                ol,
                10000,  // page size - intentional high
                false,  // no paging
                null,   // which exception to throw
                null),  // when to throw it
                5000    // maxToHarvest
                );
            
        assertEquals("should have harvested none",0,retrieved.getHarvestSize());
        assertEquals("earliest should match",null, retrieved.getEarliestTimePoint());
        assertEquals("latest should match",null, retrieved.getLatestTimePoint());
 
    }


    @Test
    public void testPagedHarvest_normal() throws Exception {
        ObjectListHarvestTask task = new ObjectListHarvestTask(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST"), 500);
        
        Calendar c = Calendar.getInstance();
        c.set(2017, 0, 1, 12, 0);
        long startTime = c.getTime().getTime();
        ObjectList ol = new ObjectList();
        for (int i=0; i < 3333; i++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("foo."+i));
            oi.setDateSysMetadataModified(new Date(startTime + 60000*i));
            ol.addObjectInfo(oi);
        }
        
        SortedHarvestTimepointMap retrieved = task.getFullObjectList(buildNodeComm(
                new Date(startTime -100 ), // lastHarvestDate just before the objectList contents
                ol,
                1000,  // page size - lower than batchSize
                true,  //  paging
                null,   // which exception to throw
                null),  // when to throw it
                5000    // maxToHarvest
                );
        
        assertEquals("should have harvested all",3333,retrieved.getHarvestSize());
        assertEquals("earliest should match",new Date(startTime), retrieved.getEarliestTimePoint());
        assertEquals("latest should match",new Date(startTime + 3332*60000), retrieved.getLatestTimePoint());
        
        // a limited harvest
        SortedHarvestTimepointMap retrieved2 = task.getFullObjectList(buildNodeComm(
                new Date(startTime -100 ), // lastHarvestDate just before the objectList contents
                ol,
                1000,  // page size - lower than batchSize
                true,  //  paging
                null,   // which exception to throw
                null),  // when to throw it
                1000    // maxToHarvest
                );
        
        assertEquals("should have harvested all",1000,retrieved2.getHarvestSize());
        assertEquals("latest should match",new Date(startTime + 60000*999),retrieved2.getLatestTimePoint());
        
    }

    @Test(expected=ServiceFailure.class)
    public void testPagedHarvest_retrievalProblems() throws Exception {
        ObjectListHarvestTask task = new ObjectListHarvestTask(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST"), 100);
    
    
        Calendar c = Calendar.getInstance();
        c.set(2017, 0, 1, 12, 0);
        long startTime = c.getTime().getTime();
        ObjectList ol = new ObjectList();
        for (int i=0; i < 3333; i++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("foo."+i));
            oi.setDateSysMetadataModified(new Date(startTime + 60000*i));
            ol.addObjectInfo(oi);
        }
        
        SortedHarvestTimepointMap retrieved = task.getFullObjectList(buildNodeComm(
                new Date(startTime -100 ), // lastHarvestDate just before the objectList contents
                ol,
                1000,  // page size - lower than batchSize
                true,  // paging
                new ServiceFailure("123","problem getting 3rd page"),   // which exception to throw
                3),  // when to throw it
                5000    // maxToHarvest
                );
        
        fail("harvest should fail");
    }
    
    
    @Test
    public void testPagedHarvest_empty() throws NotFound, ServiceFailure, InvalidToken, NotAuthorized, NotImplemented, InvalidRequest {
        ObjectListHarvestTask task = new ObjectListHarvestTask(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST"), 100);
        
        Calendar c = Calendar.getInstance();
        c.set(2017, 0, 1, 12, 0);
        long startTime = c.getTime().getTime();
        ObjectList ol = new ObjectList();
        for (int i=0; i < 3333; i++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("foo."+i));
            oi.setDateSysMetadataModified(new Date(startTime + 60000*i));
            ol.addObjectInfo(oi);
        }
        
        // this should return 0, because lastHarvestDate is set later than al of the objectInfos
      
        SortedHarvestTimepointMap retrieved = task.getFullObjectList(buildNodeComm(
                new Date(startTime + 60000*4000 ), // last harvested is set to after the latest in the objectList
                ol,
                10000,  // page size - intentional high
                true,  // no paging
                null,   // which exception to throw
                null),  // when to throw it
                5000    // maxToHarvest
                );
            
        assertEquals("should have harvested none",0,retrieved.getHarvestSize());
        assertEquals("earliest should match",null, retrieved.getEarliestTimePoint());
        assertEquals("latest should match",null, retrieved.getLatestTimePoint());
 
    }

    

        
    private NodeComm buildNodeComm(final Date initialLastHarvest, final ObjectList objectList, final Integer pageSize, final Boolean supportsPaging, final Exception exception, final Integer errorOnRequestNumber) {
            
       
        MNRead mnread = new MNRead() {
            
            int pages = 0;
            
            @Override
            public ObjectList listObjects(Session arg0, Date fromDate, Date toDate,
                    ObjectFormatIdentifier arg3, Identifier arg4, Boolean arg5,
                    Integer start, Integer requestedCount) throws InvalidRequest,
                    InvalidToken, NotAuthorized, NotImplemented, ServiceFailure {
                
                if (!supportsPaging) {
                    if (start != null || requestedCount != null) {
                        throw new InvalidRequest("code","listObjects does not support 'start' and 'count' parameters");
                    }
                }
                if (errorOnRequestNumber != null && pages++ == errorOnRequestNumber) {
                    if (exception instanceof ServiceFailure) {
                        throw (ServiceFailure) exception; 
                    }
                    if (exception instanceof InvalidRequest) {
                        throw (InvalidRequest) exception; 
                    }
                }
                
                
                // filter the object list by from and to date
                
                
                List<ObjectInfo> filteredList = new ArrayList<>();
                for (ObjectInfo oi : objectList.getObjectInfoList()) {
                    if (fromDate != null && oi.getDateSysMetadataModified().before(fromDate)) {
                        continue;
                    }
                    if (toDate != null && !oi.getDateSysMetadataModified().before(toDate)) {
                        continue;
                    }
                    filteredList.add(oi);
                }
                // do any paging needed
                if (start == null) start = 0;
                    
                SortedSet<Integer> possibleReturnCounts = new TreeSet<>();
                possibleReturnCounts.add(pageSize);
                if (requestedCount != null) possibleReturnCounts.add(requestedCount);
                possibleReturnCounts.add(filteredList.size()-start);

                // get the smallest value
                int returnCount = possibleReturnCounts.first();
                
                ObjectList ol = new ObjectList();  
                if (returnCount > 0) { 
                    ol.setObjectInfoList(filteredList.subList(start, start + returnCount));
                }
                ol.setTotal(filteredList.size());
                System.out.println(String.format("listObjects(%s,%s,%d,%d) returning %d out of %d objectInfos",fromDate,toDate,start,requestedCount,returnCount,ol.getTotal()));
                return ol;
            }

            /////////////////////////////// NOT IMPLEMENTED ///////////////////////////////
            @Override
            public DescribeResponse describe(Session arg0, Identifier arg1)
                    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound {
                throw new NotImplemented("","");
            }
            @Override
            public InputStream get(Session arg0, Identifier arg1)
                    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound, InsufficientResources {
               
                throw new NotImplemented("","");
            }
            @Override
            public Checksum getChecksum(Session arg0, Identifier arg1, String arg2) 
                    throws InvalidRequest, InvalidToken,  NotAuthorized, NotImplemented, ServiceFailure, NotFound {
                throw new NotImplemented("","");
            }
            @Override
            public InputStream getReplica(Session arg0, Identifier arg1)
                    throws InvalidToken, NotAuthorized, NotImplemented,   ServiceFailure, NotFound, InsufficientResources {
                throw new NotImplemented("","");
            }
            @Override
            public SystemMetadata getSystemMetadata(Session arg0,
                    Identifier arg1) throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound {
                throw new NotImplemented("","");
            }
            @Override
            public boolean synchronizationFailed(Session arg0,  SynchronizationFailed arg1) 
                    throws InvalidToken,  NotAuthorized, NotImplemented, ServiceFailure {
                throw new NotImplemented("","");
            }
            @Override
            public boolean systemMetadataChanged(Session arg0, Identifier arg1,   long arg2, Date arg3) 
                    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented, InvalidRequest {
                throw new NotImplemented("","");
            }
            
        };
        

        
        NodeRegistrySyncService nrs = new NodeRegistrySyncService() {
            
            Date lastHarvested = initialLastHarvest;
            Node node = new Node();
            { 
                node.setIdentifier(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST"));
                node.setBaseURL("theBaseUrl");
            }

            @Override
            public NodeList listNodes() throws ServiceFailure, NotImplemented {
                throw new NotImplemented("sadfaf","asdfasdf");
            }

            @Override
            public Node getNode(NodeReference nodeId) throws NotFound, ServiceFailure {
                return node;            }

            @Override
            public void setDateLastHarvested(NodeReference nodeIdentifier, Date lastDateNodeHarvested) throws ServiceFailure {
                lastHarvested = lastDateNodeHarvested;
            }

            @Override
            public Date getDateLastHarvested(NodeReference nodeIdentifier) throws ServiceFailure {
                return lastHarvested;
            }
        };
        
        return new NodeComm(mnread,nrs);
        
    }
    
    
    
}
