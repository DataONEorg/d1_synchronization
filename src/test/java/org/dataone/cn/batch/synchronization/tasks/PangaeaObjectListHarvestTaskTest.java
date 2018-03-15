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
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.dataone.client.rest.DefaultHttpMultipartRestClient;
import org.dataone.client.v1.itk.D1Client;
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

public class PangaeaObjectListHarvestTaskTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testTrue() {
        ;
    }
    
 //   @Test
    public void testSpoolToSyncQueue() throws Exception {
        ObjectListHarvestTask task = new ObjectListHarvestTask(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST"), 500);
       
         Calendar c = Calendar.getInstance();
        c.set(1900, 0, 1, 0, 0);
        long startTime = c.getTime().getTime();
        ObjectList ol = new ObjectList();
//        for (int i=0; i < 3333; i++) {
//            ObjectInfo oi = new ObjectInfo();
//            oi.setIdentifier(TypeFactory.buildIdentifier("foo."+i));
//            oi.setDateSysMetadataModified(new Date(startTime + 60000*i));
//            ol.addObjectInfo(oi);
//        }
        
        NodeComm nc = buildNodeComm(
                new Date(startTime), // lastHarvestDate 
                ol,
                1000,  // page size - lower than batchSize
                true,  //  paging
                null,   // which exception to throw
                null);  // when to throw it
        
        SortedHarvestTimepointMap retrieved =task.getFullObjectList(nc,
                5000    // maxToHarvest
                );
          
 
        
        
        BlockingQueue<SyncObject> mockSyncQueue = new ArrayBlockingQueue<>(10000);
//        task.spoolToSynchronizationQueue(retrieved, mockSyncQueue, nc.getNodeRegistryService(),60);
        assertEquals("latestHarvestDate should be updated",new Date(startTime + 60000*3332),
                nc.getNodeRegistryService().getDateLastHarvested(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST")));
        assertEquals("the syncQueue should have all of the harvest", 3333,mockSyncQueue.size());
    }
    
    
    
 //   @Test
    public void testMultipleHarvests_by_HarvestLimit() throws Exception {
        int requestedPageSize = 50000;
        ObjectListHarvestTask task = new ObjectListHarvestTask(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST"), requestedPageSize);
       
        Calendar c = Calendar.getInstance();
        c.set(2015, 0, 1, 12, 0);
        c.setTimeZone(TimeZone.getDefault());
        long startTime = c.getTime().getTime();
        ObjectList ol = new ObjectList();
//        for (int i=0; i < 2222; i++) {
//            ObjectInfo oi = new ObjectInfo();
//            oi.setIdentifier(TypeFactory.buildIdentifier("foo."+i));
//            oi.setDateSysMetadataModified(new Date(startTime + 60000*i));
//            ol.addObjectInfo(oi);
//        }
        
        // first harvest
 
        
        NodeComm nc = buildNodeComm(
                new Date(startTime  ), // lastHarvestDate 
                ol,
                7000,  // mn listObjects page size - lower than batchSize
                true,  //  paging
                null,   // which exception to throw
                null);  // when to throw it
        
        BlockingQueue<SyncObject> mockSyncQueue = new ArrayBlockingQueue<>(200000);
        
        
        int index = 0;
        while (index < 10) {
        
            SortedHarvestTimepointMap retrieved = task.getFullObjectList(
                    nc,
                    50000    // maxToHarvest
                    );

            System.out.print(String.format("Harvest %3d : earliest = %s   latest = %s count = %4d ",
                    index, 
                    retrieved.getEarliestTimePoint(), 
                    retrieved.getLatestTimePoint(),
                    retrieved.getHarvestSize()));

 //           task.spoolToSynchronizationQueue(retrieved, mockSyncQueue, nc.getNodeRegistryService(),10000);

            System.out.println(" queue size: " + mockSyncQueue.size());
            
            index++;
        } 
    }
    
    /**
     * this test uses the same minute-by-minute update
     * @throws Exception
     */
 //   @Test
    public void testMultipleHarvests_timepoint_gt_harvestMax() throws Exception {
        int requestedPageSize = 500;
        ObjectListHarvestTask task = new ObjectListHarvestTask(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST"), requestedPageSize);
       
        Calendar c = Calendar.getInstance();
        c.set(2017, 0, 1, 12, 0);
        c.setTimeZone(TimeZone.getDefault());
        long startTime = c.getTime().getTime();
        ObjectList ol = new ObjectList();
        for (int i=0; i < 2222; i++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("foo."+i));
            oi.setDateSysMetadataModified(new Date(startTime + 60000*i));
            ol.addObjectInfo(oi);
        }
        
        for (int i=0; i < 1500; i++) {
            ObjectInfo oi = new ObjectInfo();
            oi.setIdentifier(TypeFactory.buildIdentifier("gloop."+i));
            oi.setDateSysMetadataModified(new Date(startTime + 60000*200));
            ol.addObjectInfo(oi);
        }
        
        // first harvest
        
        NodeComm nc = buildNodeComm(
                new Date(startTime -100 ), // lastHarvestDate just before the objectList contents
                ol,
                700,  // mn page size - lower than batchSize
                true,  //  paging
                null,   // which exception to throw
                null);  // when to throw it
        
        SortedHarvestTimepointMap retrieved =task.getFullObjectList(nc,
                1000    // maxToHarvest
                );
        
        // the large timepoint shouldn't process
        
        assertEquals("should have harvested only before large timepoint",200,retrieved.getHarvestSize());
        assertEquals("earliest should match",new Date(startTime), retrieved.getEarliestTimePoint());
        assertEquals("latest should match",new Date(startTime + 199*60000), retrieved.getLatestTimePoint());
        
        
        BlockingQueue<SyncObject> mockSyncQueue = new ArrayBlockingQueue<>(10000);
//        task.spoolToSynchronizationQueue(retrieved, mockSyncQueue, nc.getNodeRegistryService(),60);
        assertEquals("latestHarvestDate should be updated",new Date(startTime + 60000*199),
                nc.getNodeRegistryService().getDateLastHarvested(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST")));
        assertEquals("the syncQueue should have only first 200 of the harvest", 200 ,mockSyncQueue.size());

        assertEquals("the nodeRegistry last harvest date should equal the latest of those retrieved (and held)", 
                retrieved.getLatestTimePoint(),
                nc.getNodeRegistryService().getDateLastHarvested(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST")));

        SortedHarvestTimepointMap secondHarvest = task.getFullObjectList(nc,
                1000    // maxToHarvest  this should get the rest
                );
        
        assertEquals("should have harvested all",1501,secondHarvest.getHarvestSize());
        assertEquals("earliest should match",new Date(startTime + 60000*200 ), secondHarvest.getEarliestTimePoint());
        assertEquals("latest should match",new Date(startTime + 60000*200), secondHarvest.getLatestTimePoint());
        
        
//        task.spoolToSynchronizationQueue(secondHarvest, mockSyncQueue, nc.getNodeRegistryService(),60);
        assertEquals("latestHarvestDate should be updated",new Date(startTime + 60000*200),
                nc.getNodeRegistryService().getDateLastHarvested(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST")));
        assertEquals("the syncQueue should have all of the harvest", 1501 + 200 ,mockSyncQueue.size());
        
        
        
        SortedHarvestTimepointMap thirdHarvest = task.getFullObjectList(nc,
                2500    // maxToHarvest  this should get the rest
                );
        
        assertEquals("should have harvested all",2222-1-200,thirdHarvest.getHarvestSize());
        assertEquals("earliest should match",new Date(startTime + 60000*201 ), thirdHarvest.getEarliestTimePoint());
        assertEquals("latest should match",new Date(startTime + 60000*2221), thirdHarvest.getLatestTimePoint());
        
        
//        task.spoolToSynchronizationQueue(thirdHarvest, mockSyncQueue, nc.getNodeRegistryService(),60);
        assertEquals("latestHarvestDate should be updated",new Date(startTime + 60000*2221),
                nc.getNodeRegistryService().getDateLastHarvested(TypeFactory.buildNodeReference("urn:node:HARVEST_TEST")));
        assertEquals("the syncQueue should have all of the harvest", 1500 + 2222 ,mockSyncQueue.size());
        
    }
    
 

    

        
    private NodeComm buildNodeComm(final Date initialLastHarvest, final ObjectList objectList, final Integer pageSize, final Boolean supportsPaging, final Exception exception, final Integer errorOnRequestNumber) throws ServiceFailure {
            
       
        org.dataone.service.mn.tier1.v1.MNRead mnread = D1Client.getMN( "https://pangaea-dev-orc-1.test.dataone.org/mn");
        
//        {
//            
//            int pages = 0;
//            
//            @Override
//            public ObjectList listObjects(Session arg0, Date fromDate, Date toDate,
//                    ObjectFormatIdentifier arg3, Identifier arg4, Boolean arg5,
//                    Integer start, Integer requestedCount) throws InvalidRequest,
//                    InvalidToken, NotAuthorized, NotImplemented, ServiceFailure {
//                
//                if (!supportsPaging) {
//                    if (start != null || requestedCount != null) {
//                        throw new InvalidRequest("code","listObjects does not support 'start' and 'count' parameters");
//                    }
//                }
//                if (errorOnRequestNumber != null && pages++ == errorOnRequestNumber) {
//                    if (exception instanceof ServiceFailure) {
//                        throw (ServiceFailure) exception; 
//                    }
//                    if (exception instanceof InvalidRequest) {
//                        throw (InvalidRequest) exception; 
//                    }
//                }
//                
//                
//                // filter the object list by from and to date
//                
//                
//                List<ObjectInfo> filteredList = new ArrayList<>();
//                for (ObjectInfo oi : objectList.getObjectInfoList()) {
//                    if (fromDate != null && oi.getDateSysMetadataModified().before(fromDate)) {
//                        continue;
//                    }
//                    if (toDate != null && !oi.getDateSysMetadataModified().before(toDate)) {
//                        continue;
//                    }
//                    filteredList.add(oi);
//                }
//                // do any paging needed
//                if (start == null) start = 0;
//                    
//                SortedSet<Integer> possibleReturnCounts = new TreeSet<>();
//                possibleReturnCounts.add(pageSize);
//                if (requestedCount != null) possibleReturnCounts.add(requestedCount);
//                possibleReturnCounts.add(filteredList.size()-start);
//
//                // get the smallest value
//                int returnCount = possibleReturnCounts.first();
//                
//                ObjectList ol = new ObjectList();  
//                if (returnCount > 0) { 
//                    ol.setObjectInfoList(filteredList.subList(start, start + returnCount));
//                }
//                ol.setTotal(filteredList.size());
//                System.out.println(String.format("listObjects(%s,%s,%d,%d) returning %d out of %d objectInfos",fromDate,toDate,start,requestedCount,returnCount,ol.getTotal()));
//                return ol;
//            }
//
//            /////////////////////////////// NOT IMPLEMENTED ///////////////////////////////
//            @Override
//            public DescribeResponse describe(Session arg0, Identifier arg1)
//                    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound {
//                throw new NotImplemented("","");
//            }
//            @Override
//            public InputStream get(Session arg0, Identifier arg1)
//                    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound, InsufficientResources {
//               
//                throw new NotImplemented("","");
//            }
//            @Override
//            public Checksum getChecksum(Session arg0, Identifier arg1, String arg2) 
//                    throws InvalidRequest, InvalidToken,  NotAuthorized, NotImplemented, ServiceFailure, NotFound {
//                throw new NotImplemented("","");
//            }
//            @Override
//            public InputStream getReplica(Session arg0, Identifier arg1)
//                    throws InvalidToken, NotAuthorized, NotImplemented,   ServiceFailure, NotFound, InsufficientResources {
//                throw new NotImplemented("","");
//            }
//            @Override
//            public SystemMetadata getSystemMetadata(Session arg0,
//                    Identifier arg1) throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound {
//                throw new NotImplemented("","");
//            }
//            @Override
//            public boolean synchronizationFailed(Session arg0,  SynchronizationFailed arg1) 
//                    throws InvalidToken,  NotAuthorized, NotImplemented, ServiceFailure {
//                throw new NotImplemented("","");
//            }
//            @Override
//            public boolean systemMetadataChanged(Session arg0, Identifier arg1,   long arg2, Date arg3) 
//                    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented, InvalidRequest {
//                throw new NotImplemented("","");
//            }
//            
//        };
        

        
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
                
                this.lastHarvested = lastDateNodeHarvested;
                System.out.println("NodeRegistrySyncService setting dateLastHarvested: " + this.lastHarvested);
            }

            @Override
            public Date getDateLastHarvested(NodeReference nodeIdentifier) throws ServiceFailure {
                return lastHarvested;
            }
        };
        
        return new NodeComm(mnread,nrs);
        
    }
    
    
    
}
