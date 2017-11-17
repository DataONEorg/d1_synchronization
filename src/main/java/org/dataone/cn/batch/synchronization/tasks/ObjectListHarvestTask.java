/**
 * This work was created by participants in the DataONE project, and is jointly copyrighted by participating
 * institutions in DataONE. For more information on DataONE, see our web site at http://dataone.org.
 *
 * Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * $Id$
 */
package org.dataone.cn.batch.synchronization.tasks;

import java.io.Serializable;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.dataone.cn.ComponentActivationUtility;
import org.dataone.cn.batch.exceptions.ExecutionDisabledException;
import org.dataone.cn.batch.exceptions.NodeCommUnavailable;
import org.dataone.cn.batch.service.v2.NodeRegistrySyncService;
import org.dataone.cn.batch.synchronization.NodeCommFactory;
import org.dataone.cn.batch.synchronization.NodeCommObjectListHarvestFactory;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.log.MetricEvent;
import org.dataone.cn.log.MetricLogClient;
import org.dataone.cn.log.MetricLogClientFactory;
import org.dataone.cn.log.MetricLogEntry;
import org.dataone.cn.synchronization.types.SyncObject;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.mn.tier1.v2.MNRead;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v2.Node;
import org.dataone.service.util.DateTimeMarshaller;
import org.joda.time.MutableDateTime;

import com.hazelcast.client.HazelcastClient;

/**
 * An executable task that retrieve a list of ObjectInfos by calling listObject on a MN and then submits them on the
 * SyncTaskQueue for processing. It will retrieve and submit in batches of 1000 as the default.
 *
 * As an executable, it will return a date that is the latest DateSysMetadataModified of the processed nodelist
 *
 * @author waltz
 */
public class ObjectListHarvestTask implements Callable<Date>, Serializable {

    NodeReference d1NodeReference;
    private Session session;
    Integer batchSize;
    MutableDateTime currentDateTime = new MutableDateTime(new Date());
    Date endHarvestInterval;
    int backoffSeconds = 10;

    /* there are so many logging statements, I'm prefixing them with underscore to make for easier source code reading */
    static final Logger __logger = Logger.getLogger(ObjectListHarvestTask.class);
    static final MetricLogClient __metricLogger = MetricLogClientFactory.getMetricLogClient();
    private int __syncMetricTotalSubmitted;
    private int __syncMetricTotalRetrieved;

    /**
     * Define the ObjectList Harvest task for a Member Node.
     *
     * @param d1NodeReference
     * @param batchSize
     */
    public ObjectListHarvestTask(NodeReference d1NodeReference, Integer batchSize) {
        this.d1NodeReference = d1NodeReference;
        this.batchSize = batchSize;
    }

    /**
     * Calls listObjects on the Member Node and manages putting SyncObjects onto the SynchronizationObjectQueue (one per
     * object) for asynchronous processing. Also controls the lastHarvestedDate field in the NodeRegistry.
     *
     * Method to be called in a separately executing thread.
     *
     * Harvest from the MemberNode and add all of the pids to the synchronization queue
     * The strategy is to accumulate the objectInfos for the full time window and sort
     * the items into ascending chronological order.  Periodically the lastHarvestedDate
     * will be updated to avoid a complete reharvest in the face of system failures.  
     * Exceptions thrown means some or all will be reharvested (depending on whether an
     * intermediate update of lastHarvestedDate was able to take place).
     * 
     * @returns Date - when execution ends
     * @throws NodeCommUnavailable - if the harvest task can't get a NodeComm from the pool
     * @throws InterruptedException - if puts to the synchronizationQueue are interrupted while waiting
     * @throws ServiceFailure - if any communication problem with the synchronizationQueue, the NodeComm service, or nodeRegistry
     * @throws ExecutionDisabledException - if synchronization is disabled
     * @throws NotImplemented - from listObjects
     * @throws NotAuthorized - from listObjects
     * @throws InvalidToken  - from listObjects
     * @throws InvalidRequest - from listObjects
     * @throws NotFound - from listObjects
     */
    @Override
    public Date call() throws Exception {
        // throws NodeCommUnavailable, InterruptedException, ServiceFailure, ExecutionDisabledException, NotFound, InvalidRequest, InvalidToken, NotAuthorized, NotImplemented {
   
 
        __logger.info(d1NodeReference.getValue() + "- Calling ObjectListHarvestTask");
        __syncMetricTotalSubmitted = 0;
        __syncMetricTotalRetrieved = 0;       
        
        if (!ComponentActivationUtility.synchronizationIsActive()) {
            // throwing this exception will not update
            // last harvested date on the node, causing
            // a reharvesting of all records
            ExecutionDisabledException ex = new ExecutionDisabledException(d1NodeReference.getValue() + "- Disabled");
            throw ex;
        }
        

        // try to get NodeComms
        NodeCommFactory nodeCommFactory = NodeCommObjectListHarvestFactory.getInstance();
        NodeComm mnNodeComm = nodeCommFactory.getNodeComm(d1NodeReference);
        NodeRegistrySyncService nodeRegistryService = mnNodeComm.getNodeRegistryService();
        
        // get SynchronizationQueue and stats
        String synchronizationObjectQueue = Settings.getConfiguration().getString("dataone.hazelcast.synchronizationObjectQueue");
        Integer maxSyncObjectQueueSize    = Settings.getConfiguration().getInt("Synchronization.max_syncobjectqueue_size",50000);
        Integer maxHarvestSize            = Settings.getConfiguration().getInt("Synchronization.max_harvest_size",50000);
        Integer requeueTolerance          = Settings.getConfiguration().getInt("Synchronization.harvest_update_latestHarvestDate_frequency",100);
        
        HazelcastClient hazelcast = HazelcastClientFactory.getProcessingClient();        
        BlockingQueue<SyncObject> hzSyncObjectQueue = hazelcast.getQueue(synchronizationObjectQueue);
        
        
        // limit the number of pids to submit to the synchronization queue
        int maximumToHarvest = Math.min(maxHarvestSize, maxSyncObjectQueueSize - hzSyncObjectQueue.size());
        __logger.info(d1NodeReference.getValue() + " - harvest limited to " + maximumToHarvest + " items.");
        
        if (maximumToHarvest > 0) {


            SortedHarvestTimepointMap harvest = getFullObjectList(mnNodeComm, maximumToHarvest);


            spoolToSynchronizationQueue(harvest, hzSyncObjectQueue, nodeRegistryService, requeueTolerance);


            __logger.info(d1NodeReference.getValue() + "- ObjectListHarvestTask End");

            MetricLogEntry __metricHarvestRetrievedLogEvent = new MetricLogEntry(
                    MetricEvent.SYNCHRONIZATION_HARVEST_RETRIEVED,
                    d1NodeReference, null, Integer.toString(__syncMetricTotalRetrieved));
            Date __harvestMetricLogDate = (Date)__metricHarvestRetrievedLogEvent.getDateLogged().clone();
            __metricLogger.logMetricEvent(__metricHarvestRetrievedLogEvent);

            MetricLogEntry __metricHarvestSubmittedLogEvent = new MetricLogEntry(
                    MetricEvent.SYNCHRONIZATION_HARVEST_SUBMITTED,
                    d1NodeReference, null, Integer.toString(__syncMetricTotalSubmitted));
            // make the second the same Date for better alignment in viewed logs.
            __metricHarvestSubmittedLogEvent.setDateLogged(__harvestMetricLogDate);
            __metricLogger.logMetricEvent(__metricHarvestSubmittedLogEvent);
        }

        // return the date of completion of the task
        // (not sure how this is used)
        return new Date();
    }
  
    
    /** 
     * Submit the harvest to the sync queue, periodically updating the node's lastHarvestedDate
     * (able to do this since the pids are already sorted)
     * @param harvest
     * @param hzSyncObjectQueue
     * @param nodeRegistryService
     * @throws InterruptedException
     * @throws ServiceFailure
     */
    protected void spoolToSynchronizationQueue(
            SortedHarvestTimepointMap harvest, 
            BlockingQueue<SyncObject> hzSyncObjectQueue, 
            NodeRegistrySyncService nodeRegistryService,
            Integer requeueTolerance)  throws InterruptedException, ServiceFailure {
 

        int vulnerableToReharvest = 0;

        Date currentModDate = null;
        Iterator<Entry<Date,List<String>>> it = harvest.getAscendingIterator();
        while (it.hasNext()) 
        {
            Entry<Date,List<String>> timepoint = it.next();

            // submit all pids in the timepoint
            for(String pidString : timepoint.getValue()) {

                SyncObject syncObject = new SyncObject(this.d1NodeReference.getValue(), pidString);

                __syncMetricTotalSubmitted++;
                hzSyncObjectQueue.put(syncObject);
                __logger.trace("placed on hzSyncObjectQueue- " + syncObject.taskLabel());
                vulnerableToReharvest++;
            }

            currentModDate = timepoint.getKey();

            if (vulnerableToReharvest >= requeueTolerance) {
                try {
                    nodeRegistryService.setDateLastHarvested(d1NodeReference, currentModDate);
                    __logger.info(this.d1NodeReference.getValue() + " - updated lastHarvestedDate to " + currentModDate);
                    vulnerableToReharvest = 0;

                } catch (ServiceFailure e) {
                    // how far to let the lastHarvestedDate lag behind the queue submissions?
                    __logger.error(this.d1NodeReference.getValue() + " harvest - nodeRegistry not accepting new lastHarvestedDate!");
                    throw e;
                }
            } 
        }
        // after all are processed set lastHarvestedDate to the currentModDate.
        try {
            if (currentModDate != null) {
                nodeRegistryService.setDateLastHarvested(d1NodeReference, currentModDate);
                __logger.info(this.d1NodeReference.getValue() + " - updated lastHarvestedDate to " + currentModDate + " ***end of harvest***");
            }
        } catch (ServiceFailure e) {
            // how far to let the lastHarvestedDate lag behind the queue submissions?
            __logger.warn(this.d1NodeReference.getValue() + " harvest - nodeRegistry not accepting new lastHarvestedDate!");
        }

    }
    
    
    
    /**
     * get the set of timepoints and associated pids to put on the sync queue
     * @param nodeComm
     * @return
     * @throws NotFound
     * @throws ServiceFailure
     * @throws InvalidRequest
     * @throws NotImplemented 
     * @throws NotAuthorized 
     * @throws InvalidToken 
     * @throws ExecutionDisabledException 
     */
    protected SortedHarvestTimepointMap getFullObjectList(NodeComm nodeComm, Integer maxToHarvest) 
            throws NotFound, ServiceFailure, InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ExecutionDisabledException {
        
        
        // 1. set up the harvest time interval of
        //
        //       lastHarvestDate to (currentTime - backoffSeconds)
        //
        // the backoff is to help ensure that objects with the same date
        // are all loaded and available in listObjects.
        // Otherwise, the next harvest would miss those.
        Date lastModifiedDate = nodeComm.getNodeRegistryService().getDateLastHarvested(d1NodeReference);
 
        MutableDateTime startHarvestDateTime = new MutableDateTime(lastModifiedDate);
        startHarvestDateTime.addMillis(1);
        Date startHarvestDate = startHarvestDateTime.toDate();
        
        currentDateTime.addSeconds(-backoffSeconds);
        endHarvestInterval = currentDateTime.toDate();
        
        
        
        
        if (__logger.isDebugEnabled()) {
            __logger.debug(d1NodeReference.getValue() + "- starting retrieval " + nodeComm.getNodeRegistryService().getNode(d1NodeReference).getBaseURL()
                + " with startDate of " + DateTimeMarshaller.serializeDateToUTC(startHarvestDate)
                + " and endDate of " + DateTimeMarshaller.serializeDateToUTC(endHarvestInterval));
        }
        
        // Member Nodes are only required to support the 'fromDate' parameter in listObjects.
        // However, most implementations support 'count' and 'start' and 'toDate'
        // Because of varying implementations, multiple strategies for harvesting are needed.
        // 
        // If a parameter is not supported, the MemberNode is required to throw InvalidRequest
        // if that parameter is used in the request.  This method uses this fact to determine
        // the harvest strategy.
        
        //
        //   2. determine the strategy while also getting the total within the harvest time window
        //
        int total = -1;
        try {
            ObjectList ol = doListObjects(nodeComm,startHarvestDate,endHarvestInterval,0,0);
            total = ol.getTotal();
            __logger.info(d1NodeReference.getValue() + "- has " + total + " pids to harvest.");
            
            endHarvestInterval =  adjustFilterWindow(nodeComm,total,maxToHarvest,startHarvestDate, endHarvestInterval);
            __logger.info(d1NodeReference.getValue() + "- adjusting harvest toDate to limit the total for paged harvest [to between max and 2*max]");
            
            
                                
        } catch (InvalidRequest e) {
            // proceed assuming it's the start/count that's not implemented

            __logger.warn(d1NodeReference.getValue() + "- Node doesn't like slicing parameters, trying single-page harvest strategy: "  + e.serialize(BaseException.FMT_XML));
            
            //
            //  3a. use the all-in-one harvest strategy
            //
            return doAllInOneHarvest(nodeComm,startHarvestDate,endHarvestInterval, maxToHarvest);
            
            
        } catch (InvalidToken | NotAuthorized | NotImplemented | ServiceFailure e) {
            // if here, nothing was harvested so exit with exception after logging
            __logger.error(d1NodeReference.getValue() + "- " + e.serialize(BaseException.FMT_XML));
        }
   
        //
        // 3b.  use the paged-harvest strategy
        //
        return doPagedHarvest(nodeComm,startHarvestDate, endHarvestInterval, total, maxToHarvest);
    }
 
    
    
    
 
    /**
     * This method should optimize the time window size so that the total within group is not 
     * more than the maximum synchronization will take.  This will reduce load on the MNs in that
     * they will not need to sort more objects than needed (assuming O(NlogN)).
     */
    protected Date adjustFilterWindow(NodeComm nc, int total, int max, Date fromDate, Date toDate) 
            throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure {
        
        
        if (total < max || total < 200) {
            // the initial window is good OR not worth trying to optimize for small totals.
            return toDate;
        }
        
        long adjustedToDate = toDate.getTime();
        long fromDateValue = fromDate.getTime();
        boolean incomplete = false;
        
        int i = 0;
        long deltaT = adjustedToDate - fromDateValue;
        while (total > max * 2 || total < max) {
            deltaT /= 2;
            if (total < max) {
                adjustedToDate = adjustedToDate + deltaT;
            } else {
                adjustedToDate = adjustedToDate - deltaT;
            }
            
            if (__logger.isDebugEnabled()) {
                __logger.debug(String.format("%d. total = %d, max = %d: adjustingFilterWindow: [%s to %s]",i, total, max, fromDate,new Date(adjustedToDate)));
            }
            
            total = doListObjects(nc, fromDate, new Date(adjustedToDate),0,0).getTotal();     
            
            // avoid infinite optimizations for tightly clustered modification dates 
            if (i++ > 25) {
                incomplete = true;
                break;
            }
        }
        if (incomplete && total == 0) {
            return toDate;
        }
        __logger.info(d1NodeReference.getValue() + String.format(" - final time window: total = %d, max = %d: adjustingFilterWindow: [%s to %s]", total, max, fromDate,new Date(adjustedToDate)));

        
        return new Date(adjustedToDate);
    }
 
    
    
    
    
    /**
     * Assemble the timepoint-pid map from the ObjectList iteratively, using paging
     * @param nodeComm
     * @param start
     * @param end
     * @param total
     * @return
     * @throws ServiceFailure 
     * @throws NotImplemented 
     * @throws NotAuthorized 
     * @throws InvalidToken 
     * @throws InvalidRequest 
     * @throws ExecutionDisabledException 
     */
    private SortedHarvestTimepointMap doPagedHarvest(NodeComm nodeComm, Date fromDate, Date toDate, Integer total, Integer maxToHarvest) 
            throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, ExecutionDisabledException {

        SortedHarvestTimepointMap harvest = new SortedHarvestTimepointMap(fromDate,toDate,maxToHarvest);
        
        int currentStart = 0;
        int latestReportedTotal = total;
        int count = Math.min(this.batchSize,  latestReportedTotal - currentStart);
        while (currentStart < latestReportedTotal) {
            
            
            if (!ComponentActivationUtility.synchronizationIsActive()) {
                throw  new ExecutionDisabledException(d1NodeReference.getValue() + "- Disabled");
            }
            
            ObjectList ol = doListObjects(nodeComm, fromDate, toDate, currentStart, count);
            
            if (ol.sizeObjectInfoList() == 0) {
                break;
            }
            __syncMetricTotalRetrieved += ol.sizeObjectInfoList();
            
            currentStart += ol.sizeObjectInfoList();
            latestReportedTotal = ol.getTotal();
            
            harvest.addObjectList(ol);
        }        
        return harvest;
    }
  
    
    /**
     * The all in one strategy is basically asserting that everything requested must be returned
     * in the response because there is no other way to know we have all of them and advance the
     * dateLastHarvested value.
     * 
     * @param nodeComm
     * @param start
     * @param end
     * @return
     * @throws InvalidRequest
     * @throws InvalidToken
     * @throws NotAuthorized
     * @throws NotImplemented
     * @throws ServiceFailure
     */
    private SortedHarvestTimepointMap doAllInOneHarvest(NodeComm nodeComm, Date fromDate, Date toDate, Integer maxToHarvest) 
            throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure {
             
        
        // 1. try to get the entire list of items matching the date criteria
        
        ObjectList ol = null;
        try {
            ol = doListObjects(nodeComm, fromDate, toDate, null, null);
        } 
        catch (InvalidRequest e) {
            // since 'start' and 'count' parameters are null, proceed assuming it's now only the fromDate that works
            
            __logger.warn(d1NodeReference.getValue() + "- Node doesn't like toDate parameters, trying basic strategy " + e.serialize(BaseException.FMT_XML));
            
            ol = doListObjects(nodeComm, fromDate, null, null, null);
            __syncMetricTotalRetrieved += ol.sizeObjectInfoList();
        }
        
        
        // 2. check that listObjects returned the entire list for the time interval
        
        if (ol.getTotal() != ol.sizeObjectInfoList()) {
            
            __logger.error(d1NodeReference.getValue() + " - MemberNode does not support paging, " +
                    "but also doesn't return the total list within the harvest period.  Cannot harvest!");
        
            throw new InvalidRequest("0000-MNode Failure","Cannot reliably harvest from this MemberNode:  " +
            		"It does not support paging, yet does not return all of the object infos for the time period requested!");
        }
         
        // 3. add the objectList to the harvest
        
        SortedHarvestTimepointMap harvest = new SortedHarvestTimepointMap(fromDate,toDate,maxToHarvest);
        harvest.addObjectList(ol);
        
       
        return harvest;
    }
    
    private ObjectList doListObjects(NodeComm nodeComm, Date fromDate, Date toDate, Integer start, Integer count) 
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure {
        

        Object mnRead = nodeComm.getMnRead();
        if (mnRead instanceof MNRead) {
            return ((MNRead) mnRead).listObjects(session, fromDate, toDate, null, null, null, start, count);
        }
        if (mnRead instanceof org.dataone.service.mn.tier1.v1.MNRead) {
            return  ((org.dataone.service.mn.tier1.v1.MNRead) mnRead).listObjects(session, fromDate, toDate, null, null, start, count);
        }
        throw new ServiceFailure("0000", this.d1NodeReference.getValue() + 
                " - the NodeComm.getMNRead() did not return a usable instance.  Got instance of " + mnRead.getClass().getCanonicalName());
    }
 
}
