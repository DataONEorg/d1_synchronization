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

import com.hazelcast.core.HazelcastInstance;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.exceptions.ExecutionDisabledException;
import org.dataone.cn.batch.synchronization.NodeCommFactory;
import org.dataone.cn.batch.synchronization.NodeCommObjectListHarvestFactory;
import org.dataone.cn.batch.synchronization.jobs.MemberNodeHarvestJob;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.batch.synchronization.type.SyncObject;
import org.dataone.cn.hazelcast.HazelcastInstanceFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.impl.v2.NodeRegistryService;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.mn.tier1.v2.MNRead;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectInfo;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.Session;
import org.dataone.service.util.DateTimeMarshaller;
import org.joda.time.MutableDateTime;

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
    private int start = 0;
    private int total = 0;
    Integer batchSize;
    MutableDateTime currentDateTime = new MutableDateTime(new Date());
    Date now;
    Log logger = LogFactory.getLog(MemberNodeHarvestJob.class);

    public ObjectListHarvestTask(NodeReference d1NodeReference, Integer batchSize) {
        this.d1NodeReference = d1NodeReference;
        this.batchSize = batchSize;
    }

    /**
     *
     * Method to be called in a separately executing thread.
     *
     * @return Date
     * @throws Exception
     */
    @Override
    public Date call() throws Exception {
        String synchronizationObjectQueue = Settings.getConfiguration().getString("dataone.hazelcast.synchronizationObjectQueue");
        boolean activateJob = Boolean.parseBoolean(Settings.getConfiguration().getString("Synchronization.active"));
        Integer maxSyncObjectQueueSize = Settings.getConfiguration().getInt("Synchronization.max_syncobjectqueue_size");
        if (!activateJob) {
            ExecutionDisabledException ex = new ExecutionDisabledException(d1NodeReference.getValue() + "- Disabled");
            throw ex;
        }
        logger.info(d1NodeReference.getValue() + "- Calling ObjectListHarvestTask");
        // we are going to write directly to ldap for the updateLastHarvested
        // because we do not want hazelcast to spam us about
        // all of these updates since we have a listener in HarvestSchedulingManager
        // that determines when updates/additions have occured and 
        // re-adjusts scheduling
        
        // Assuming that only one MN harvesting job is executing at at time
        // therefore the only one mnNodeComm per MemberNode is needed for all the runs of
        // the harvester
        NodeCommFactory nodeCommFactory = NodeCommObjectListHarvestFactory.getInstance();
        NodeComm mnNodeComm = nodeCommFactory.getNodeComm(d1NodeReference);
        NodeRegistryService nodeRegistryService = mnNodeComm.getNodeRegistryService();
        // logger is not  be serializable, but no need to make it transient imo
        Logger logger = Logger.getLogger(ObjectListHarvestTask.class.getName());
        HazelcastInstance hazelcast = HazelcastInstanceFactory.getProcessingInstance();
        BlockingQueue<SyncObject> hzSyncObjectQueue = hazelcast.getQueue(synchronizationObjectQueue);
        // Need the LinkedHashMap to preserver insertion order
        Node d1Node = nodeRegistryService.getNode(d1NodeReference);
        Date lastMofidiedDate = d1Node.getSynchronization().getLastHarvested();
        MutableDateTime startHarvestDateTime = new MutableDateTime(lastMofidiedDate);

        startHarvestDateTime.addMillis(1);
        Date startHarvestDate = startHarvestDateTime.toDate();

        List<ObjectInfo> readQueue = null;
        // subtract 10 seconds from the current date time 
        currentDateTime.addSeconds(-10);
        now = currentDateTime.toDate();
        if ((hzSyncObjectQueue.size() + batchSize) > maxSyncObjectQueueSize) {
            // if we don't have much capacity to process a single batch then 
            // make a smaller batch size for now
            batchSize = batchSize/2;
        }
        // make certain it is still after the lastModified date
        if (startHarvestDate.before(now)) {
            // if not then do not run (we should be running this less than every ten seconds for a membernode
            logger.debug(d1NodeReference.getValue() + "- starting retrieval " + d1Node.getBaseURL() + " with startDate of " + DateTimeMarshaller.serializeDateToUTC(startHarvestDate) + " and endDate of " + DateTimeMarshaller.serializeDateToUTC(now));
            do {
                activateJob = Boolean.parseBoolean(Settings.getConfiguration().getString("Synchronization.active"));
                if (!activateJob) {
                    // throwing this exception will not update
                    // last harvested date on the node, causing
                    // a reharvesting of all records
                    // if the DateLastHarvested was modified on the node
                    // to be the most recent lastMofidiedDate, then
                    // it is likely on the next run, records will be missed
                    // since nodeList is not an ordered list
                    ExecutionDisabledException ex = new ExecutionDisabledException(d1NodeReference.getValue() + "- Disabled");
                    throw ex;
                }
                // read upto a 1000 objects (the default, but it can be overwritten)
                // from ListObjects and process before retrieving more
                if (start == 0 || (start < total)) {
                    readQueue = this.retrieve(mnNodeComm, startHarvestDate, now);
                    int loopCount = 0;
                    while (((hzSyncObjectQueue.size() + readQueue.size()) > maxSyncObjectQueueSize) && (loopCount < 1440)) {
                        activateJob = Boolean.parseBoolean(Settings.getConfiguration().getString("Synchronization.active"));
                        if (!activateJob) {
                            // throwing this exception will not update
                            // last harvested date on the node, causing
                            // a reharvesting of all records
                            // if the DateLastHarvested was modified on the node
                            // to be the most recent lastMofidiedDate, then
                            // it is likely on the next run, records will be missed
                            // since nodeList is not an ordered list
                            ExecutionDisabledException ex = new ExecutionDisabledException(d1NodeReference.getValue() + "- Disabled");
                            throw ex;
                        }
                         logger.debug("Sleeping for 5 secs. hzSyncObjectQueue has " + hzSyncObjectQueue.size() + " # of objects.");
                         Thread.sleep(5000L);

                        ++loopCount;
                    }
                    if (loopCount >= 1440) {
                        //Sleep is 5 secs, looping 1440 should be 2 hrs of waiting..
                        // I'd say something has gone horribly wrong and so dying would be
                        // appropriate
                        nodeRegistryService.getNodeAccess().setDateLastHarvested(d1NodeReference, lastMofidiedDate);
                        throw new Exception("hzSyncObjectQueue has not had more than " + hzSyncObjectQueue.remainingCapacity() + " remaining capacity for 2 hrs.");
                    }
                    for (ObjectInfo objectInfo : readQueue) {
                        SyncObject syncObject = new SyncObject(d1Node.getIdentifier().getValue(), objectInfo.getIdentifier().getValue());

                        if ((objectInfo.getDateSysMetadataModified().after(lastMofidiedDate)) && !(objectInfo.getDateSysMetadataModified().after(now))) {
                            // increase the lastModifiedDate if the current record's modified date
                            // is after the date currently specified.
                            // However, if the date returned is past now, then an update must have
                            // occurred between the time query was returned and this statement is processed
                            lastMofidiedDate = objectInfo.getDateSysMetadataModified();
                        }
                        // process the unexpected update, the next time synchronization is run
                        if (!objectInfo.getDateSysMetadataModified().after(now))
                                {
                                hzSyncObjectQueue.put(syncObject);
                                logger.debug("placed on hzSyncObjectQueue- " +syncObject.getNodeId() + ":"+ syncObject.getPid());
                                }
                    }
                } else {
                    readQueue = null;
                }
            } while ((readQueue != null) && (!readQueue.isEmpty()));

            if (lastMofidiedDate.after(d1Node.getSynchronization().getLastHarvested())) {
                // use nodeAccess directly to avoid hazelcast broadcasting the event
                nodeRegistryService.getNodeAccess().setDateLastHarvested(d1NodeReference, lastMofidiedDate);
            }
        } else {
            logger.warn(d1NodeReference.getValue() + "- Difference between Node's LastHarvested Date and Current Date time was less than 10 seconds");
        }
        logger.info(d1NodeReference.getValue() + "- ObjectListHarvestTask End");
        // return the date of completion of the task
        return new Date();
    }

    /*
     * performs the retrieval of the nodelist from a membernode. It retrieves the list in batches and should be called
     * iteratively until all objects have been retrieved from a node.
     * 
     * @param Node d1Node
     * @param Date fromDate
     * @param Date toDate
     * @return List<ObjectInfo>
     */
    private List<ObjectInfo> retrieve(NodeComm nodeComm, Date fromDate, Date toDate) {
        // logger is not  be serializable, but no need to make it transient imo
        Logger logger = Logger.getLogger(ObjectListHarvestTask.class.getName());

        List<ObjectInfo> writeQueue = new ArrayList<ObjectInfo>();

        ObjectList objectList = null;
        Boolean replicationStatus = null;

        try {
            Object mnRead = nodeComm.getMnRead();
            if (mnRead instanceof MNRead) {
                objectList = ((MNRead) mnRead).listObjects(session, fromDate, toDate, null, null, replicationStatus, start, batchSize);
            }
            if (mnRead instanceof org.dataone.service.mn.tier1.v1.MNRead) {
                objectList = ((org.dataone.service.mn.tier1.v1.MNRead) mnRead).listObjects(session, fromDate, toDate, null, replicationStatus, start, batchSize);
            }
            // always execute for the first run (for start = 0)
            // otherwise skip because when the start is equal or greater
            // then total, then all objects have been harvested

            // if objectList is null or the count is 0 or the list is empty, then
            // there is nothing to process
            if (!((objectList == null)
                    || (objectList.getCount() == 0)
                    || (objectList.getObjectInfoList().isEmpty()))) {

                start += objectList.getCount();
                writeQueue.addAll(objectList.getObjectInfoList());
                total = objectList.getTotal();

            }
        } catch (NotAuthorized ex) {
            logger.error(d1NodeReference.getValue() + "- " + ex.serialize(ex.FMT_XML));
        } catch (InvalidRequest ex) {
            logger.error(d1NodeReference.getValue() + "- " + ex.serialize(ex.FMT_XML));
        } catch (NotImplemented ex) {
            logger.error(d1NodeReference.getValue() + "- " + ex.serialize(ex.FMT_XML));
        } catch (ServiceFailure ex) {
            logger.error(d1NodeReference.getValue() + "- " + ex.serialize(ex.FMT_XML));
        } catch (InvalidToken ex) {
            logger.error(d1NodeReference.getValue() + "- " + ex.serialize(ex.FMT_XML));
        } 

        return writeQueue;
    }
}
