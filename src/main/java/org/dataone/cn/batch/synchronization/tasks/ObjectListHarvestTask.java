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

package org.dataone.cn.batch.synchronization.tasks;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.synchronization.NodeCommD1ClientFactory;
import org.dataone.cn.batch.synchronization.jobs.MemberNodeHarvestJob;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.batch.synchronization.type.SyncObject;
import org.dataone.cn.ldap.NodeAccess;
import org.dataone.service.cn.impl.v1.NodeRegistryService;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.mn.tier1.v1.MNRead;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectInfo;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.Session;
import org.dataone.service.util.DateTimeMarshaller;
import org.joda.time.DateTime;
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

    @Override
    public Date call() throws Exception {
        // we are going to write directly to ldap for the updateLastHarvested
        // because we do not want hazelcast to spam us about
        // all of these updates since we have a listener in HarvestSchedulingManager
        // that determines when updates/additions have occured and 
        // re-adjusts scheduling
        NodeRegistryService nodeRegistryService = new NodeRegistryService();
        NodeAccess nodeAccess = new NodeAccess();
        // logger is not  be serializable, but no need to make it transient imo
        Logger logger = Logger.getLogger(ObjectListHarvestTask.class.getName());
        logger.info(d1NodeReference +"- ObjectListHarvestTask Start");
        HazelcastInstance hazelcast = Hazelcast.getDefaultInstance();
        BlockingQueue<SyncObject> hzSyncObjectQueue = hazelcast.getQueue("hzSyncObjectQueue");
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
        // make certain it is still after the lastModified date
        if (startHarvestDate.before(now)) {
            // if not then do not run (we should be running this less than every ten seconds for a membernode
            logger.debug(d1NodeReference +"- starting retrieval " + d1Node.getBaseURL() + " with startDate of " + DateTimeMarshaller.serializeDateToUTC(startHarvestDate) + " and endDate of " + DateTimeMarshaller.serializeDateToUTC(now));
            do {
                // read upto a 1000 objects (the default, but it can be overwritten)
                // from ListObjects and process before retrieving more
                if (start == 0 || (start < total)) {
                    readQueue = this.retrieve(d1Node, startHarvestDate, now);

                    for (ObjectInfo objectInfo : readQueue) {
                        SyncObject syncObject = new SyncObject(d1Node.getIdentifier().getValue(), objectInfo.getIdentifier().getValue());

                        if (objectInfo.getDateSysMetadataModified().after(lastMofidiedDate)) {
                            lastMofidiedDate = objectInfo.getDateSysMetadataModified();
                        }
                        hzSyncObjectQueue.put(syncObject);
                        logger.debug(d1NodeReference +"- syncTask " + syncObject.getPid() + " placed on Queue");
                    }
                } else {
                    readQueue = null;
                }
            } while ((readQueue != null) && (!readQueue.isEmpty()));

            if (lastMofidiedDate.after(d1Node.getSynchronization().getLastHarvested())) {
                // use nodeAccess directly to avoid hazelcast broadcasting the event
                nodeAccess.setDateLastHarvested(d1NodeReference, lastMofidiedDate);
            }
        } else {
            logger.warn(d1NodeReference +"- Difference between Node's LastHarvested Date and Current Date time was less than 10 seconds");
        }
        logger.info(d1NodeReference + "- ObjectListHarvestTask End");
        // return the date of completion of the task
        return new Date();
    }

    /*
     * performs the retrieval of the nodelist from a membernode. It retrieves the list in batches and should be called
     * iteratively until all objects have been retrieved from a node.
     */
    private List<ObjectInfo> retrieve(Node d1Node, Date fromDate, Date toDate ) {
        // logger is not  be serializable, but no need to make it transient imo
        Logger logger = Logger.getLogger(ObjectListHarvestTask.class.getName());

        NodeCommD1ClientFactory nodeCommClientFactory = new NodeCommD1ClientFactory();

        List<ObjectInfo> writeQueue = new ArrayList<ObjectInfo>();


        ObjectList objectList = null;
        Boolean replicationStatus = null;
        
        try {
            NodeComm nodeComm = nodeCommClientFactory.getNodeComm(d1Node.getBaseURL());
            MNRead mnRead = nodeComm.getMnRead();
            // always execute for the first run (for start = 0)
            // otherwise skip because when the start is equal or greater
            // then total, then all objects have been harvested

            objectList = mnRead.listObjects(session, fromDate, toDate, null, replicationStatus, start, batchSize);
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
            logger.error(d1NodeReference +"- " + ex.serialize(ex.FMT_XML));
        } catch (InvalidRequest ex) {
            logger.error(d1NodeReference +"- " + ex.serialize(ex.FMT_XML));
        } catch (NotImplemented ex) {
            logger.error(d1NodeReference +"- " + ex.serialize(ex.FMT_XML));
        } catch (ServiceFailure ex) {
            logger.error(d1NodeReference +"- " + ex.serialize(ex.FMT_XML));
        } catch (InvalidToken ex) {
            logger.error(d1NodeReference +"- " + ex.serialize(ex.FMT_XML));
        }

        return writeQueue;
    }
}
