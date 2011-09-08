/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.synchronization.tasks;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Instance;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.synchronization.NodeCommD1ClientFactory;
import org.dataone.cn.batch.synchronization.NodeCommFactory;
import org.dataone.cn.batch.type.NodeComm;
import org.dataone.cn.batch.type.SimpleNode;
import org.dataone.cn.batch.type.SyncObject;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.mn.tier1.v1.MNRead;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectInfo;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;

/**
 * An executable task that retrieve a list of ObjectInfos
 * by calling listObject on a MN  and then submits them on
 * the SyncTaskQueue for processing. It will retrieve and submit
 * in batches of 1000 as the default.
 *
 * As an executable, it will return a date that is the latest DateSysMetadataModified
 * of the processed nodelist
 * 
 * @author waltz
 */
public class ObjectListHarvestTask implements Callable<Date>, Serializable {

    SimpleNode simpleNode;
    private Session session;
    private int start = 0;
    private int total = 0;
    Integer batchSize;
    private Date now = new Date();

    public ObjectListHarvestTask(SimpleNode simpleNode, Integer batchSize) {
        this.simpleNode = simpleNode;
        this.batchSize = batchSize;
    }

    @Override
    public Date call() throws Exception {
        // logger is not  be serializable, but no need to make it transient imo
        Logger logger = Logger.getLogger(ObjectListHarvestTask.class.getName());
        logger.debug("called ObjectListHarvestTask");
        HazelcastInstance hazelcast = Hazelcast.getDefaultInstance();
        BlockingQueue<SyncObject> syncTaskQueue = hazelcast.getQueue("syncTaskQueue");
        // Need the LinkedHashMap to preserver insertion order
        Date lastMofidiedDate = simpleNode.getLastHarvested();
        List<ObjectInfo> readQueue = null;

        do {
            // read upto a 1000 objects (the default, but it can be overwritten)
            // from ListObjects and process before retrieving more
            if (start == 0 || (start < total)) {
                readQueue = this.retrieve(this.simpleNode);

                for (ObjectInfo objectInfo : readQueue) {
                    SyncObject syncObject = new SyncObject(simpleNode.getNodeId(), objectInfo.getIdentifier().getValue());

                    if (objectInfo.getDateSysMetadataModified().after(lastMofidiedDate)) {
                        lastMofidiedDate = objectInfo.getDateSysMetadataModified();
                    }
                    syncTaskQueue.put(syncObject);
                    logger.debug("syncTask " + syncObject.getPid() + " placed on Queue");
                }
            } else {
                readQueue = null;
            }
        } while ((readQueue != null) && (!readQueue.isEmpty()));


        return lastMofidiedDate;
    }

    /*
     * performs the retrieval of the nodelist from a membernode.
     * It retrieves the list in batches and should be called iteratively
     * until all objects have been retrieved from a node.
     */
    private List<ObjectInfo> retrieve(SimpleNode mnNode) {
        // logger is not  be serializable, but no need to make it transient imo
        Logger logger = Logger.getLogger(ObjectListHarvestTask.class.getName());

        NodeCommD1ClientFactory nodeCommClientFactory = new NodeCommD1ClientFactory();
        NodeComm nodeComm = nodeCommClientFactory.getNodeComm(simpleNode.getBaseUrl());
        MNRead mnRead = nodeComm.getMnRead();

        List<ObjectInfo> writeQueue = new ArrayList<ObjectInfo>();

        Date startTime;
        ObjectList objectList = null;
        Boolean replicationStatus = null;

        Date lastHarvestDate = mnNode.getLastHarvested();
        logger.debug("starting retrieval " + mnNode.getBaseUrl());
        try {

            // always execute for the first run (for start = 0)
            // otherwise skip because when the start is equal or greater
            // then total, then all objects have been harvested

            objectList = mnRead.listObjects(session, lastHarvestDate, now, null, replicationStatus, start, batchSize);
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
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (InvalidRequest ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (NotImplemented ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (ServiceFailure ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (InvalidToken ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        }

        return writeQueue;
    }
}
