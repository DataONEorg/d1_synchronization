/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.harvest;

import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.log4j.*;
import org.dataone.client.MNode;
import org.dataone.cn.batch.utils.NodeReference;

import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Node;
import org.dataone.service.types.ObjectInfo;
import org.dataone.service.types.ObjectList;

/**
 *
 * @author rwaltz
 */
public class ObjectListQueueBuilder {

    Logger logger = Logger.getLogger(ObjectListQueueBuilder.class.getName());
    private MNode mnReader;
    private List<ObjectInfo> writeQueue;
    private NodeReference nodeReferenceUtility;
    private Integer objectRetrievalCount = 1000;
    AuthToken token;
    static final Comparator<ObjectInfo> LAST_MOFIDIED_ORDER =
            new Comparator<ObjectInfo>() {

                public int compare(ObjectInfo o1, ObjectInfo o2) {
                    Long o1Time = o1.getDateSysMetadataModified().getTime();
                    Long o2Time = o2.getDateSysMetadataModified().getTime();
                    return o1Time.compareTo(o2Time);
                }
            };

    public void buildQueue() {
        Integer start = 0;
        Date startTime;
        Node mnNode = nodeReferenceUtility.getMnNode();
        ObjectList objectList = null;
        Boolean replicationStatus = null;
        Date now = new Date();
        Date lastHarvestDate = mnNode.getSynchronization().getLastHarvested();
        try {
            do {
                objectList = mnReader.listObjects(token, lastHarvestDate, now, null, replicationStatus, start, objectRetrievalCount);
                if (objectList == null || objectList.getTotal() == 0) {
                    break;
                }
                start += objectList.getCount();
                writeQueue.addAll(objectList.getObjectInfoList());
            } while (start < objectList.getTotal());
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
        // TODO get sorting sorted out
        // Sorting the collection is very important to ensure processing of all
        // but do to time constraints, this will have to be re-added in
        // and refactored later. SpringBatch may be the way to go in the
        // next iteration
//        Collections.sort(writeQueue, LAST_MOFIDIED_ORDER);
    }

    public MNode getMnReader() {
        return mnReader;
    }

    public void setMnReader(MNode mnReader) {
        this.mnReader = mnReader;
    }

    public List<ObjectInfo> getWriteQueue() {
        return writeQueue;
    }

    public void setWriteQueue(List<ObjectInfo> writeQueue) {
        this.writeQueue = writeQueue;
    }

    public AuthToken getToken() {
        return token;
    }

    public void setToken(AuthToken token) {
        this.token = token;
    }

    public NodeReference getNodeReferenceUtility() {
        return nodeReferenceUtility;
    }

    public void setNodeReferenceUtility(NodeReference nodeReferenceUtility) {
        this.nodeReferenceUtility = nodeReferenceUtility;
    }

    public int getObjectRetrievalCount() {
        return objectRetrievalCount;
    }

    public void setObjectRetrievalCount(int objectRetrievalCount) {
        this.objectRetrievalCount = objectRetrievalCount;
    }
}
