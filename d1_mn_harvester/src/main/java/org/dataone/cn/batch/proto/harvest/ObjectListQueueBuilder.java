/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.harvest;

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.log4j.*;
import org.dataone.cn.batch.proto.harvest.persist.NodeMapPersistence;

import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.mn.MemberNodeReplication;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.ObjectInfo;
import org.dataone.service.types.ObjectList;
import org.dataone.service.types.util.ServiceTypeUtil;

/**
 *
 * @author rwaltz
 */
public class ObjectListQueueBuilder {

    Logger logger = Logger.getLogger(ObjectListQueueBuilder.class.getName());
    private MemberNodeReplication mnReplication;
    private List<ObjectInfo> writeQueue;
    private String mnIdentifier;
    private Integer objectRetrievalCount = 1000;
    private AuthToken token;
    private NodeMapPersistence nodeMapPersistance;
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
        ObjectList objectList = null;
        Boolean replicationStatus = null;
        Date now = new Date();
        Date lastHarvestDate;
        Map<String, Date> nodeMap = nodeMapPersistance.getPersistMapping().getMap();
        if (nodeMap.containsKey(mnIdentifier)) {
            lastHarvestDate = nodeMap.get(mnIdentifier);
        } else {
            lastHarvestDate = ServiceTypeUtil.deserializeDateToUTC("1900-01-01T00:00:00.000"); // Mon, 01 Jan 1900 00:00:00 GMT
            nodeMap.put(mnIdentifier, lastHarvestDate);
        }
        try {
            do {
                objectList = mnReplication.listObjects(token, lastHarvestDate, now, null, replicationStatus, start, objectRetrievalCount);
                if ((objectList == null) ||
                        (objectList.getCount() == 0) ||
                        (objectList.getObjectInfoList().isEmpty()) ) {
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

    public MemberNodeReplication getMnReplication() {
        return mnReplication;
    }

    public void setMnReplication(MemberNodeReplication mnReplication) {
        this.mnReplication = mnReplication;
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

    public int getObjectRetrievalCount() {
        return objectRetrievalCount;
    }

    public void setObjectRetrievalCount(int objectRetrievalCount) {
        this.objectRetrievalCount = objectRetrievalCount;
    }

    public NodeMapPersistence getNodeMapPersistance() {
        return nodeMapPersistance;
    }

    public void setNodeMapPersistance(NodeMapPersistence nodeMapPersistance) {
        this.nodeMapPersistance = nodeMapPersistance;
    }

    public String getMnIdentifier() {
        return mnIdentifier;
    }

    public void setMnIdentifier(String mnIdentifier) {
        this.mnIdentifier = mnIdentifier;
    }
}
