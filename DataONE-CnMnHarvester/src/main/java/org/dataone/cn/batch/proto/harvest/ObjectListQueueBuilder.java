/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.harvest;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.log4j.*;
import org.dataone.cn.batch.utils.NodeReference;

import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.mn.MemberNodeReplication;
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
    private MemberNodeReplication mnReader;
    private List<ObjectInfo> writeQueue;
    private NodeReference nodeReferenceUtility;
    AuthToken token;
    
    static final Comparator<ObjectInfo> LAST_MOFIDIED_ORDER =
                                 new Comparator<ObjectInfo>() {
        public int compare(ObjectInfo o1, ObjectInfo o2) {
            return o1.getDateSysMetadataModified().compareTo(o2.getDateSysMetadataModified());
        }
    };

    public void buildQueue() {
        int start = 0;
        int count = 10;
        Date startTime;
        Node mnNode = nodeReferenceUtility.getMnNode();
        ObjectList objectList = null;
        Date now = new Date();
        Date lastHarvestDate = mnNode.getSynchronization().getLastCompleteHarvest();
        try {
            do {
                objectList = mnReader.listObjects(token, lastHarvestDate, now, null, true, start, count);
                if (objectList == null) {
                    break;
                }
                start += objectList.getCount();
                writeQueue.addAll(objectList.getObjectInfoList());
            } while (start < objectList.getTotal());
        } catch (NotAuthorized ex) {
            logger.error( ex.serialize(ex.FMT_XML));
        } catch (InvalidRequest ex) {
           logger.error( ex.serialize(ex.FMT_XML));
        } catch (NotImplemented ex) {
            logger.error( ex.serialize(ex.FMT_XML));
        } catch (ServiceFailure ex) {
            logger.error( ex.serialize(ex.FMT_XML));
        } catch (InvalidToken ex) {
            logger.error( ex.serialize(ex.FMT_XML));
        }
        Collections.sort(writeQueue, LAST_MOFIDIED_ORDER);
    }

    public MemberNodeReplication getMnReader() {
        return mnReader;
    }

    public void setMnReader(MemberNodeReplication mnReader) {
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


}
