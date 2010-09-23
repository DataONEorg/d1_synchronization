/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.harvest;

import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.mn.MemberNodeCrud;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.NodeReference;
import org.dataone.service.types.ObjectInfo;
import org.dataone.service.types.SystemMetadata;
import org.dataone.service.types.SystemMetadata.Replica;
import org.dataone.service.types.SystemMetadata.Replica.ReplicationStatus;

/**
 *
 * @author rwaltz
 */
public class ObjectListQueueProcessor {

    /**
     *
     *
    1) testCreateData() & testCreateScienceMetadata() do not use the setAccess method.  But, testGet() uses it. How does this work?

    they don't need setAccess because they use the authenticated sessionid to do the testing

    if you wanted to pull the created docs as user public, you'd have to use the setAccess() method
    like in testGet()

    2) if objects are created as user public, then you don't have to use setAccess to get?
    you can't create as user public
    you must be logged in


    3) Do all records have access set as public when they are created, regardless of the user?

    no, if you create something as uid=kepler, then by default only uid=kepler can read it
    so you have to setAccess() to public for the "public" user to be able to read it
    the public user isn't really a user at all.  just a sessionid of 0 or null


    Executive summary:
    1) only logged in users can create
    2) by default, only that logged in user can read the created doc
    (including with listObjects)
    3) you must setAccess to public if you want anonymous users to be able to read the doc

    so you see in testGet(),
    I create the doc as user "kepler"
    then I can read it with the kepler token
    but as I setAccess to public read
    I can use the publicToken to read it
     *
     *
     */
    Logger logger = Logger.getLogger(ObjectListQueueProcessor.class.getName());
    private MemberNodeCrud mnReader;
    private List<ObjectInfo> readQueue;
    private Map<Identifier, SystemMetadata> writeQueue;
    private AuthToken token;
    private String mnIdentifier; // This is only a temporary solution
//    public void processQueue(Node node) { XXX future call i think, because we will be processing this for specific nodes
    public void processQueue() {
        boolean hasException;
        for (ObjectInfo objectInfo : readQueue) {
            hasException = true;
            try {
    //            sciMetaFile = this.writeScienceMetadataToFile(objectInfo);
                logger.debug("Retrieve SystemMetadata for " + objectInfo.getIdentifier().getValue());
                SystemMetadata systemMetadata = mnReader.getSystemMetadata(null, objectInfo.getIdentifier());
                logger.debug("Found SystemMetadata " + systemMetadata.getIdentifier().getValue() +  " for Identifier " + objectInfo.getIdentifier().getValue());
                NodeReference nodeReference = new NodeReference();

                nodeReference.setValue(mnIdentifier); //XXX get this from the identifier of the node that is being synchronized
                Replica originalReplica = new Replica();
                originalReplica.setReplicaMemberNode(nodeReference);
                originalReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
                originalReplica.setReplicaVerified(new Date());
                systemMetadata.addReplica(originalReplica);

                //
                // XXX  do we really want
                // to add in the originating node as a Replica,
                // or just leave it as the authoritative node
                //
                systemMetadata.setOriginMemberNode(nodeReference);
                writeQueue.put( objectInfo.getIdentifier(), systemMetadata);
                hasException = false;
            } catch (InvalidToken ex) {
                logger.error(ex.serialize(ex.FMT_XML));
            } catch (ServiceFailure ex) {
                logger.error(ex.serialize(ex.FMT_XML));
            } catch (NotAuthorized ex) {
                logger.error(ex.serialize(ex.FMT_XML));
            } catch (NotFound ex) {
                logger.error(ex.serialize(ex.FMT_XML));
            } catch (NotImplemented ex) {
                logger.error(ex.serialize(ex.FMT_XML));
            } catch (InvalidRequest ex) {
                logger.error(ex.serialize(ex.FMT_XML));
            }
        }
        readQueue.clear();
    }

    public MemberNodeCrud getMnReader() {
        return mnReader;
    }

    public void setMnReader(MemberNodeCrud mnReader) {
        this.mnReader = mnReader;
    }

    public Map<Identifier, SystemMetadata> getWriteQueue() {
        return writeQueue;
    }

    public void setWriteQueue(Map<Identifier, SystemMetadata> writeQueue) {
        this.writeQueue = writeQueue;
    }

    public List<ObjectInfo> getReadQueue() {
        return readQueue;
    }

    public void setReadQueue(List<ObjectInfo> readQueue) {
        this.readQueue = readQueue;
    }

    public AuthToken getToken() {
        return token;
    }

    public void setToken(AuthToken token) {
        this.token = token;
    }

    public String getMnIdentifier() {
        return mnIdentifier;
    }

    public void setMnIdentifier(String mnIdentifier) {
        this.mnIdentifier = mnIdentifier;
    }

}
