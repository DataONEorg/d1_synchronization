/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.synchronization.tasks;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.dataone.client.MNode;
import org.dataone.cn.batch.type.NodeCommState;
import org.dataone.cn.batch.type.NodeComm;
import org.dataone.cn.batch.type.SyncObject;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.SynchronizationFailed;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.jibx.runtime.JiBXException;

/**
 * Transfer an object from a MemberNode(MN) to a CoordinatingNode(CN).
 * Executes as a thread that is executed by the SyncObjectTask class
 *
 * This class will download SystemMetadata from a MN and
 * process the SystemMetadata. It will then take actions based
 * on the information in the SystemMetadata.
 *
 * If the SystemMetadata describes a new Science Metadata object, it
 * will download the object and perform a create
 *
 * If the SystemMetadata describes a new Science Data Object, it will
 * register itself with the CN leaving the Data on the MN
 *
 * If the SystemMetadata already exists on the CN, then based on the
 * state of the SystemMetadata it will either be ignored, updated, or
 * throw an exception.
 * 
 * @author waltz
 */
public class TransferObjectTask implements Callable<Void> {

    Logger logger = Logger.getLogger(TransferObjectTask.class.getName());
    private NodeComm nodeCommunications;
    private SyncObject task;
    private Session session = null;
    // need this task queue if a failure occurs on the CN such that the task will
    // need to be processed on a separate CN
    private HazelcastInstance hazelcast = Hazelcast.getDefaultInstance();
    String cnIdentifier = Settings.getConfiguration().getString("Synchronization.CN_REPLICA_NODE");
    String hzSystemMetaMapString = Settings.getConfiguration().getString("Synchronization.hzSystemMetaMap");
    IMap<Identifier, SystemMetadata> hzSystemMetaMap;
    Lock lock;

    public TransferObjectTask(NodeComm nodeCommunications, SyncObject task) {
        this.nodeCommunications = nodeCommunications;
        this.task = task;
        hzSystemMetaMap = nodeCommunications.getHzClient().getMap(hzSystemMetaMapString);
    }

    @Override
    public Void call() {
        
        Identifier lockPid = new Identifier();
        lockPid.setValue(task.getPid());
        boolean isLocked = false;
        try {
            // this will be from the hazelcast client running against metacat
            logger.debug("Task-" + task.getNodeId() + ":" + task.getPid() + " Locking task");
            long timeToWait = 1;
            
            lock = nodeCommunications.getHzClient().getLock(lockPid);
            isLocked = lock.tryLock(timeToWait, TimeUnit.SECONDS);
            if (isLocked) {
                logger.info("Task-" + task.getNodeId() + ":" + task.getPid() + " Processing task");
                SystemMetadata systemMetadata = process(task.getNodeId(), task.getPid());
                if (systemMetadata != null) {
                    logger.info("Task-" + task.getNodeId() + ":" + task.getPid() + " Writing task");
                    write(systemMetadata);
                } // else it was a failure and it should have been reported to MN so do nothing
            } else {
                try {
                    logger.warn("Task-" + task.getNodeId() + ":" + task.getPid() + "Pid Locked! Placing back on hzSyncObjectQueue");
                    hazelcast.getQueue("hzSyncObjectQueue").put(task);
                } catch (InterruptedException ex) {
                    logger.error("Unable to process pid " + task.getPid() + " from node " + task.getNodeId());
                    ServiceFailure serviceFailure = new ServiceFailure("564001", "Checksum does not match existing object with same pid");

                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.getMessage());
        }
        if (isLocked) {
            logger.info("Task-" + task.getNodeId() + ":" + task.getPid() + " Unlocking task");
            lock.unlock();
            logger.debug("Task-" + task.getNodeId() + ":" + task.getPid() + " Unlocked task");
        }
        return null;
    }

    private SystemMetadata process(String memberNodeId, String pid) {
        SystemMetadata systemMetadata = null;
        try {
            //            sciMetaFile = this.writeScienceMetadataToFile(objectInfo);
            Identifier identifier = new Identifier();
            identifier.setValue(pid);
            int tryAgain = 0;
            boolean needSystemMetadata = true;

            do {
                try {
                    systemMetadata = nodeCommunications.getMnRead().getSystemMetadata(null, identifier);
                    needSystemMetadata = false;
                } catch (NotAuthorized ex) {
                    if (tryAgain < 2) {
                        ++tryAgain;
                        logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
                        try {
                            Thread.sleep(5000L);
                        } catch (InterruptedException ex1) {
                            logger.warn("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex);
                        }
                    } else {
                        // only way to get out of loop if NotAuthorized keeps getting thrown
                        throw ex;
                    }
                }
            } while (needSystemMetadata);

            logger.info("Task-" + task.getNodeId() + ":" + task.getPid() + " Retrieved SystemMetadata Identifier:" + systemMetadata.getIdentifier().getValue() + " from node " + memberNodeId + " for ObjectInfo Identifier " + identifier.getValue());
            NodeReference nodeReference = new NodeReference();

            nodeReference.setValue(memberNodeId);
            logger.debug("Task-" + task.getNodeId() + ":" + task.getPid() + " Retrieved nodeReference set");
            List<Replica> replicaList = systemMetadata.getReplicaList();
            boolean addOriginalReplica = true;
            if (replicaList == null) {
                replicaList = new ArrayList<Replica>();
                systemMetadata.setReplicaList(replicaList);
            }
            if (!replicaList.isEmpty()) {
                for (Replica replica : replicaList) {
                    if (replica.getReplicaMemberNode().getValue().contentEquals(memberNodeId)) {
                        replica.setReplicationStatus(ReplicationStatus.COMPLETED);
                        replica.setReplicaVerified(new Date());
                        addOriginalReplica = false;
                    }
                }

            }
            if (addOriginalReplica) {
                Replica originalReplica = new Replica();
                NodeReference originalReplicaReference = new NodeReference();
                originalReplicaReference.setValue(memberNodeId);
                originalReplica.setReplicaMemberNode(originalReplicaReference);
                originalReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
                originalReplica.setReplicaVerified(new Date());
                logger.debug("Task-" + task.getNodeId() + ":" + task.getPid() + " Retrieved originalReplica");
                systemMetadata.addReplica(originalReplica);
            }
            logger.debug("Task-" + task.getNodeId() + ":" + task.getPid() + " Finished adding replica for original MN");
            try {
                // data objects are not fully synchronized, only their metadata is
                // synchronized,
                // only set valid science metadata formats as having been replicated
                logger.debug("Task-" + task.getNodeId() + ":" + task.getPid() + " Get Object Format");
                ObjectFormat objectFormat = nodeCommunications.getCnCore().getFormat(systemMetadata.getFormatId());
                if ((objectFormat != null) && !(objectFormat.getFormatType().equalsIgnoreCase("DATA"))) {
                    NodeReference cnReference = new NodeReference();
                    cnReference.setValue(cnIdentifier);
                    Replica cnReplica = new Replica();
                    cnReplica.setReplicaMemberNode(cnReference);
                    cnReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
                    cnReplica.setReplicaVerified(new Date());
                    systemMetadata.addReplica(cnReplica);
                    logger.debug("Task-" + task.getNodeId() + ":" + task.getPid() + " Added CN as replica because formatType " + objectFormat.getFormatType() + " is sciMetadata");
                }
            } catch (InsufficientResources ex) {
                try {
                    // maybe another
                    logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + ex.serialize(BaseException.FMT_XML));
                    hazelcast.getQueue("hzSyncObjectQueue").offer(task, 2, TimeUnit.SECONDS);
                } catch (Exception ex1) {
                    logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + " Unable to process pid " + pid + " from node " + memberNodeId);
                    ServiceFailure serviceFailure = new ServiceFailure("-1", ex1.getMessage());
                    submitSynchronizationFailed(pid, serviceFailure);
                }
                return null;
            }
            NodeReference originMemberNode = new NodeReference();
            originMemberNode.setValue(memberNodeId);
            systemMetadata.setOriginMemberNode(originMemberNode);

            NodeReference authoritativeMemberNode = new NodeReference();
            authoritativeMemberNode.setValue(memberNodeId);
            systemMetadata.setAuthoritativeMemberNode(authoritativeMemberNode);
        } catch (NotAuthorized ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(pid, ex);
            return null;
        } catch (InvalidToken ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(pid, ex);
            return null;
        } catch (ServiceFailure ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(pid, ex);
            return null;
        } catch (NotFound ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(pid, ex);
            return null;
        } catch (NotImplemented ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(pid, ex);
            return null;
        } catch (Exception ex) {
            ex.printStackTrace();

            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + " this didn't work", ex);
            ServiceFailure serviceFailure = new ServiceFailure("-1", ex.getMessage());
            submitSynchronizationFailed(pid, serviceFailure);
            return null;
        }
        return systemMetadata;
    }

    private void write(SystemMetadata systemMetadata) {
        // is this an update or create?

        try {


            logger.info("Task-" + task.getNodeId() + ":" + task.getPid() + " Getting sysMeta from CN");
            /*            try {
            existingChecksum = nodeCommunications.getCnRead().getChecksum(session, systemMetadata.getIdentifier());
            } catch (NotFound notFound) {
            // it is a create operation !
            logger.info("Task-" + task.getNodeId() + ":" + task.getPid() + " Create sysMeta");
            createObject(systemMetadata);
            return;
            }
             *
             */
            SystemMetadata cnSystemMetadata = null;
            try {
                cnSystemMetadata = hzSystemMetaMap.get(systemMetadata.getIdentifier());
            } catch (Exception ex) {
                // assume if hazelcast has thrown an exception SystemMetadata does not exist
                logger.info("Task-" + task.getNodeId() + ":" + task.getPid() + " Create sysMeta from Exception");
                createObject(systemMetadata);
                return;
            }
            // could hazelcast ever return a null instead? prepare for it
            if (cnSystemMetadata == null) {
                logger.info("Task-" + task.getNodeId() + ":" + task.getPid() + " Create sysMeta");
                createObject(systemMetadata);
            } else {
                Checksum existingChecksum = cnSystemMetadata.getChecksum(); // maybe an update, maybe duplicate, maybe a conflicting pid
                if (systemMetadata != null) {
                    Checksum newChecksum = systemMetadata.getChecksum();
                    if (!existingChecksum.getAlgorithm().equalsIgnoreCase(systemMetadata.getChecksum().getAlgorithm())) {
                        // we can't check algorithms that do not match, so get MN to recalculate with original checksum
                        logger.info("Task-" + task.getNodeId() + ":" + task.getPid() + " Try to retrieve a checksum from membernode that matches the checksum of existing systemMetadata");
                        newChecksum = nodeCommunications.getMnRead().getChecksum(session, systemMetadata.getIdentifier(), existingChecksum.getAlgorithm());
                    }
                    if (newChecksum.getValue().contentEquals(existingChecksum.getValue())) {
                        // how do we determine what is unique about this and whether it should be processed?
                        logger.info("Task-" + task.getNodeId() + ":" + task.getPid() + " Update sysMeta because checksum is same");
                        updateSystemMetadata(systemMetadata);
                    } else {
                        logger.info("Task-" + task.getNodeId() + ":" + task.getPid() + " Update sysMeta Not Unique! Checksum is different");

                        IdentifierNotUnique notUnique = new IdentifierNotUnique("-1", "Checksum does not match existing object with same pid.");
                        submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), notUnique);
                    }
                }
            }
        } catch (InvalidSystemMetadata ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (InvalidToken ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (NotFound ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (NotAuthorized ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (InvalidRequest ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (ServiceFailure ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (InsufficientResources ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (NotImplemented ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (UnsupportedType ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (IdentifierNotUnique ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (Exception ex) {
            logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.getMessage());
            ServiceFailure serviceFailure = new ServiceFailure("-1", ex.getMessage());
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), serviceFailure);
        }
    }

    private void createObject(SystemMetadata systemMetadata) throws InvalidRequest, ServiceFailure, NotFound, InsufficientResources, NotImplemented, InvalidToken, NotAuthorized, InvalidSystemMetadata, IdentifierNotUnique, UnsupportedType {
        Identifier d1Identifier = new Identifier();
        d1Identifier.setValue(systemMetadata.getIdentifier().getValue());
        // All though this should take place when the object is processsed, it needs to be
        // performed here due to the way last DateSysMetadataModified is used to
        // determine the next batch of records to retreive from a MemberNode
        systemMetadata.setDateSysMetadataModified(new Date());
        ObjectFormat objectFormat = nodeCommunications.getCnCore().getFormat(systemMetadata.getFormatId());
        if ((objectFormat != null) && !objectFormat.getFormatType().equalsIgnoreCase("DATA")) {
            InputStream sciMetaStream = null;
            // get the scimeta object and then feed it to metacat
            int tryAgain = 0;
            boolean needSciMetadata = true;
            do {
                try {
                    logger.debug("Task-" + task.getNodeId() + ":" + task.getPid() + " getting ScienceMetadata ");
                    sciMetaStream = nodeCommunications.getMnRead().get(session, systemMetadata.getIdentifier());
                    needSciMetadata = false;
                } catch (NotAuthorized ex) {
                    if (tryAgain < 2) {
                        ++tryAgain;
                        logger.error("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
                        try {
                            Thread.sleep(5000L);
                        } catch (InterruptedException ex1) {
                            logger.warn("Task-" + task.getNodeId() + ":" + task.getPid() + "\n" + ex);
                        }
                    } else {
                        // only way to get out of loop if NotAuthorized keeps getting thrown
                        throw ex;
                    }
                }
            } while (needSciMetadata);
            logger.info("Task-" + task.getNodeId() + ":" + task.getPid() + " Creating Object");

            d1Identifier = nodeCommunications.getCnCore().create(session, d1Identifier, sciMetaStream, systemMetadata);
            logger.info("Task-" + task.getNodeId() + ":" + task.getPid() + " Created Object");
        } else {
            logger.info("Task-" + task.getNodeId() + ":" + task.getPid() + " Registering SystemMetadata");
            hzSystemMetaMap.put(d1Identifier, systemMetadata);
             logger.info("Task-" + task.getNodeId() + ":" + task.getPid() + " Registered SystemMetadata");
//            nodeCommunications.getCnCore().registerSystemMetadata(session, d1Identifier, systemMetadata);
        }
    }

    private void updateSystemMetadata(SystemMetadata newSystemMetadata) throws InvalidSystemMetadata, NotFound, NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, InvalidToken {
        // Only update the systemMetadata fields that can be updated by a membernode
        // dateSysMetadataModified
        // obsoletedBy
        Identifier pid = new Identifier();
        pid.setValue(newSystemMetadata.getIdentifier().getValue());
//        SystemMetadata cnSystemMetadata = nodeCommunications.getCnRead().getSystemMetadata(session, newSystemMetadata.getIdentifier());
        SystemMetadata cnSystemMetadata = hzSystemMetaMap.get(pid);
        if (cnSystemMetadata.getAuthoritativeMemberNode().getValue().contentEquals(task.getNodeId())) {
            // this is an update from the original memberNode
            if ((cnSystemMetadata.getObsoletedBy() == null) && (newSystemMetadata.getObsoletedBy() != null)) {
                logger.debug(task.getNodeId() + ":" + task.getPid() + " Performing Update of systemMetadata due to an update operation having been performed on MN: " + task.getNodeId());
                cnSystemMetadata.setObsoletedBy(newSystemMetadata.getObsoletedBy());

                cnSystemMetadata.setDateSysMetadataModified(newSystemMetadata.getDateSysMetadataModified());
//                nodeCommunications.getCnCore().updateSystemMetadata(session, pid, cnSystemMetadata);
                hzSystemMetaMap.put(pid, cnSystemMetadata);
                auditReplicaSystemMetadata(cnSystemMetadata);
            } else {
                logger.warn(task.getNodeId() + ":" + task.getPid() + " Ignoring update from Authoritative MN");
            }
        } else {
            boolean performUpdate = true;
            // this may be an unrecorded replica
            // membernodes may have replicas of dataone objects that were created
            // before becoming a part of dataone
            List<Replica> prevReplicaList = cnSystemMetadata.getReplicaList();
            for (Replica replica : prevReplicaList) {
                if (task.getNodeId().equals(replica.getReplicaMemberNode().getValue())) {
                    performUpdate = false;
                    break;
                }
            }
            if (performUpdate) {
                logger.debug(task.getNodeId() + ":" + task.getPid() + " Performing Update of systemMetadata due a new replica being reported MN: " + task.getNodeId());
                Replica mnReplica = new Replica();
                NodeReference nodeReference = new NodeReference();
                nodeReference.setValue(task.getNodeId());
                mnReplica.setReplicaMemberNode(nodeReference);
                mnReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
                mnReplica.setReplicaVerified(new Date());
                cnSystemMetadata.getReplicaList().add(mnReplica);

                cnSystemMetadata.setDateSysMetadataModified(newSystemMetadata.getDateSysMetadataModified());
                cnSystemMetadata.setSerialVersion(cnSystemMetadata.getSerialVersion().add(BigInteger.ONE));
//                nodeCommunications.getCnCore().updateSystemMetadata(session, pid, cnSystemMetadata);

                hzSystemMetaMap.put(pid, cnSystemMetadata);
                // Eventually this should be triggering the Audit SystemMetadata Process Story #2040
                auditReplicaSystemMetadata(cnSystemMetadata);
            } else {
                logger.warn(task.getNodeId() + ":" + task.getPid() + " Ignoring update from Replica MN");
            }
        }
        // perform audit of replicas to make certain they all are at the same serialVersion level, if no update
        
    }

    private void auditReplicaSystemMetadata(SystemMetadata cnSystemMetadata) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, InvalidRequest, NotImplemented {
        IMap<NodeReference, Node> hzNodes = hazelcast.getMap("hzNodes");
        List<Replica> prevReplicaList = cnSystemMetadata.getReplicaList();
        Session session = null;
        for (Replica replica : prevReplicaList) {
            Node node = hzNodes.get(replica.getReplicaMemberNode());
            if (node.getType().equals(NodeType.MN)) {
                String mnUrl = node.getBaseURL();

                // Get an target MNode reference to communicate with
                // TODO: need to figure out better way to handle versioning! -rpw
                logger.info("Getting the MNode reference for " + node.getIdentifier().getValue() + " with baseURL " + mnUrl);
                MNode mnNode = new MNode(mnUrl);
                SystemMetadata mnSystemMetadata = mnNode.getSystemMetadata(session, cnSystemMetadata.getIdentifier());

                if (mnSystemMetadata.getSerialVersion() != cnSystemMetadata.getSerialVersion()) {

                    mnNode.systemMetadataChanged(session, cnSystemMetadata.getIdentifier(), cnSystemMetadata.getSerialVersion().longValue(), cnSystemMetadata.getDateSysMetadataModified());
                }
                
            }
        }
    }

    private void submitSynchronizationFailed(String pid, BaseException exception) {
        SyncFailedTask syncFailedTask = new SyncFailedTask(nodeCommunications, task);
        syncFailedTask.submitSynchronizationFailed(pid, exception);
    }
}
