/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.synchronization.tasks;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.log4j.Logger;
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
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;

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

    public TransferObjectTask(NodeComm nodeCommunications, SyncObject task) {
        this.nodeCommunications = nodeCommunications;
        this.task = task;
    }

    @Override
    public Void call() {
        logger.debug("Locking task");
        try {
// this will be from the hazelcast client running against metacat
//        Lock lockObject = hazelcast.getLock(task.getPid());
//        if (lockObject.tryLock()) {
            logger.info("Processing task");
            SystemMetadata systemMetadata = process(task.getNodeId(), task.getPid());
            if (systemMetadata != null) {
                logger.info("Writing task");
                write(systemMetadata);
            } //else {
            // object never written such that metacat replication
            // will never report that replication is complete
            // and listener will never unlock the object
//                lockObject.unlock();
//            }
//        } else {
//            try {
//                logger.warn("Pid Locked! Placing task pid: " + task.getPid() + " from " + task.getNodeId() + " back on hzSyncObjectQueue");
//                hazelcast.getQueue("hzSyncObjectQueue").put(task);
//            } catch (InterruptedException ex) {
//                logger.error("Unable to process pid " + task.getPid() + " from node " + task.getNodeId());
            //               ServiceFailure serviceFailure = new ServiceFailure("564001", "Checksum does not match existing object with same pid");

            //           }
//        }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error(ex.getMessage());
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
                        logger.error(ex.serialize(ex.FMT_XML));
                        try {
                            Thread.sleep(5000L);
                        } catch (InterruptedException ex1) {
                            logger.warn(ex);
                        }
                    } else {
                        // only way to get out of loop if NotAuthorized keeps getting thrown
                        throw ex;
                    }
                }
            } while (needSystemMetadata);

            logger.debug("Retrieved SystemMetadata Identifier:" + systemMetadata.getIdentifier().getValue() + " from node " + memberNodeId + " for ObjectInfo Identifier " + identifier.getValue());
            NodeReference nodeReference = new NodeReference();

            nodeReference.setValue(memberNodeId);
            logger.debug("Retrieved nodeReference set");
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
                originalReplica.setReplicaMemberNode(nodeReference);
                originalReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
                originalReplica.setReplicaVerified(new Date());
                logger.debug("Retrieved originalReplica");
                systemMetadata.addReplica(originalReplica);
            }
            logger.debug("Finished adding replica for original MN");
            try {
                // data objects are not fully synchronized, only their metadata is
                // synchronized,
                // only set valid science metadata formats as having been replicated
                logger.debug("Get Object Format");
                ObjectFormat objectFormat = nodeCommunications.getCnCore().getFormat(systemMetadata.getFmtid());
                if (objectFormat != null && !objectFormat.getFormatType().equalsIgnoreCase("DATA")) {
                    NodeReference cnReference = new NodeReference();
                    cnReference.setValue(cnIdentifier);
                    Replica cnReplica = new Replica();
                    cnReplica.setReplicaMemberNode(cnReference);
                    cnReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
                    cnReplica.setReplicaVerified(new Date());
                    systemMetadata.addReplica(cnReplica);
                    logger.debug("Added CN as replica because data is sciMetadata");
                }
            } catch (InsufficientResources ex) {
                try {
                    // maybe another
                    logger.error(ex.serialize(BaseException.FMT_XML));
                    hazelcast.getQueue("hzSyncObjectQueue").offer(task, 2, TimeUnit.SECONDS);
                } catch (Exception ex1) {
                    logger.error("Unable to process pid " + pid + " from node " + memberNodeId);
                    ServiceFailure serviceFailure = new ServiceFailure("-1", ex1.getMessage());
                    submitSynchronizationFailed(pid, serviceFailure);
                }
                return null;
            }

            systemMetadata.setOriginMemberNode(nodeReference);
            systemMetadata.setAuthoritativeMemberNode(nodeReference);
        } catch (NotAuthorized ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(pid, ex);
            return null;
        } catch (InvalidToken ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(pid, ex);
            return null;
        } catch (ServiceFailure ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(pid, ex);
            return null;
        } catch (NotFound ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(pid, ex);
            return null;
        } catch (NotImplemented ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(pid, ex);
            return null;
        } catch (InvalidRequest ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(pid, ex);
            return null;
        } catch (Exception ex) {
            ex.printStackTrace();

            logger.error("this didn't work", ex);
            ServiceFailure serviceFailure = new ServiceFailure("-1", ex.getMessage());
            submitSynchronizationFailed(pid, serviceFailure);
            return null;
        }
        return systemMetadata;
    }

    private void write(SystemMetadata systemMetadata) {
        // is this an update or create?

        try {

            Checksum existingChecksum = null; // maybe an update, maybe duplicate, maybe a conflicting pid
            logger.info("Getting sysMeta from CN");
            try {
                existingChecksum = nodeCommunications.getCnRead().getChecksum(session, systemMetadata.getIdentifier());
            } catch (NotFound notFound) {
                // it is a create operation !
                logger.info("Create sysMeta");
                createObject(systemMetadata);
                return;
            }
            if (systemMetadata != null) {
                  Checksum newChecksum = systemMetadata.getChecksum();
                  if (!existingChecksum.getAlgorithm().equalsIgnoreCase(systemMetadata.getChecksum().getAlgorithm())) {
                        // we can't check algorithms that do not match, so get MN to recalculate with original checksum
                        logger.info("Try to retrieve a checksum from membernode that matches the checksum of existing systemMetadata");
                        newChecksum = nodeCommunications.getMnRead().getChecksum(session, systemMetadata.getIdentifier(), existingChecksum.getAlgorithm());
                   }
                if (newChecksum.getValue().contentEquals(existingChecksum.getValue())) {
                    // how do we determine what is unique about this and whether it should be processed?
                    logger.info("Update sysMeta because checksum is same");
                    updateSystemMetadata(systemMetadata);
                } else {
                    logger.info("Update sysMeta Not Unique! Checksum is different");

                    IdentifierNotUnique notUnique = new IdentifierNotUnique("-1", "Checksum does not match existing object with same pid.");
                    submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), notUnique);
                }
            }
        } catch (InvalidSystemMetadata ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (InvalidToken ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (NotFound ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (NotAuthorized ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (InvalidRequest ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (ServiceFailure ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (InsufficientResources ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (NotImplemented ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (UnsupportedType ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (IdentifierNotUnique ex) {
            logger.error(ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
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
        ObjectFormat objectFormat = nodeCommunications.getCnCore().getFormat(systemMetadata.getFmtid());
        if ((objectFormat != null) && !objectFormat.getFormatType().equalsIgnoreCase("DATA")) {
            InputStream sciMetaStream = null;
            // get the scimeta object and then feed it to metacat
            int tryAgain = 0;
            boolean needSciMetadata = true;
            do {
                try {
                    sciMetaStream = nodeCommunications.getMnRead().get(session, systemMetadata.getIdentifier());
                    needSciMetadata = false;
                } catch (NotAuthorized ex) {
                    if (tryAgain < 2) {
                        ++tryAgain;
                        logger.error(ex.serialize(ex.FMT_XML));
                        try {
                            Thread.sleep(5000L);
                        } catch (InterruptedException ex1) {
                            logger.warn(ex);
                        }
                    } else {
                        // only way to get out of loop if NotAuthorized keeps getting thrown
                        throw ex;
                    }
                }
            } while (needSciMetadata);

            d1Identifier = nodeCommunications.getCnCore().create(session, d1Identifier, sciMetaStream, systemMetadata);

        } else {
            nodeCommunications.getCnCore().registerSystemMetadata(session, d1Identifier, systemMetadata);
        }
    }

    private void updateSystemMetadata(SystemMetadata newSystemMetadata) throws InvalidSystemMetadata, NotFound, NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, InvalidToken {
        // Only update the systemMetadata fields that can be updated by a membernode
        // dateSysMetadataModified
        // obsoletedBy
        
        SystemMetadata oldSystemMetadata = nodeCommunications.getCnRead().getSystemMetadata(session, newSystemMetadata.getIdentifier());
        if (oldSystemMetadata.getAuthoritativeMemberNode().getValue().contentEquals(task.getNodeId())) {
            // this is an update from the original memberNode
            if (newSystemMetadata.getObsoletedBy() != null) {
                logger.debug("Performing Update of systemMetadata due to an update operation having been performed on MN: " + task.getNodeId());
                oldSystemMetadata.setObsoletedBy(newSystemMetadata.getObsoletedBy());
                Identifier pid = new Identifier();
                pid.setValue(oldSystemMetadata.getIdentifier().getValue());
                oldSystemMetadata.setDateSysMetadataModified(newSystemMetadata.getDateSysMetadataModified());
                nodeCommunications.getCnCore().updateSystemMetadata(session, pid, oldSystemMetadata);
            }
        } else {
            boolean performUpdate = true;
            // this may be an unrecorded replica
            // membernodes may have replicas of dataone objects that were created
            // before becoming a part of dataone
            List<Replica> prevReplicaList = oldSystemMetadata.getReplicaList();
            for (Replica replica : prevReplicaList) {
                if (task.getNodeId().equals(replica.getReplicaMemberNode().getValue())) {
                    performUpdate = false;
                    break;
                }
            }
            if (performUpdate) {

                Replica mnReplica = new Replica();
                NodeReference nodeReference = new NodeReference();
                nodeReference.setValue(task.getNodeId());
                mnReplica.setReplicaMemberNode(nodeReference);
                mnReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
                mnReplica.setReplicaVerified(new Date());
                oldSystemMetadata.getReplicaList().add(mnReplica);
                Identifier pid = new Identifier();
                pid.setValue(oldSystemMetadata.getIdentifier().getValue());
                oldSystemMetadata.setDateSysMetadataModified(newSystemMetadata.getDateSysMetadataModified());
                nodeCommunications.getCnCore().updateSystemMetadata(session, pid, oldSystemMetadata);

            }
        }
    }

    private void submitSynchronizationFailed(String pid, BaseException exception) {
        SyncFailedTask syncFailedTask = new SyncFailedTask(nodeCommunications, task);
        syncFailedTask.submitSynchronizationFailed(pid, exception);
    }
}
