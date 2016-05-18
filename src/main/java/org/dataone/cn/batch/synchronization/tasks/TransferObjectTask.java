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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.IMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.exceptions.NodeCommUnavailable;
import org.dataone.cn.batch.synchronization.NodeCommSyncObjectFactory;
import org.dataone.cn.batch.service.v2.IdentifierReservationQueryService;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.batch.synchronization.type.NodeCommState;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.synchronization.types.SyncObject;
import org.dataone.configuration.Settings;
import org.dataone.ore.ResourceMapFactory;
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
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.mn.tier1.v2.MNRead;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.util.ChecksumUtil;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.ObjectFormat;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.types.v2.TypeFactory;
import org.dataone.service.util.TypeMarshaller;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREParserException;
import org.jibx.runtime.JiBXException;

/**
 * Transfer an object from a MemberNode(MN) to a CoordinatingNode(CN). Executes as a thread that is executed by the
 * SyncObjectTask class
 *
 * This class will download SystemMetadata from a MN and process the SystemMetadata. It will then take actions based on
 * the information in the SystemMetadata.
 *
 * If the SystemMetadata describes a new Science Metadata object, it will download the object and perform a create
 *
 * If the SystemMetadata describes a new Science Data Object, it will register itself with the CN leaving the Data on
 * the MN
 *
 * If the SystemMetadata already exists on the CN, then based on the state of the SystemMetadata it will either be
 * ignored, updated, or throw an exception.
 *
 * @author waltz
 */
public class TransferObjectTask implements Callable<Void> {

    private static final BigInteger CHECKSUM_VERIFICATION_SIZE_BYPASS_THRESHOLD
            = Settings.getConfiguration().getBigInteger(
                    "Synchronization.checksum.verify.size.bypass.threshold",
                    BigInteger.valueOf(10000000));

    static final Logger logger = Logger.getLogger(TransferObjectTask.class);
    private NodeComm nodeCommunications;
    private SyncObject task;
    private Session session = null;
    // need this task queue if a failure occurs on the CN such that the task will
    // need to be processed on a separate CN
    private HazelcastClient hzProcessingClient = HazelcastClientFactory.getProcessingClient();
    private HazelcastClient hzStorageClient = HazelcastClientFactory.getStorageClient();
    String cnIdentifier
            = Settings.getConfiguration().getString("cn.router.nodeId");
    String synchronizationObjectQueue
            = Settings.getConfiguration().getString("dataone.hazelcast.synchronizationObjectQueue");

    String hzSystemMetaMapString
            = Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");

    IMap<Identifier, SystemMetadata> hzSystemMetaMap;
    IdentifierReservationQueryService reserveIdentifierService;

    public TransferObjectTask(NodeComm nodeCommunications, SyncObject task) {
        this.nodeCommunications = nodeCommunications;
        this.task = task;
        this.hzSystemMetaMap = hzStorageClient.getMap(hzSystemMetaMapString);
        this.reserveIdentifierService = nodeCommunications.getReserveIdentifierService();
    }

    /**
     * Implement the Callable interface. The process will attempt to lock the pid in order to exclude replication and
     * synchronization from changing the same object. It will then process the systemMetadata from the Membernode, and
     * lastly write the data (systemMetadata and any storage types of information) to Storage. The lock will then be
     * released
     *
     * @return null
     * @throws Exception
     *
     */
    @Override
    public Void call() throws Exception {
        Lock hzPidLock = null;
        String lockPid = task.getPid();
        boolean isPidLocked = false;
        try {

            // this will be from the hzProcessingClient client running against metacat
            logger.info(task.taskLabel()
                    + " Locking task of attempt " + task.getAttempt());

            hzPidLock = hzProcessingClient.getLock(lockPid);
            isPidLocked = hzPidLock.tryLock(1, TimeUnit.SECONDS); // both parameters define the wait time
            if (isPidLocked) {
                logger.info(task.taskLabel() + " Processing task");
                SystemMetadata systemMetadata = retrieveSystemMetadata();
                if (systemMetadata != null) {
                    logger.info(task.taskLabel() + " Writing task");
                    try {
                        write(systemMetadata);
                    } catch (VersionMismatch ex) {

                        logger.warn(task.taskLabel()
                                + " Pid altered before processing complete! "
                                + "Placing back on hzSyncObjectQueue of attempt "
                                + task.getAttempt());
                        if (task.getAttempt() == 1) {
                            // Member node should be informed to update its systemMetadata. 
                            // If the member node is unable to update, 
                            // this will be a nasty failure
                            Identifier pid = new Identifier();
                            pid.setValue(task.getPid());
                            auditReplicaSystemMetadata(pid);
                        }

                        if (task.getAttempt() < 6) {
                            //sleep for 10 seconds before trying again in order
                            // to allow time for membernode to refresh
                            // for a maximum of 6Xs
                            // TODO: pro-active sleeping and tying up a thread 
                            // before re-queueing seems to waste CN time
                            // recording when we returned the task to the queue
                            // then sleeping the difference upon retrying might 
                            // work, although accounting for different CN instance 
                            // clocks might make this imprecise.
                            try {
                                Thread.sleep(10000L);
                            } catch (InterruptedException iex) {
                                logger.error(task.taskLabel() + " " + iex.getMessage());

                            }
                            hzProcessingClient.getQueue(synchronizationObjectQueue).put(task);
                            task.setAttempt(task.getAttempt() + 1);
                        } else {
                            logger.error(task.taskLabel() + " Pid altered before processing complete! Unable to process");
                        }

                    }
                } // else it was a failure and it should have been reported to MN so do nothing
            } else { // could not lock the pid

                // there should be a max # of attempts from locking
                if (task.getAttempt() < 100) {

                    logger.warn(task.taskLabel()
                            + " Pid Locked! Placing back on hzSyncObjectQueue of attempt "
                            + task.getAttempt());

                    /*
                     * allow a maximum number of attempts before permanent failure
                     */
                    task.setAttempt(task.getAttempt() + 1);
                        // wait a second to give the CN (replication processing) a bit of time to complete
                    // its action and release the lock before the next sync
                    // attempt on this object.
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException iex) {
                        logger.error(task.taskLabel() + " " + iex.getMessage());

                    }
                    hzProcessingClient.getQueue(synchronizationObjectQueue).put(task);

                } else {
                    logger.error(task.taskLabel()
                            + " Pid Locked! Unable to process pid " + task.getPid()
                            + " from node " + task.getNodeId());
                }

            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error(task.taskLabel() + "\n" + ex.getMessage());
        }
        if (isPidLocked) {
            hzPidLock.unlock();
            logger.debug(task.taskLabel() + " Unlocked task");
        }
        return null;
    }

    /*
     * Process the Task before writing it out to storage
     *
     * Read in the SystemMetadata from the Membernode. Set the membernode being synchronized as the Origin &
     * Authoritative Membernode (will be ignored if an update op) Add member node as a replica Add CN as a replica if
     * the object is not a Sci Data object
     *
     * @return SystemMetadata
     *
     */
    private SystemMetadata retrieveSystemMetadata() {
        String memberNodeId = task.getNodeId();
        SystemMetadata systemMetadata = null;
        try {
            //            sciMetaFile = this.writeScienceMetadataToFile(objectInfo);
            Identifier identifier = new Identifier();
            identifier.setValue(task.getPid());
            int tryAgain = 0;
            boolean needSystemMetadata = true;

            do {
                try {
                    Object mnRead = nodeCommunications.getMnRead();
                    if (mnRead instanceof MNRead) {
                        systemMetadata = ((MNRead) mnRead).getSystemMetadata(null, identifier);
                        needSystemMetadata = false;
                    } else if (mnRead instanceof org.dataone.service.mn.tier1.v1.MNRead) {
                        org.dataone.service.types.v1.SystemMetadata oldSystemMetadata = ((org.dataone.service.mn.tier1.v1.MNRead) mnRead)
                                .getSystemMetadata(null, identifier);
                        systemMetadata = TypeFactory.convertTypeFromType(oldSystemMetadata,
                                SystemMetadata.class);
                        needSystemMetadata = false;
                    }
                } catch (NotAuthorized ex) {
                    // TODO: why does the logger.warn statements have "+ ex" in the message?
                    if (tryAgain < 2) {
                        ++tryAgain;
                        logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
                        try {
                            Thread.sleep(5000L);
                        } catch (InterruptedException ex1) {
                            logger.warn(task.taskLabel() + "\n" + ex);
                        }
                    } else {
                        // only way to get out of loop if NotAuthorized keeps getting thrown
                        throw ex;
                    }
                } catch (ServiceFailure ex) {
                    if (tryAgain < 6) {
                        ++tryAgain;
                        logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
                        try {
                            Thread.sleep(5000L);
                        } catch (InterruptedException ex1) {
                            logger.warn(task.taskLabel() + "\n" + ex);
                        }
                    } else {
                        // only way to get out of loop if NotAuthorized keeps getting thrown
                        throw ex;
                    }
                }
            } while (needSystemMetadata);
            if (!task.getPid().contentEquals(systemMetadata.getIdentifier().getValue())) {
                InvalidSystemMetadata invalidSystemMetadata = new InvalidSystemMetadata(
                        "567100",
                        "Identifier "
                        + task.getPid()
                        + " retrieved from getObjectList is different from that contained in systemMetadata "
                        + systemMetadata.getIdentifier().getValue());
                logger.error(task.taskLabel() + "\n"
                        + invalidSystemMetadata.serialize(invalidSystemMetadata.FMT_XML));
                submitSynchronizationFailed(task.getPid(), invalidSystemMetadata);
                return null;
            }
            logger.info(task.taskLabel() + " Retrieved SystemMetadata Identifier:"
                    + systemMetadata.getIdentifier().getValue() + " from node " + memberNodeId
                    + " for ObjectInfo Identifier " + identifier.getValue());

        } catch (NotAuthorized ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (InvalidToken ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (ServiceFailure ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (NotFound ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (NotImplemented ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (Exception ex) {
            ex.printStackTrace();

            logger.error(task.taskLabel() + "\n this didn't work", ex);
            ServiceFailure serviceFailure = new ServiceFailure("-1", ex.getMessage());
            submitSynchronizationFailed(task.getPid(), serviceFailure);
            return null;
        }
        return systemMetadata;
    }

    /*
     * Process the Task before creating it
     *
     * Set the membernode being synchronized as the Origin &
     * Authoritative Membernode (will be ignored if an update op) 
     *
     * Replaces replica list if provided by MN
     *
     * Add member node as a replica Add CN as a replica if
     * the object is not a Sci Data object
     *
     * @param SystemMetadata
     * @return SystemMetadata 
     *
     */
    private SystemMetadata processSystemMetadata(SystemMetadata systemMetadata) {

        try {
            logger.debug(task.taskLabel() + " Processing SystemMetadata");
            boolean addOriginalReplica = true;
            /*
             * DataONE Bug #2603 Synchronization should delete existing replicas on create
             */
            List<Replica> replicaList = new ArrayList<Replica>();
            systemMetadata.setReplicaList(replicaList);

            Replica originalReplica = new Replica();
            NodeReference originalReplicaReference = new NodeReference();
            originalReplicaReference.setValue(task.getNodeId());
            originalReplica.setReplicaMemberNode(originalReplicaReference);
            originalReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
            originalReplica.setReplicaVerified(new Date());
            systemMetadata.addReplica(originalReplica);

            logger.debug(task.taskLabel() + " Included replica for original MN");
            // data objects are not fully synchronized, only their metadata is
            // synchronized,
            // only set valid science metadata formats as having been replicated
            logger.debug(task.taskLabel() + " Get Object Format");
            ObjectFormat objectFormat = nodeCommunications.getCnCore().getFormat(
                    systemMetadata.getFormatId());
            if ((objectFormat != null) && !(objectFormat.getFormatType().equalsIgnoreCase("DATA"))) {
                NodeReference cnReference = new NodeReference();
                cnReference.setValue(cnIdentifier);
                Replica cnReplica = new Replica();
                cnReplica.setReplicaMemberNode(cnReference);
                cnReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
                cnReplica.setReplicaVerified(new Date());
                systemMetadata.addReplica(cnReplica);
                logger.debug(task.taskLabel()
                        + " Added CN as replica because formatType " + objectFormat.getFormatType()
                        + " is sciMetadata");
            }
            // the origin membernode may be different from the node
            // being harvested.  
            if (systemMetadata.getOriginMemberNode() == null
                    || systemMetadata.getOriginMemberNode().getValue().isEmpty()) {
                NodeReference originMemberNode = new NodeReference();
                originMemberNode.setValue(task.getNodeId());
                systemMetadata.setOriginMemberNode(originMemberNode);
            }
            // Do not override the authoritative MemberNode assigned by a MemberNode
            // part of redmine Task #3062
            if (systemMetadata.getAuthoritativeMemberNode() == null
                    || systemMetadata.getAuthoritativeMemberNode().getValue().isEmpty()) {
                //                InvalidSystemMetadata invalidSystemMetadata = new InvalidSystemMetadata("567100", "Identifier " + task.getPid() + " does not contain valid AuthoritativeNode Entry ");
                //                logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + invalidSystemMetadata.serialize(invalidSystemMetadata.FMT_XML));
                //                submitSynchronizationFailed(task.getPid(), invalidSystemMetadata);
                //                return null;
                // while I agree with the above comment, Authoritative MemberNode is a field that is optional
                // but it is important for the usefulness of an object
                // so for now, fill it in if it is empty
                NodeReference authoritativeMemberNode = new NodeReference();
                authoritativeMemberNode.setValue(task.getNodeId());
                systemMetadata.setAuthoritativeMemberNode(authoritativeMemberNode);
            }

        } catch (ServiceFailure ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (NotFound ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (NotImplemented ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (Exception ex) {
            ex.printStackTrace();

            // TODO: why is there a newline in this log statement?
            logger.error(task.taskLabel() + "\n this didn't work", ex);
            ServiceFailure serviceFailure = new ServiceFailure("-1", ex.getMessage());
            submitSynchronizationFailed(task.getPid(), serviceFailure);
            return null;
        }
        return systemMetadata;
    }

    /*
     * Determine if the object should be created as a new entry, updated or ignored
     *
     * @param SystemMetadata systemMetdata from the MN 
     * @throws VersionMismatch
     */
    private void write(SystemMetadata systemMetadata) throws VersionMismatch {
        // is this an update or create?

        try {

            logger.info(task.taskLabel() + " Getting sysMeta from CN");

            // use the identity manager to determine if the PID already exists or is previously
            // reserved. 
            // If the PID is already created hasReservation throws an IdentifierNotUnique
            // this means we should go through the update logic
            // If the PID has been reserved, then either NotAuthorized will be thrown
            // indicating that the PID was reserved by another user
            // or true is returned, indicating that the subject indeed has the reservation
            //
            boolean doCreate = false;
            try {
                Session verifySubmitter = new Session();
                verifySubmitter.setSubject(systemMetadata.getSubmitter());
                doCreate = reserveIdentifierService.hasReservation(verifySubmitter,
                        systemMetadata.getSubmitter(), systemMetadata.getIdentifier());
                logger.info(task.taskLabel() + " Create from reservation");
            } catch (NotFound ex) {
                doCreate = true;
                // assume if identifierReservationService has thrown NotFound exception SystemMetadata does not exist
                logger.info(task.taskLabel() + " Create from Exception");
//            } catch (IdentifierNotUnique ex) {
//                logger.info(task.taskLabel() + " Pid Exists. Must be an Update");
            }
            // create, update or ignore
            if (doCreate) {
                systemMetadata = processSystemMetadata(systemMetadata);
                if (systemMetadata != null) {
                    // if (systemMetadata.getOriginMemberNode().getValue().contentEquals(systemMetadata.getAuthoritativeMemberNode().getValue())) {
                    createObject(systemMetadata);
                    //} else {
                    // the object does not yet exist and a replica is attempting to create the object
                    // this can not be performed until the original object is created.
                    // replicas can not be synchronized before the original object
                    // because the original object will have different information in the systemMetadata
                    //   InvalidRequest invalidRequest = new InvalidRequest("567121", "Authoritative MemberNode " + systemMetadata.getAuthoritativeMemberNode().getValue() + " is different than Origin Member Node " + systemMetadata.getOriginMemberNode().getValue() + ". Replicas may not be synchronized before the Original Object has been created");
                    //  logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + invalidRequest.serialize(invalidRequest.FMT_XML));
                    //  submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), invalidRequest);    
                    //  logger.warn(task.getNodeId() + "-" + task.getPid() + " Ignoring create from Replica MN");
                    //}
                }
            } else {
                // determine if this is a valid update
                SystemMetadata cnSystemMetadata = hzSystemMetaMap.get(systemMetadata.getIdentifier());
                if (cnSystemMetadata != null && cnSystemMetadata.getChecksum() != null) {
                    Checksum existingChecksum = cnSystemMetadata.getChecksum(); // maybe an update, maybe duplicate, maybe a conflicting pid
                    Checksum newChecksum = systemMetadata.getChecksum();
                    if (!existingChecksum.getAlgorithm().equalsIgnoreCase(
                            systemMetadata.getChecksum().getAlgorithm())) {
                        // we can't check algorithms that do not match, so get MN to recalculate with original checksum
                        logger.info(task.taskLabel() + " Try to retrieve a checksum from "
                                + "membernode that matches the checksum of existing systemMetadata");
                        Object mnRead = nodeCommunications.getMnRead();
                        if (mnRead instanceof MNRead) {
                            newChecksum = ((MNRead) mnRead)
                                    .getChecksum(session, systemMetadata.getIdentifier(),
                                            existingChecksum.getAlgorithm());
                        } else if (mnRead instanceof org.dataone.service.mn.tier1.v1.MNRead) {
                            newChecksum = ((org.dataone.service.mn.tier1.v1.MNRead) mnRead)
                                    .getChecksum(session, systemMetadata.getIdentifier(),
                                            existingChecksum.getAlgorithm());
                        }
                    }
                    if (newChecksum.getValue().contentEquals(existingChecksum.getValue())) {
                        // how do we determine what is unique about this and whether it should be processed?
                        logger.info(task.taskLabel() + " Update sysMeta because checksum is same");
                        updateSystemMetadata(systemMetadata);
                    } else {
                        logger.info(task.taskLabel() + " Update sysMeta Not Unique! Checksum is different");

                        IdentifierNotUnique notUnique = new IdentifierNotUnique("-1",
                                "Checksum does not match existing object with same pid.");
                        submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(),
                                notUnique);
                    }
                } else {
                    if (cnSystemMetadata == null) {
                        logger.error(task.taskLabel() + " cn's systemMetadata is null when get called from Hazelcast "
                                + hzSystemMetaMapString + " Map");
                    } else {
                        logger.error(task.taskLabel()
                                + " cn's systemMetadata's checksum is null when get called from Hazelcast "
                                + hzSystemMetaMapString + " Map");
                    }
                }
            }
        } catch (VersionMismatch ex) {
            logger.warn(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            throw ex;
        } catch (InvalidSystemMetadata ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (InvalidToken ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (NotFound ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (NotAuthorized ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (InvalidRequest ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (ServiceFailure ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (InsufficientResources ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (NotImplemented ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (UnsupportedType ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (IdentifierNotUnique ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error(task.taskLabel() + "\n" + ex.getMessage());
            ServiceFailure serviceFailure = new ServiceFailure("-1", ex.getMessage());
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), serviceFailure);
        }
    }

    /*
     * Create the object if a resource or sci meta object. Register systemmetadata if a sci data object.
     *
     *
     * @param SystemMetadata systemMetdata from the MN 
     * @throws InvalidRequest 
     * @throws ServiceFailure 
     * @throws NotFound
     * @throws InsufficientResources 
     * @throws NotImplemented 
     * @throws InvalidToken 
     * @throws NotAuthorized 
     * @throws InvalidSystemMetadata 
     * @throws IdentifierNotUnique 
     * @throws UnsupportedType
     *
     */
    private void createObject(SystemMetadata systemMetadata) throws InvalidRequest, ServiceFailure,
            NotFound, InsufficientResources, NotImplemented, InvalidToken, NotAuthorized,
            InvalidSystemMetadata, IdentifierNotUnique, UnsupportedType {
        Identifier d1Identifier = new Identifier();
        d1Identifier.setValue(systemMetadata.getIdentifier().getValue());
        // All though this should take place when the object is processsed, it needs to be
        // performed here due to the way last DateSysMetadataModified is used to
        // determine the next batch of records to retreive from a MemberNode
        systemMetadata.setDateSysMetadataModified(new Date());
        ObjectFormat objectFormat = nodeCommunications.getCnCore().getFormat(
                systemMetadata.getFormatId());

        validateSystemMetadata(systemMetadata);
        validateChecksum(systemMetadata);

        if ((objectFormat != null) && !objectFormat.getFormatType().equalsIgnoreCase("DATA")) {
            InputStream sciMetaStream = null;
            // get the scimeta object and then feed it to metacat

            try {
                int tryAgain = 0;

                boolean needSciMetadata = true;
                do {
                    try {
                        logger.debug(task.taskLabel() + " getting ScienceMetadata ");
                        Object mnRead = nodeCommunications.getMnRead();
                        if (mnRead instanceof MNRead) {
                            sciMetaStream = ((MNRead) mnRead).get(null, systemMetadata.getIdentifier());
                            needSciMetadata = false;
                        } else if (mnRead instanceof org.dataone.service.mn.tier1.v1.MNRead) {
                            sciMetaStream = ((org.dataone.service.mn.tier1.v1.MNRead) mnRead).get(null,
                                    systemMetadata.getIdentifier());
                            needSciMetadata = false;
                        }
                    } catch (NotAuthorized ex) {
                        if (tryAgain < 2) {
                            ++tryAgain;
                            logger.error(task.taskLabel() + "\n" + ex.serialize(BaseException.FMT_XML));
                            try {
                                Thread.sleep(5000L);
                            } catch (InterruptedException ex1) {
                                logger.warn(task.taskLabel() + "\n" + ex);
                            }
                        } else {
                            // only way to get out of loop if NotAuthorized keeps getting thrown
                            throw ex;
                        }
                    } catch (ServiceFailure ex) {
                        if (tryAgain < 6) {
                            ++tryAgain;
                            logger.error(task.taskLabel() + "\n" + ex.serialize(BaseException.FMT_XML));
                            try {
                                Thread.sleep(5000L);
                            } catch (InterruptedException ex1) {
                                logger.warn(task.taskLabel() + "\n" + ex);
                            }
                        } else {
                            // only way to get out of loop if NotAuthorized keeps getting thrown
                            throw ex;
                        }
                    }
                } while (needSciMetadata);

                // while good intentioned, this may be too restrictive for "RESOURCE" formats
                // see: https://redmine.dataone.org/issues/6848
                // commenting out for now. BRL 20150211
                /*
                 if (isResource(objectFormat)) {
                 byte[] resourceBytes = null;
                 try {
                 resourceBytes = IOUtils.toByteArray(sciMetaStream);
                 } catch (IOException e) {
                 throw new InsufficientResources("413",
                 "Unable to create ByteArrayInputStream for pid: "
                 + systemMetadata.getIdentifier().getValue() + " with message: "
                 + e.getMessage());
                 }
                 if (resourceBytes != null) {
                 sciMetaStream = new ByteArrayInputStream(resourceBytes);
                 validateResource(resourceBytes);
                 }
                 }
                 */
                logger.info(task.taskLabel() + " Creating Object");
                d1Identifier = nodeCommunications.getCnCore().create(null, d1Identifier, sciMetaStream,
                        systemMetadata);
                logger.info(task.taskLabel() + " Created Object");
            } finally {
                IOUtils.closeQuietly(sciMetaStream);
            }
        } else {
            logger.info(task.taskLabel() + " Registering SystemMetadata");
            nodeCommunications.getCnCore().registerSystemMetadata(null, d1Identifier,
                    systemMetadata);
            logger.info(task.taskLabel() + " Registered SystemMetadata");
        }
    }

    private void validateChecksum(SystemMetadata systemMetadata) throws InvalidSystemMetadata {
        if (systemMetadata.getSize().compareTo(CHECKSUM_VERIFICATION_SIZE_BYPASS_THRESHOLD) < 0) {
            Exception checksumException = null;
            Checksum expectedChecksum = systemMetadata.getChecksum();
            Checksum actualChecksum = null;
            try {
                Object mnRead = nodeCommunications.getMnRead();
                if (mnRead instanceof MNRead) {
                    actualChecksum = ((MNRead) mnRead).getChecksum(session,
                            systemMetadata.getIdentifier(), expectedChecksum.getAlgorithm());
                } else if (mnRead instanceof org.dataone.service.mn.tier1.v1.MNRead) {
                    actualChecksum = ((org.dataone.service.mn.tier1.v1.MNRead) mnRead).getChecksum(
                            session, systemMetadata.getIdentifier(),
                            expectedChecksum.getAlgorithm());
                }
            } catch (InvalidRequest e) {
                checksumException = e;
            } catch (InvalidToken e) {
                checksumException = e;
            } catch (NotAuthorized e) {
                checksumException = e;
            } catch (NotImplemented e) {
                checksumException = e;
            } catch (ServiceFailure e) {
                checksumException = e;
            } catch (NotFound e) {
                checksumException = e;
            }
            if (!ChecksumUtil.areChecksumsEqual(expectedChecksum, actualChecksum)
                    || checksumException != null) {
                String pid = "null";
                if (systemMetadata != null && systemMetadata.getIdentifier() != null
                        && systemMetadata.getIdentifier().getValue() != null) {
                    pid = systemMetadata.getIdentifier().getValue();
                }
                String errorMessage = "The checksum for pid: " + pid
                        + " does not match the actual checksum supplied by the member node: "
                        + systemMetadata.getOriginMemberNode().getValue() + ".  Actual checksum: "
                        + actualChecksum.getValue() + ". System metadata checksum: "
                        + expectedChecksum.getValue();
                InvalidSystemMetadata be = new InvalidSystemMetadata("000", errorMessage);
                if (checksumException != null) {
                    be.initCause(checksumException);
                }
                logger.error(task.taskLabel() + ": " + errorMessage);
                throw be;
            }
        }
    }

    private boolean validateResource(byte[] resourceBytes) throws UnsupportedType {
        boolean valid = false;
        if (resourceBytes != null) {
            InputStream resourceStream = null;
            try {
                resourceStream = new ByteArrayInputStream(resourceBytes);
                ResourceMapFactory.getInstance().parseResourceMap(resourceStream);
                valid = true;
            } catch (UnsupportedEncodingException e) {
                throw new UnsupportedType("Invalid Resource Map",
                        "Unable to parse document as a resource map: " + e.getMessage());
            } catch (OREException e) {
                throw new UnsupportedType("Invalid Resource Map",
                        "Unable to parse document as a resource map: " + e.getMessage());
            } catch (URISyntaxException e) {
                throw new UnsupportedType("Invalid Resource Map",
                        "Unable to parse document as a resource map: " + e.getMessage());
            } catch (OREParserException e) {
                throw new UnsupportedType("Invalid Resource Map",
                        "Unable to parse document as a resource map: " + e.getMessage());
            } finally {
                IOUtils.closeQuietly(resourceStream);
            }
        }
        return valid;
    }

    private boolean isResource(ObjectFormat format) {
        boolean isResource = false;
        if (format != null && format.getFormatType().equalsIgnoreCase("RESOURCE")) {
            isResource = true;
        }
        return isResource;
    }

    /**
     * Throws InvalidSystemMetadata if the input sysmeta param is not schema valid.
     *
     * @param sysmeta
     * @throws InvalidSystemMetadata
     */
    private void validateSystemMetadata(SystemMetadata sysmeta) throws InvalidSystemMetadata {
        Exception caught = null;
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            TypeMarshaller.marshalTypeToOutputStream(sysmeta, os);
            os.close();
        } catch (JiBXException e) {
            caught = e;
        } catch (IOException e) {
            caught = e;
        }
        if (caught != null) {
            String pid = "null";
            if (sysmeta != null && sysmeta.getIdentifier() != null
                    && sysmeta.getIdentifier().getValue() != null) {
                pid = sysmeta.getIdentifier().getValue();
            }
            String errorMessage = "The SystemMetadata for pid: " + pid + " is not schema valid";
            InvalidSystemMetadata be = new InvalidSystemMetadata("000", errorMessage);
            be.initCause(caught);
            logger.error(errorMessage, be);
            throw be;
        }
    }

    /*
     * To be called if the Object is already created. This operation will update 
     * system metadata on the CN if the source of the new SystemMetadata is from
     * the Authoritative source and only allowable fields have changed OR if it's
     * from a replicate node, it will register the node in the Replica section of
     * the system metadata if the system metadata is the same and it is not already
     * registered.
     * <br/>
     * (For v1) Allowable fields for the Authoritative Member Node to change are:
     *  'obsoletedBy' and 'archived' (and of course 'dateSystemMetadataUpdated')
     *
     * @param SystemMetadata systemMetdata from the MN 
     * @throws InvalidRequest 
     * @throws ServiceFailure 
     * @throws NotFound
     * @throws NotImplemented 
     * @throws InvalidToken 
     * @throws NotAuthorized
     * @throws InvalidSystemMetadata
     * @throwsVersionMismatch
     */
    private void updateSystemMetadata(SystemMetadata newSystemMetadata)
            throws InvalidSystemMetadata, NotFound, NotImplemented, NotAuthorized, ServiceFailure,
            InvalidRequest, InvalidToken, VersionMismatch {
        // Only update the systemMetadata fields that can be updated by a membernode
        //
        // obsoletedBy - via mn.update
        // archived - via mn.archive
        // replicas - via peer-to-peer-replication (outside of D1 api)?
        //
        Identifier pid = new Identifier();
        pid.setValue(newSystemMetadata.getIdentifier().getValue());
        SystemMetadata cnSystemMetadata = hzSystemMetaMap.get(pid);

        if (task.getNodeId().contentEquals(
                cnSystemMetadata.getAuthoritativeMemberNode().getValue())) {

            // this is an update from the authoritative memberNode
            // so look for fields with valid changes
            boolean foundValidMNChange = false;

            // obsoletedBy can be updated to a value only if its value hasn't
            // already been set.  Once set, it cannot change.
            if ((cnSystemMetadata.getObsoletedBy() == null)
                    && (newSystemMetadata.getObsoletedBy() != null)) {
                logger.info(task.taskLabel() + " Updating ObsoletedBy...");

                nodeCommunications.getCnCore().setObsoletedBy(session, pid,
                        newSystemMetadata.getObsoletedBy(),
                        cnSystemMetadata.getSerialVersion().longValue());
                //                auditReplicaSystemMetadata(pid);
                // serial version will be updated at this point, so get the new version
                logger.info(task.taskLabel() + " Updated ObsoletedBy");
                foundValidMNChange = true;
            }

            // (getArchived() returns a boolean)
            // only process the update if the new sysmeta set it to true and the 
            // existing value is null or false.  Cannot change the value from true
            // to false.
            if (((newSystemMetadata.getArchived() != null) && newSystemMetadata.getArchived())
                    && ((cnSystemMetadata.getArchived() == null) || !cnSystemMetadata.getArchived())) {
                logger.info(task.taskLabel() + " Updating Archived...");
                nodeCommunications.getCnCore().archive(session, pid);
                //                auditReplicaSystemMetadata(pid);
                // serial version will be updated at this point, so get the new version
                logger.info(task.taskLabel() + " Updated Archived");
                foundValidMNChange = true;
            }
            if (foundValidMNChange) {
                auditReplicaSystemMetadata(pid);

            } else {
                // TODO: refactor to assume less about how we got here and whether or not to throw an exception
                // 
                // a simple reharvest may lead to getting to this point, so check
                // the sysmeta modified date before throwing an exception
                if (newSystemMetadata.getDateSysMetadataModified().after(
                        cnSystemMetadata.getDateSysMetadataModified())) {
                    // something has changed, and we should probably investigate,
                    // but for now just assume that an out-of-bounds change was attempted.
                    InvalidRequest invalidRequest = new InvalidRequest(
                            "567123",
                            "Synchronization unable to process the update request. Only archived and obsoletedBy may be updated");
                    logger.error(task.taskLabel() + "\n" + invalidRequest.serialize(invalidRequest.FMT_XML));
                    submitSynchronizationFailed(pid.getValue(), invalidRequest);
                    logger.warn(task.taskLabel() + " Ignoring update from MN. Only archived and obsoletedBy may be updated");
                }
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
                logger.info(task.taskLabel() + " Update Replica");
                Replica mnReplica = new Replica();
                NodeReference nodeReference = new NodeReference();
                nodeReference.setValue(task.getNodeId());
                mnReplica.setReplicaMemberNode(nodeReference);
                mnReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
                mnReplica.setReplicaVerified(new Date());

                nodeCommunications.getCnReplication().updateReplicationMetadata(session, pid,
                        mnReplica, cnSystemMetadata.getSerialVersion().longValue());

                auditReplicaSystemMetadata(pid);
                logger.info(task.taskLabel() + " Updated Replica");
            } else {

                // attempt to determine what the MN was updating and report back with
                // synchronizationFailed
                InvalidRequest invalidRequest = new InvalidRequest(
                        "567123",
                        "Not Authorized Node to perform Updates. Synchronization unable to process the update request. Only archived and obsoletedBy may be updated from Authorized Node");
                logger.error(task.taskLabel() + "\n" + invalidRequest.serialize(invalidRequest.FMT_XML));
                submitSynchronizationFailed(pid.getValue(), invalidRequest);
                logger.warn(task.taskLabel() + " Ignoring update from Replica MN");
            }
        }
        // perform audit of replicas to make certain they all are at the same serialVersion level, if no update ?

    }

    /*
     * Inform Member Nodes that may have a copy to refresh their version of the systemmetadata
     *
     * @param Identifier pid 
     * @throws InvalidRequest 
     * @throws ServiceFailure 
     * @throws NotFound 
     * @throws NotImplemented
     * @throws InvalidToken 
     * @throws NotAuthorized
     *
     */
    private void auditReplicaSystemMetadata(Identifier pid) throws InvalidToken, ServiceFailure,
            NotAuthorized, NotFound, InvalidRequest, NotImplemented {
        
        SystemMetadata cnSystemMetadata = hzSystemMetaMap.get(pid);
        if (cnSystemMetadata != null) {
            List<Replica> prevReplicaList = cnSystemMetadata.getReplicaList();
            Session session = null;
            logger.info(task.taskLabel() + " auditReplicaSystemMetadata");
            for (Replica replica : prevReplicaList) {
                Node node = nodeCommunications.getNodeRegistryService().getNode(replica.getReplicaMemberNode());
                if (node.getType().equals(NodeType.MN)) {
                    boolean isTier3 = false;
                    // Find out if a tier 3 node, if not then do not callback since it is not implemented
                    for (Service service : node.getServices().getServiceList()) {
                        if (service.getName().equals("MNStorage") && service.getAvailable()) {
                            isTier3 = true;
                            break;
                        }
                    }
                    if (isTier3) {
                        NodeComm nodeComm = null;
                        try {
                            nodeComm = NodeCommSyncObjectFactory.getInstance().getNodeComm(
                                    node.getIdentifier());


                            Object mNode = nodeComm.getMnRead();
                            if (mNode instanceof MNRead) {
                                SystemMetadata mnSystemMetadata = ((MNRead) mNode).getSystemMetadata(
                                        session, cnSystemMetadata.getIdentifier());
                                if (mnSystemMetadata.getSerialVersion() != cnSystemMetadata
                                        .getSerialVersion()) {
                                    ((MNRead) mNode)
                                    .systemMetadataChanged(session, cnSystemMetadata
                                            .getIdentifier(), cnSystemMetadata
                                            .getSerialVersion().longValue(), cnSystemMetadata
                                            .getDateSysMetadataModified());
                                }
                            } else if (mNode instanceof org.dataone.client.v1.MNode) {
                                org.dataone.service.types.v1.SystemMetadata mnSystemMetadata = ((org.dataone.client.v1.MNode) mNode)
                                        .getSystemMetadata(session, cnSystemMetadata.getIdentifier());
                                if (mnSystemMetadata.getSerialVersion() != cnSystemMetadata
                                        .getSerialVersion()) {
                                    ((org.dataone.client.v1.MNode) mNode).systemMetadataChanged(
                                            session, cnSystemMetadata.getIdentifier(), cnSystemMetadata
                                            .getSerialVersion().longValue(), cnSystemMetadata
                                            .getDateSysMetadataModified());
                                }
                            }
                        } catch (NodeCommUnavailable e) {
                            throw new ServiceFailure("0000", e.getMessage());
                        } finally {
                            if (nodeComm != null)
                                nodeComm.setState(NodeCommState.AVAILABLE);
                        }
                    }
                }
            }
        } else {
            logger.error(task.taskLabel() + " is null when get called from Hazelcast "
                    + hzSystemMetaMapString + " Map");
        }
    }

    /*
     * Inform Member Nodes that synchronization task failed
     *
     * @param String pid 
     * @param BaseException message showing reason of failure
     *
     */
    private void submitSynchronizationFailed(String pid, BaseException exception) {
        SyncFailedTask syncFailedTask = new SyncFailedTask(nodeCommunications, task);
        syncFailedTask.submitSynchronizationFailed(pid, null, exception);
    }
}
