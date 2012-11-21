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
import com.hazelcast.core.IMap;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.log4j.Logger;
import org.dataone.client.MNode;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.batch.synchronization.type.SyncObject;
import org.dataone.cn.hazelcast.HazelcastInstanceFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.impl.v1.ReserveIdentifierService;
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
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;

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

    Logger logger = Logger.getLogger(TransferObjectTask.class.getName());
    private NodeComm nodeCommunications;
    private SyncObject task;
    private Session session = null;
    // need this task queue if a failure occurs on the CN such that the task will
    // need to be processed on a separate CN
    private HazelcastInstance hazelcast = HazelcastInstanceFactory.getProcessingInstance();
    String cnIdentifier = Settings.getConfiguration().getString("cn.router.nodeId");
    String synchronizationObjectQueue = Settings.getConfiguration().getString("dataone.hazelcast.synchronizationObjectQueue");
    String hzNodesName = Settings.getConfiguration().getString("dataone.hazelcast.nodes");
    String hzSystemMetaMapString = Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");
    IMap<Identifier, SystemMetadata> hzSystemMetaMap;
    ReserveIdentifierService reserveIdentifierService;

    public TransferObjectTask(NodeComm nodeCommunications, SyncObject task) {
        this.nodeCommunications = nodeCommunications;
        this.task = task;
        this.hzSystemMetaMap = nodeCommunications.getHzClient().getMap(hzSystemMetaMapString);
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
        Lock lock = null;
        String lockPid = task.getPid();
        boolean isLocked = false;
        try {
            // this will be from the hazelcast client running against metacat
            logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Locking task of attempt " + task.getAttempt());
            long timeToWait = 1;

            lock = hazelcast.getLock(lockPid);
            isLocked = lock.tryLock(timeToWait, TimeUnit.SECONDS);
            if (isLocked) {
                logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Processing task");
                SystemMetadata systemMetadata = retrieveSystemMetadata();
                if (systemMetadata != null) {
                    logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Writing task");
                    try {
                        write(systemMetadata);
                    } catch (VersionMismatch ex) {

                        logger.warn("Task-" + task.getNodeId() + "-" + task.getPid() + " Pid altered before processing complete! Placing back on hzSyncObjectQueue of attempt " + task.getAttempt());
                        if (task.getAttempt() == 1) {
                            /*
                             * Member node should be informed to update its systemMetadata. If the member node is unable
                             * to update, this will be a nasty failure
                             */
                            Identifier pid = new Identifier();
                            pid.setValue(task.getPid());
                            auditReplicaSystemMetadata(pid);
                        }

                        if (task.getAttempt() < 6) {
                            //sleep for 10 seconds before trying again in order
                            // to allow time for membernode to refresh
                            // for a maximum of 6Xs
                            try {
                                Thread.sleep(10000L);
                            } catch (InterruptedException iex) {
                                logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + " " + iex.getMessage());

                            }
                            hazelcast.getQueue(synchronizationObjectQueue).put(task);
                            task.setAttempt(task.getAttempt() + 1);
                        } else {
                            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + " Pid altered before processing complete! Unable to process");
                        }

                    }
                } // else it was a failure and it should have been reported to MN so do nothing
            } else {
                try {
                    // there should be a max # of attempts from locking
                    if (task.getAttempt() < 100) {

                        logger.warn("Task-" + task.getNodeId() + "-" + task.getPid() + " Pid Locked! Placing back on hzSyncObjectQueue of attempt " + task.getAttempt());

                        /*
                         * allow a maximum number of attempts before permanent failure
                         */
                        task.setAttempt(task.getAttempt() + 1);
                        // wait a second to see if replication completes its action and releases the lock
                        try {
                            Thread.sleep(1000L);
                        } catch (InterruptedException iex) {
                            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + " " + iex.getMessage());

                        }
                        hazelcast.getQueue(synchronizationObjectQueue).put(task);

                    } else {
                        logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + " Pid Locked! Unable to process pid " + task.getPid() + " from node " + task.getNodeId());
                    }
                } catch (InterruptedException ex) {
                    logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + " Pid Locked! Unable to process pid " + task.getPid() + " from node " + task.getNodeId());
                    ServiceFailure serviceFailure = new ServiceFailure("564001", "Checksum does not match existing object with same pid");

                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.getMessage());
        }
        if (isLocked) {
            lock.unlock();
            logger.debug("Task-" + task.getNodeId() + "-" + task.getPid() + " Unlocked task");
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
                    systemMetadata = nodeCommunications.getMnRead().getSystemMetadata(null, identifier);
                    needSystemMetadata = false;
                } catch (NotAuthorized ex) {
                    if (tryAgain < 2) {
                        ++tryAgain;
                        logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
                        try {
                            Thread.sleep(5000L);
                        } catch (InterruptedException ex1) {
                            logger.warn("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex);
                        }
                    } else {
                        // only way to get out of loop if NotAuthorized keeps getting thrown
                        throw ex;
                    }
                } catch (ServiceFailure ex) {
                    if (tryAgain < 2) {
                        ++tryAgain;
                        logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
                        try {
                            Thread.sleep(5000L);
                        } catch (InterruptedException ex1) {
                            logger.warn("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex);
                        }
                    } else {
                        // only way to get out of loop if NotAuthorized keeps getting thrown
                        throw ex;
                    }
                }
            } while (needSystemMetadata);

            logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Retrieved SystemMetadata Identifier:" + systemMetadata.getIdentifier().getValue() + " from node " + memberNodeId + " for ObjectInfo Identifier " + identifier.getValue());

        } catch (NotAuthorized ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (InvalidToken ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (ServiceFailure ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (NotFound ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (NotImplemented ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (Exception ex) {
            ex.printStackTrace();

            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + " this didn't work", ex);
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

            logger.debug("Task-" + task.getNodeId() + "-" + task.getPid() + " Processing SystemMetadata");
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

            logger.debug("Task-" + task.getNodeId() + "-" + task.getPid() + " Included replica for original MN");
            // data objects are not fully synchronized, only their metadata is
            // synchronized,
            // only set valid science metadata formats as having been replicated
            logger.debug("Task-" + task.getNodeId() + "-" + task.getPid() + " Get Object Format");
            ObjectFormat objectFormat = nodeCommunications.getCnCore().getFormat(systemMetadata.getFormatId());
            if ((objectFormat != null) && !(objectFormat.getFormatType().equalsIgnoreCase("DATA"))) {
                NodeReference cnReference = new NodeReference();
                cnReference.setValue(cnIdentifier);
                Replica cnReplica = new Replica();
                cnReplica.setReplicaMemberNode(cnReference);
                cnReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
                cnReplica.setReplicaVerified(new Date());
                systemMetadata.addReplica(cnReplica);
                logger.debug("Task-" + task.getNodeId() + "-" + task.getPid() + " Added CN as replica because formatType " + objectFormat.getFormatType() + " is sciMetadata");
            }
            NodeReference originMemberNode = new NodeReference();
            originMemberNode.setValue(task.getNodeId());
            systemMetadata.setOriginMemberNode(originMemberNode);

            NodeReference authoritativeMemberNode = new NodeReference();
            authoritativeMemberNode.setValue(task.getNodeId());
            systemMetadata.setAuthoritativeMemberNode(authoritativeMemberNode);

        } catch (ServiceFailure ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (NotFound ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (NotImplemented ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(task.getPid(), ex);
            return null;
        } catch (Exception ex) {
            ex.printStackTrace();

            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + " this didn't work", ex);
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

            logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Getting sysMeta from CN");


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
                doCreate = reserveIdentifierService.hasReservation(verifySubmitter, systemMetadata.getSubmitter(), systemMetadata.getIdentifier());
                logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Create from reservation");
            } catch (NotFound ex) {
                doCreate = true;
                // assume if reserveIdentifierService has thrown NotFound exception SystemMetadata does not exist
                logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Create from Exception");
            } catch (IdentifierNotUnique ex) {
                logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Pid Exists. Must be an Update");
            }
            // create, update or ignore
            if (doCreate) {
                systemMetadata = processSystemMetadata(systemMetadata);
                if (systemMetadata != null) {
                    createObject(systemMetadata);
                }
            } else {
                // determine if this is a valid update
                SystemMetadata cnSystemMetadata = hzSystemMetaMap.get(systemMetadata.getIdentifier());
                if (cnSystemMetadata != null) {
                    Checksum existingChecksum = cnSystemMetadata.getChecksum(); // maybe an update, maybe duplicate, maybe a conflicting pid
                    Checksum newChecksum = systemMetadata.getChecksum();
                    if (!existingChecksum.getAlgorithm().equalsIgnoreCase(systemMetadata.getChecksum().getAlgorithm())) {
                        // we can't check algorithms that do not match, so get MN to recalculate with original checksum
                        logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Try to retrieve a checksum from membernode that matches the checksum of existing systemMetadata");
                        newChecksum = nodeCommunications.getMnRead().getChecksum(session, systemMetadata.getIdentifier(), existingChecksum.getAlgorithm());
                    }
                    if (newChecksum.getValue().contentEquals(existingChecksum.getValue())) {
                        // how do we determine what is unique about this and whether it should be processed?
                        logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Update sysMeta because checksum is same");
                        updateSystemMetadata(systemMetadata);
                    } else {
                        logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Update sysMeta Not Unique! Checksum is different");

                        IdentifierNotUnique notUnique = new IdentifierNotUnique("-1", "Checksum does not match existing object with same pid.");
                        submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), notUnique);
                    }
                } else {
                    logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + " is null when get called from Hazelcast " + hzSystemMetaMapString + " Map");
                }
            }
        } catch (VersionMismatch ex) {
            logger.warn("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            throw ex;
        } catch (InvalidSystemMetadata ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (InvalidToken ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (NotFound ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (NotAuthorized ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (InvalidRequest ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (ServiceFailure ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (InsufficientResources ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (NotImplemented ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (UnsupportedType ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (IdentifierNotUnique ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
            submitSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.getMessage());
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
                    logger.debug("Task-" + task.getNodeId() + "-" + task.getPid() + " getting ScienceMetadata ");
                    sciMetaStream = nodeCommunications.getMnRead().get(session, systemMetadata.getIdentifier());
                    needSciMetadata = false;
                } catch (NotAuthorized ex) {
                    if (tryAgain < 2) {
                        ++tryAgain;
                        logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
                        try {
                            Thread.sleep(5000L);
                        } catch (InterruptedException ex1) {
                            logger.warn("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex);
                        }
                    } else {
                        // only way to get out of loop if NotAuthorized keeps getting thrown
                        throw ex;
                    }
                } catch (ServiceFailure ex) {
                    if (tryAgain < 2) {
                        ++tryAgain;
                        logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex.serialize(ex.FMT_XML));
                        try {
                            Thread.sleep(5000L);
                        } catch (InterruptedException ex1) {
                            logger.warn("Task-" + task.getNodeId() + "-" + task.getPid() + "\n" + ex);
                        }
                    } else {
                        // only way to get out of loop if NotAuthorized keeps getting thrown
                        throw ex;
                    }
                }
            } while (needSciMetadata);
            logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Creating Object");

            d1Identifier = nodeCommunications.getCnCore().create(session, d1Identifier, sciMetaStream, systemMetadata);
            logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Created Object");
        } else {
            logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Registering SystemMetadata");
            nodeCommunications.getCnCore().registerSystemMetadata(session, d1Identifier, systemMetadata);
            logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Registered SystemMetadata");
        }
    }

    /*
     * Object is already created. This opertation will only update systemmetadata if a portion of the systemmeta data
     * has changed that synchroniziation can update. Namely, The authoritative member node can update the obsoletedBy
     * field or if an existing replica is found, then the replica information is added to the systemMetadata.
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
     *
     */
    private void updateSystemMetadata(SystemMetadata newSystemMetadata) throws InvalidSystemMetadata, NotFound, NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, InvalidToken, VersionMismatch {
        // Only update the systemMetadata fields that can be updated by a membernode
        //
        // obsoletedBy
        // archived
        // replicas
        //
        Identifier pid = new Identifier();
        pid.setValue(newSystemMetadata.getIdentifier().getValue());
        SystemMetadata cnSystemMetadata = hzSystemMetaMap.get(pid);
        if (cnSystemMetadata.getAuthoritativeMemberNode().getValue().contentEquals(task.getNodeId())) {
            // this is an update from the original memberNode
            if ((cnSystemMetadata.getObsoletedBy() == null) && (newSystemMetadata.getObsoletedBy() != null)) {
                logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Update ObsoletedBy");

                nodeCommunications.getCnCore().setObsoletedBy(session, pid, newSystemMetadata.getObsoletedBy(), cnSystemMetadata.getSerialVersion().longValue());
                auditReplicaSystemMetadata(pid);
                // serial version will be updated at this point, so get the new version
                logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Updated ObsoletedBy");
            } else if (((newSystemMetadata.getArchived() != null) && newSystemMetadata.getArchived())
                    && ((cnSystemMetadata.getArchived() == null) || !cnSystemMetadata.getArchived())) {
                logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Update Archived");

                nodeCommunications.getCnCore().archive(session, pid);
                auditReplicaSystemMetadata(pid);
                // serial version will be updated at this point, so get the new version
                logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Updated Archived");
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
                logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Update Replica");
                Replica mnReplica = new Replica();
                NodeReference nodeReference = new NodeReference();
                nodeReference.setValue(task.getNodeId());
                mnReplica.setReplicaMemberNode(nodeReference);
                mnReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
                mnReplica.setReplicaVerified(new Date());

                nodeCommunications.getCnReplication().updateReplicationMetadata(session, pid, mnReplica, cnSystemMetadata.getSerialVersion().longValue());

                auditReplicaSystemMetadata(pid);
                logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " Updated Replica");
            } else {
                logger.warn(task.getNodeId() + "-" + task.getPid() + " Ignoring update from Replica MN");
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
    private void auditReplicaSystemMetadata(Identifier pid) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, InvalidRequest, NotImplemented {
        IMap<NodeReference, Node> hzNodes = hazelcast.getMap(hzNodesName);
        SystemMetadata cnSystemMetadata = hzSystemMetaMap.get(pid);
        if (cnSystemMetadata != null) {
            List<Replica> prevReplicaList = cnSystemMetadata.getReplicaList();
            Session session = null;
            logger.info("Task-" + task.getNodeId() + "-" + task.getPid() + " auditReplicaSystemMetadata");
            for (Replica replica : prevReplicaList) {
                Node node = hzNodes.get(replica.getReplicaMemberNode());
                if (node.getType().equals(NodeType.MN)) {
                    boolean isTier3 = false;
                    // Find out if a teir 3 node, if not then do not callback since it is not implemented
                    for (Service service : node.getServices().getServiceList()) {
                        if (service.getName().equals("MNStorage") && service.getAvailable()) {
                            isTier3 = true;
                            break;
                        }
                    }
                    if (isTier3) {
                        String mnUrl = node.getBaseURL();

                        MNode mnNode = new MNode(mnUrl);
                        SystemMetadata mnSystemMetadata = mnNode.getSystemMetadata(session, cnSystemMetadata.getIdentifier());

                        if (mnSystemMetadata.getSerialVersion() != cnSystemMetadata.getSerialVersion()) {

                            mnNode.systemMetadataChanged(session, cnSystemMetadata.getIdentifier(), cnSystemMetadata.getSerialVersion().longValue(), cnSystemMetadata.getDateSysMetadataModified());
                        }
                    }
                }
            }
        } else {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + " is null when get called from Hazelcast " + hzSystemMetaMapString + " Map");
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
        syncFailedTask.submitSynchronizationFailed(pid, exception);
    }
}
