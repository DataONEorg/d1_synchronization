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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.cn.batch.exceptions.InternalSyncFailure;
import org.dataone.cn.batch.exceptions.NodeCommUnavailable;
import org.dataone.cn.batch.synchronization.D1TypeUtils;
import org.dataone.cn.batch.synchronization.NodeCommSyncObjectFactory;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.batch.synchronization.type.SyncObject;
import org.dataone.cn.hazelcast.HazelcastInstanceFactory;
import org.dataone.configuration.Settings;
import org.dataone.ore.ResourceMapFactory;
import org.dataone.service.cn.impl.v2.ReserveIdentifierService;
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
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.mn.tier1.v2.MNRead;
import org.dataone.service.types.v1.AccessPolicy;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.util.AccessUtil;
import org.dataone.service.types.v1.util.AuthUtils;
import org.dataone.service.types.v1.util.ChecksumUtil;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.ObjectFormat;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREParserException;
import org.jibx.runtime.JiBXException;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

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
public class V2TransferObjectTask implements Callable<Void> {

    private static final BigInteger CHECKSUM_VERIFICATION_SIZE_BYPASS_THRESHOLD = 
            Settings.getConfiguration().getBigInteger(
                    "Synchronization.checksum.verify.size.bypass.threshold",
                    BigInteger.valueOf(10000000));

    private static final String[] VALIDATING_RESOURCE_FORMATS = new String[]{
        "http://www.openarchives.org/ore/terms"
        };
    
    Logger logger = Logger.getLogger(V2TransferObjectTask.class.getName());
    private NodeComm nodeCommunications;
    private SyncObject task;
    private Session session = null;
    // need this task queue if a failure occurs on the CN such that the task will
    // need to be processed on a separate CN
    private HazelcastInstance hazelcast = HazelcastInstanceFactory.getProcessingInstance();
    String cnIdentifier = 
            Settings.getConfiguration().getString("cn.router.nodeId");
    String synchronizationObjectQueue = 
            Settings.getConfiguration().getString("dataone.hazelcast.synchronizationObjectQueue");
    String hzNodesName = 
            Settings.getConfiguration().getString("dataone.hazelcast.nodes");
    String hzSystemMetaMapString = 
            Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");
    
    IMap<Identifier, SystemMetadata> hzSystemMetaMap;
    ReserveIdentifierService reserveIdentifierService;

    public V2TransferObjectTask(NodeComm nodeCommunications, SyncObject task) {
        this.nodeCommunications = nodeCommunications;
        this.task = task;
        this.hzSystemMetaMap = nodeCommunications.getHzClient().getMap(hzSystemMetaMapString);
        this.reserveIdentifierService = nodeCommunications.getReserveIdentifierService();
    }

    private Identifier cloneIdentifier(Identifier pid) {
        Identifier id = new Identifier();
        id.setValue(pid.getValue());
        return id;
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
            
            // this will be from the hazelcast client running against metacat
            logger.info(task.taskLabel()
                    + " Locking task of attempt " + task.getAttempt());

            hzPidLock = hazelcast.getLock(lockPid);
            isPidLocked = hzPidLock.tryLock(1, TimeUnit.SECONDS); // both parameters define the wait time
            if (isPidLocked) {      
                logger.info(task.taskLabel() + " Processing task");
                SystemMetadata systemMetadata = retrieveSystemMetadata();
                
                try {
                    logger.info(task.taskLabel() + " Writing task");
                    write(systemMetadata);
                } 
                catch (VersionMismatch ex) {
                    logger.warn(task.taskLabel()
                            + " Pid altered before processing complete! " +
                            "Placing back on hzSyncObjectQueue of attempt "
                            + task.getAttempt());
                    if (task.getAttempt() == 1) {
                        // Member node should be informed to update its systemMetadata. 
                        // If the member node is unable to update, an exception is thrown(?)
                        auditReplicaSystemMetadata(D1TypeBuilder.buildIdentifier(task.getPid()));
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
                        interruptableSleep(10000L);
                        hazelcast.getQueue(synchronizationObjectQueue).put(task);
                        task.setAttempt(task.getAttempt() + 1);
                    } else {
                        logger.error(task.taskLabel() + " Pid altered before processing complete! Unable to process");
                    }

                }
                
            } else { // could not lock the pid
                try {
                    if (task.getAttempt() < 100) {
                        logger.warn(task.taskLabel()
                                + " Cannot lock Pid! Requeueing the task. Attempt " + task.getAttempt());

                        task.setAttempt(task.getAttempt() + 1);
                        Thread.sleep(1000L); // to give the lock-holder time before trying again
                        hazelcast.getQueue(synchronizationObjectQueue).put(task);
                    } else {
                        logger.error(task.taskLabel()
                                + " Cannot lock Pid! Reached Max attempts, abandoning processing of this pid.");
                    }
                } catch (InterruptedException ex) {
                    logger.error(task.taskLabel()
                            + " Cannot lock Pid! Interrupted. Abandoning processing of this pid." + " " + ex.getMessage());
                }
            }
        } catch (SynchronizationFailed ex) {
            SyncFailedTask syncFailedTask = new SyncFailedTask(nodeCommunications, task);
            syncFailedTask.submitSynchronizationFailed(ex);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            logger.error(task.taskLabel() + "\n" + ex.getMessage());
        }
        if (isPidLocked) {
            hzPidLock.unlock();
            logger.debug(task.taskLabel() + " Unlocked Pid.");
        }
        return null;
    }

    /**
     * Retrieves the SystemMetadata from the target MemberNode.  
     * @return
     * @throws SynchronizationFailed - if it cannot get valid SystemMetadata
     *         (deserialization errors, or pid doesn't match that in the request)
     */
    private SystemMetadata retrieveSystemMetadata() throws SynchronizationFailed 
    {
        String memberNodeId = task.getNodeId();
        SystemMetadata systemMetadata = null;
        try {
            systemMetadata = getSystemMetadataHandleRetry();

        } catch (BaseException ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), ex);

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error(task.taskLabel() + "\n this didn't work", ex);
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), ex);
        }
        return systemMetadata;
    }

    
    /**
     * Tries to get the SystemMetadata from the node specified in the task.  It
     * has a retry strategy implemented for NotAuthorized and ServiceFailured.
     * It also verifies that the retrieved SysMeta has a matching PID.
     * 
     * @return SystemMetadata
     * @throws NotAuthorized   - retries up to 2x
     * @throws ServiceFailure  - retired up to 6x
     * @throws InvalidToken
     * @throws NotImplemented
     * @throws NotFound
     * @throws InvalidSystemMetadata - if the pid in the Sysmeta doesn't match the request
     */
    private SystemMetadata getSystemMetadataHandleRetry() throws NotAuthorized, ServiceFailure, 
    InvalidToken, NotImplemented, NotFound, InvalidSystemMetadata {
        SystemMetadata retrievedSysMeta = null;
        boolean needSystemMetadata = true;
        int tryAgain = 0;
        do {
            try {
                Object mnRead = nodeCommunications.getMnRead();
                if (mnRead instanceof MNRead) {
                    retrievedSysMeta = ((MNRead) mnRead).getSystemMetadata(null, D1TypeBuilder.buildIdentifier(task.getPid()));

                } else if (mnRead instanceof org.dataone.service.mn.tier1.v1.MNRead) {
                    org.dataone.service.types.v1.SystemMetadata oldSystemMetadata = ((org.dataone.service.mn.tier1.v1.MNRead) mnRead)
                            .getSystemMetadata(null, D1TypeBuilder.buildIdentifier(task.getPid()));
                    
                    try {
                        retrievedSysMeta = TypeMarshaller.convertTypeFromType(oldSystemMetadata,
                                SystemMetadata.class);

                    } catch (Exception e) { // catches conversion issues
                        e.printStackTrace();
                        throw new ServiceFailure("-1", "Error converting v1.SystemMetadata to v2.SystemMetadata: " + e.getMessage());
                    }
                }
                if (!retrievedSysMeta.getIdentifier().getValue().contentEquals(task.getPid())) {
                    // didn't get the right SystemMetadata after all
                    throw new InvalidSystemMetadata(
                            "567100",
                            String.format(
                                    "Identifier in the retrieved SystemMetadata (%s) is different from "
                                            + "the identifier used to retrieve the SystemMetadata (%s).)",
                                            task.getPid(),
                                            retrievedSysMeta.getIdentifier().getValue()
                                    )
                            );
                }
                logger.info(task.taskLabel() + " Retrieved SystemMetadata Identifier:"
                        + retrievedSysMeta.getIdentifier().getValue() + " from node " + task.getNodeId()
                        + " for ObjectInfo Identifier " + task.getPid());
            } 
            // be persistent on a couple kinds of exceptions
            catch (NotAuthorized ex) {
                if (tryAgain < 2) {
                    ++tryAgain;
                    logger.error(task.taskLabel() + ": NotAuthorized. Sleeping 5s and retrying...\n" + ex.serialize(ex.FMT_XML));
                    interruptableSleep(5000L);
                } else {
                    throw ex;
                }
            } catch (ServiceFailure ex) {
                if (tryAgain < 6) {
                    ++tryAgain;
                    logger.error(task.taskLabel() + ": ServiceFailure. Sleeping 5s and retrying...\n" + ex.serialize(ex.FMT_XML));
                    interruptableSleep(5000L);
                } else {
                    throw ex;
                }
            }
        } while (needSystemMetadata);
        return retrievedSysMeta;
    }
    
    
    
    private void interruptableSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            logger.warn(task.taskLabel() + "\n" + ex);
        }
    }


    /*
     * Modify the systemMetadata with new values set by the CN during synchronization
     * 
     * Primarily, it sets the ReplicaList with replicas it knows about (the source
     * MN and the CN if it's not DATA object)
     * 
     * It also sets the Origin and Authoritative MemberNode fields appropriately.
     *
     * @param SystemMetadata
     * @return SystemMetadata 
     *
     */
    private SystemMetadata processNewSystemMetadata(SystemMetadata systemMetadata) throws SynchronizationFailed {

        try {
            IMap<NodeReference, Node> hzNodes = hazelcast.getMap(hzNodesName);
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
            if (D1TypeUtils.emptyEquals(systemMetadata.getOriginMemberNode(),null)) {
                NodeReference originMemberNode = new NodeReference();
                originMemberNode.setValue(task.getNodeId());
                systemMetadata.setOriginMemberNode(originMemberNode);
            }
            // Do not override the authoritative MemberNode assigned by a MemberNode
            // part of redmine Task #3062
            if (D1TypeUtils.emptyEquals(systemMetadata.getAuthoritativeMemberNode(), null)) {
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
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), ex);

        } catch (NotFound ex) {
            logger.error(task.taskLabel() +  "\n" + ex.serialize(ex.FMT_XML));
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), ex);

        } catch (NotImplemented ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), ex);

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error(task.taskLabel() + "\n this didn't work", ex);
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), ex);

        }
        return systemMetadata;
    }

    
    /**
     * Check the reservation service to see if the the object already exists or not.
     * @param sysMeta
     * @return
     * @throws NotAuthorized
     */
    private boolean alreadyExists(SystemMetadata sysMeta) throws NotAuthorized {
        // use the identity manager to determine if the PID already exists or is previously
        // reserved. 
        // If the PID is already created hasReservation throws an IdentifierNotUnique
        // this means we should go through the update logic
        // If the PID has been reserved, then either NotAuthorized will be thrown
        // indicating that the PID was reserved by another user
        // or true is returned, indicating that the subject indeed has the reservation
        //
        boolean exists = false;
        try {
            Session verifySubmitter = new Session();
            verifySubmitter.setSubject(sysMeta.getSubmitter());
            reserveIdentifierService.hasReservation(verifySubmitter, sysMeta.getSubmitter(), sysMeta.getIdentifier());
            logger.info(task.taskLabel() + " Create from reservation");
        } catch (NotFound ex) {
            // assume if reserveIdentifierService has thrown NotFound exception SystemMetadata does not exist
            logger.info(task.taskLabel() + " Create from Exception");
        } catch (IdentifierNotUnique ex) {
            exists = true;
            logger.info(task.taskLabel() + " Pid Exists. Must be an Update");
        }
        return exists;
    }
    
    
    /*
     * Determine if the object should be created as a new entry, updated or ignored
     *
     * @param SystemMetadata systemMetdata from the MN 
     * @throws VersionMismatch
     */
    private void write(SystemMetadata systemMetadata) throws VersionMismatch, SynchronizationFailed {
        try {
            logger.info(task.taskLabel() + " Getting sysMeta from CN");

            if (!alreadyExists(systemMetadata)) {
                systemMetadata = processNewSystemMetadata(systemMetadata);
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
                updateSystemMetadata(systemMetadata);
            }
        } catch (VersionMismatch ex) {
            logger.warn(task.taskLabel() + "\n" + ex.serialize(ex.FMT_XML));
            throw ex;
        } catch (BaseException be) {
            logger.error(task.taskLabel() + "\n" + be.serialize(BaseException.FMT_XML));
            throw SyncFailedTask.createSynchronizationFailed(systemMetadata.getIdentifier().getValue(), be); 
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error(task.taskLabel() + "\n" + ex.getMessage());
            throw SyncFailedTask.createSynchronizationFailed(systemMetadata.getIdentifier().getValue(), ex);
        }
    }
    
    /**
     * The essential properties of the retrieved systemMetadata are those that 
     * describe immutable properties of the object.  These must not be different
     * in the the new version of the systemMetadata.
     * 
     * @param newSysMeta
     * @throws BaseException - thrown if there is a reportable problem validating
     * @throws InternalSyncFailure - thrown if there is an internal problem validating
     */
    private void validateEssentialProperties(SystemMetadata newSysMeta, SystemMetadata cnSysMeta)
    throws BaseException, InternalSyncFailure
    {
        if (cnSysMeta == null) {
            String message = task.taskLabel()  + " cn's systemMetadata is null when get called from Hazelcast "
                    + hzSystemMetaMapString + " Map";
            logger.error(message);
            throw new InternalSyncFailure(message);
        }
        
        if (cnSysMeta.getChecksum() == null) {
            String message = task.taskLabel() + " cn's systemMetadata's checksum is null when get called from Hazelcast "
                    + hzSystemMetaMapString + " Map";
            logger.error(message);
            throw new InternalSyncFailure(message);
        }
        

        String cnCsAlgorithm = cnSysMeta.getChecksum().getAlgorithm();
        Checksum newChecksum = newSysMeta.getChecksum();
        
        if (!cnCsAlgorithm.equalsIgnoreCase(newChecksum.getAlgorithm())) {
            
            // we can't check algorithms that do not match, so get MN to recalculate using provided algorithm
            logger.info(task.taskLabel() + " Try to retrieve a checksum from " +
                    "membernode that matches the algorithm of existing systemMetadata");
            Object mnRead = nodeCommunications.getMnRead();
            
            if (mnRead instanceof MNRead) {
                newChecksum = ((MNRead) mnRead)
                        .getChecksum(session, newSysMeta.getIdentifier(), cnCsAlgorithm);

            } else if (mnRead instanceof org.dataone.service.mn.tier1.v1.MNRead) {
                newChecksum = ((org.dataone.service.mn.tier1.v1.MNRead) mnRead)
                        .getChecksum(session, newSysMeta.getIdentifier(), cnCsAlgorithm);
            }
        }
        
        if (!newChecksum.getValue().contentEquals(cnSysMeta.getChecksum().getValue())) {
            logger.info(task.taskLabel() + " submitted checksum doesn't match the existing one!");
            throw new IdentifierNotUnique("-1",
                    "Checksum does not match existing object with same pid.");
        }
        
        logger.info(task.taskLabel()  + " submitted checksum matches existing one");
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
                        interruptableSleep(5000L);
                    } else {
                        // only way to get out of loop if NotAuthorized keeps getting thrown
                        throw ex;
                    }
                } catch (ServiceFailure ex) {
                    if (tryAgain < 6) {
                        ++tryAgain;
                        logger.error(task.taskLabel() + "\n" + ex.serialize(BaseException.FMT_XML));
                        interruptableSleep(5000L);
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
             validateResourceMap(objectFormat, sciMetaStream);
             */

            logger.info(task.taskLabel() + " Creating Object");
            d1Identifier = nodeCommunications.getCnCore().create(null, d1Identifier, sciMetaStream,
                    systemMetadata);
            logger.info(task.taskLabel() + " Created Object");
        } else {
            logger.info(task.taskLabel() + " Registering SystemMetadata");
            nodeCommunications.getCnCore().registerSystemMetadata(null, d1Identifier,
                    systemMetadata);
            logger.info(task.taskLabel() + " Registered SystemMetadata");
        }
    }

    /**
     * Compares the checksum in the systemMetadata against the checksum returned
     * by an MNRead.getChecksum call to the source membernode.
     * 
     * Large objects are given a bye according to the property 'Synchronization.checksum.verify.size.bypass.threshold'
     * 
     * @param systemMetadata
     * @throws InvalidSystemMetadata
     */
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
                if (!D1TypeUtils.valueEquals(systemMetadata.getIdentifier(), null)) {
                    pid = systemMetadata.getIdentifier().getValue();
                }
                String errorMessage = "The checksum for pid: " + pid
                        + " does not match the actual checksum supplied by the member node: "
                        + task.getNodeId() + ".  Actual checksum: "
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
    
    /**
     * Validate the ResourceMap by parsing it with ResourceMapFactory if the format
     * has a way of validating.
     * 
     * @param format - cannot be null or throws NPE
     * @param reourceInputStream - cannot be null
     * @return
     * @throws UnsupportedType
     * @throws InsufficientResources 
     */
    private void validateResourceMap(ObjectFormat format, InputStream sciMetaStream) throws UnsupportedType, InsufficientResources {

        boolean attemptValidation = false;
        
        if (format != null && format.getFormatType().equalsIgnoreCase("RESOURCE"))
            for (int i=0; i< VALIDATING_RESOURCE_FORMATS.length; i++) 
                if (format.getFormatId().getValue().contentEquals(VALIDATING_RESOURCE_FORMATS[i])) 
                    attemptValidation = true;
        
        if (attemptValidation) {
            
            // first clone the inputStream so it can be used later
            byte[] resourceBytes = null;
            try {
                resourceBytes = IOUtils.toByteArray(sciMetaStream);
            } catch (IOException e) {
                throw new InsufficientResources("413",
                        "Unable to create ByteArrayInputStream for pid: "
                                + task.getPid() + " with message: "
                                + e.getMessage());
            }
            finally {
                IOUtils.closeQuietly(sciMetaStream);
            }

            if (resourceBytes == null) 
                throw new UnsupportedType("Invalid Resource Map", "input byte[] was null");


            sciMetaStream = new ByteArrayInputStream(resourceBytes);

            InputStream resourceStream = null;
            try {
                resourceStream = new ByteArrayInputStream(resourceBytes);
                ResourceMapFactory.getInstance().parseResourceMap(resourceStream);
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
    }

    /**
     * Makes sure the system metadata is valid against the schema.
     * @param sysmeta
     * @throws InvalidSystemMetadata - with pid from sysmeta if it can get it
     */
    private void validateSystemMetadata(SystemMetadata sysmeta) throws InvalidSystemMetadata {
        Exception caught = null;
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            TypeMarshaller.marshalTypeToOutputStream(sysmeta, os);
            os.close();
        } catch (JiBXException | IOException e) {
            String pid = "null";
            if (sysmeta != null && !D1TypeUtils.valueEquals(sysmeta.getIdentifier(),null)) {
                pid = sysmeta.getIdentifier().getValue();
            }
            String errorMessage = "The SystemMetadata for pid: " + pid + " is not schema valid";
            InvalidSystemMetadata be = new InvalidSystemMetadata("000", errorMessage);
            be.initCause(caught);
            logger.error(errorMessage, be);
            throw be;
        }
        finally {
            IOUtils.closeQuietly(os);
        }
    }

    /**
     * To be called if the Object is already created. Updates can have 4 final 
     * outcomes: 1) changes not being allowed, 2) no changes to apply, 
     * 3) updates applied, or 4) addition of a replica.
     * <br/>
     * The first outcome is reached when the new system metadata contains one or more
     * field values that cannot replace existing ones.  (Repica differences are
     * ignored)
     * <br/>
     * The second outcome is when there aren't any differences, and usually occurs
     * when an object is reharvested during a full-reharvest, or if an object
     * is synchronized by CN.synchronize ahead of it's scheduled harvest.
     * <br/>
     * The third outcome is considered the normal case, which happens when the 
     * authoritative Member Node updates its system metadata.
     * <br/>
     * The fourth outcome occurs when the new system metadata is coming from a 
     * replica node.  This can happen if two Member Nodes are part of a different
     * replication network (as is the case with several Metacat nodes) and the 
     * replica's copy is synchronized before the CNs learn of the replica from
     * the authoritative Member Node.
     *
     * @param SystemMetadata systemMetdata from the MN 
     * @throws InternalSyncFailure 
     * @throws BaseException 
     */
    private void updateSystemMetadata(SystemMetadata newSystemMetadata) throws BaseException, InternalSyncFailure {
        Identifier pid = new Identifier();
        pid.setValue(newSystemMetadata.getIdentifier().getValue());
        SystemMetadata cnSystemMetadata = hzSystemMetaMap.get(pid);
        
        
        validateEssentialProperties(newSystemMetadata, cnSystemMetadata);
        
        // if here, we know that the new system metadata is referring to the same
        // object, and can consider updating other values.
        
        boolean sourceIsAuthoritative = task.getNodeId().contentEquals(
                cnSystemMetadata.getAuthoritativeMemberNode().getValue());
        
        Replica newReplica = null; 
        if (!sourceIsAuthoritative) {
            newReplica = determineIfNewReplica(task.getNodeId(), cnSystemMetadata);
        }
        boolean hasChanges = validateUpdates(newSystemMetadata, cnSystemMetadata,  newReplica, sourceIsAuthoritative);

        if (hasChanges) {
            nodeCommunications.getCnReplication().updateReplicationMetadata(session, pid,
                    newReplica, cnSystemMetadata.getSerialVersion().longValue());

            auditReplicaSystemMetadata(pid);
            logger.info(task.taskLabel() + " Updated Replica");
        }
    }

    /**
     * Returns a new Replica for the given nodeId if it isn't already listed
     * in the provided systemMetadata.  Otherwise returns null;
     * 
     * @param nodeId
     * @param cnSysMeta
     * @return
     */
    private Replica determineIfNewReplica(String nodeId, SystemMetadata cnSysMeta) {
        
        // look for nodeId in the current list of replicas
        List<Replica> cnReplicaList = cnSysMeta.getReplicaList();
        for (Replica replica : cnReplicaList) {
            if (nodeId.equals(replica.getReplicaMemberNode().getValue())) {
                return null;
            }
        }
        
        Replica mnReplica = new Replica();
        NodeReference nodeReference = new NodeReference();
        nodeReference.setValue(task.getNodeId());
        mnReplica.setReplicaMemberNode(nodeReference);
        mnReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
        mnReplica.setReplicaVerified(new Date());

        return mnReplica;
    }
    
    /**
     * Goes through each property and applies any updated values to the passed-in
     * newSystemMetadata object, if allowed.
     * It throws InvalidRequest if there are any disallowed changes introduced.
     * <br/>
     * @see http://mule1.dataone.org/ArchitectureDocs-current/design/SystemMetadata.html
     * for specification on ownership and editability of properties.
     * 
     * @param newSysMeta
     * @param cnSysMeta
     * @param newReplica
     * @param sourceIsAuthoritative
     * @return
     * @throws InvalidRequest - if changes do not validate
     * @throws ServiceFailure - if an AccessPolicy or ReplicationPolicy couldn't be reserialized for comparison
     */
    private boolean validateUpdates(SystemMetadata newSysMeta, SystemMetadata cnSysMeta,
            Replica newReplica, boolean sourceIsAuthoritative)  throws InvalidRequest, ServiceFailure {
        
        // TODO: how to handle the case where the source is an already registered
        // replica with out of date values? We don't change the systemMetadata,
        // but need to trigger an audit.
        int changes = validateRestrictedFields(newSysMeta, cnSysMeta);
        if (changes > 0) {
            return true;
        }
        if (findChangesInUnrestrictedFields(newSysMeta, cnSysMeta)) {
            return true;
        }
        return false;
    }
    
    private int validateRestrictedFields(SystemMetadata newSysMeta, SystemMetadata cnSysMeta) throws InvalidRequest {

        int changes = 0;

        List<String> illegalChangeFields = new LinkedList<String>();

        // serialVersion:  CN controls it, it must be greater than or equal to the existing
        if (newSysMeta.getSerialVersion().compareTo(cnSysMeta.getSerialVersion()) < 0) {
            illegalChangeFields.add("serialVersion");
        }

        // identifier - already checked with validateEssentialProperties

        // formatId - immutable
        if (!D1TypeUtils.equals(cnSysMeta.getFormatId(), newSysMeta.getFormatId())) {
            illegalChangeFields.add("formatId");
        }
        // size - is immutable, TODO: should it be part of validateEssentialProperties?
        if (!ObjectUtils.equals(cnSysMeta.getSize(), newSysMeta.getSize())) {
            illegalChangeFields.add("size");
        }

        // checksum - immutable, already checked with validateEssentialProperties

        // submitter - immutable
        if (!D1TypeUtils.equals(cnSysMeta.getSubmitter(), newSysMeta.getSubmitter())) {
            illegalChangeFields.add("submitter");
        }

        // obsoletes:   can only be set at creation, so is effectively immutable here
        if (!D1TypeUtils.equals(cnSysMeta.getObsoletes(), newSysMeta.getObsoletes())) {
            illegalChangeFields.add("obsoletes");
        }

        // obsoletedBy:   can only go from null => a value
        if (!D1TypeUtils.equals(cnSysMeta.getObsoletedBy(), newSysMeta.getObsoletedBy())) {
            if (cnSysMeta.getObsoletedBy() == null) {
                changes++;
            } else {
                illegalChangeFields.add("obsoletedBy");
            }
        }
        
        // archived - can only go from false/null to true 
        if (!ObjectUtils.equals(cnSysMeta.getArchived(), newSysMeta.getArchived())) {
            if ((Boolean) ObjectUtils.defaultIfNull(newSysMeta.getArchived(), Boolean.FALSE)) {
                // can't go from true to false or null
                illegalChangeFields.add("archived");
            } else {
                changes++;
            }
        }
        
        // dateUploaded - immutable  //TODO: should this be part of validateEssentialProperties?
        if (!ObjectUtils.equals(cnSysMeta.getDateUploaded(), newSysMeta.getDateUploaded())) {
            illegalChangeFields.add("dateUploaded");
        }
        
        // dateSystemMetadataModified - can't be earlier than what we already have
        if (newSysMeta.getDateSysMetadataModified().before(cnSysMeta.getDateSysMetadataModified())) {
            illegalChangeFields.add("dateSystemMetdataModified");
        }


        // originMemberNode - immutable
        if (!ObjectUtils.equals(cnSysMeta.getOriginMemberNode(), newSysMeta.getOriginMemberNode())) {
            illegalChangeFields.add("originMemberNode");
        }
        // seriesId:   can only go from null => a value
        if (!D1TypeUtils.equals(cnSysMeta.getSeriesId(), newSysMeta.getSeriesId())) {
            if (cnSysMeta.getSeriesId() == null) {
                changes++;
            } else {
                illegalChangeFields.add("obsoletedBy");
            }
        }

        if (illegalChangeFields.size() > 0) {
            throw new InvalidRequest("-1", "Illegal changes attempted to the fields: "
                    + StringUtils.join(illegalChangeFields, ", "));
        }
        return changes;
    }
        
    private boolean findChangesInUnrestrictedFields(SystemMetadata newSysMeta, SystemMetadata cnSysMeta) throws ServiceFailure {
        
        // rightsHolder
        if (!D1TypeUtils.equals(cnSysMeta.getRightsHolder(), newSysMeta.getRightsHolder())) {
            return true;
        }
        
        // authoritativeMemberNode
        if (!D1TypeUtils.equals(cnSysMeta.getAuthoritativeMemberNode(), newSysMeta.getAuthoritativeMemberNode())) {
            return true;
        }
        


        // use XOR to find cases where one is null and the other is not
        if ((newSysMeta.getAccessPolicy() == null) ^ (cnSysMeta.getAccessPolicy() == null)) {
            return true;
        }
        if ((newSysMeta.getReplicationPolicy() == null) ^ (cnSysMeta.getReplicationPolicy() == null)) {
            return true;
        }

        // look for simple property changes inside the AccessPolicy and ReplicationPolicy structures

        // (because of the XOR statements above, we only have to null-check one of the copies again to 
        // make sure neither are null, and we can do property comparisons

        if (newSysMeta.getAccessPolicy() != null) {
            if (newSysMeta.getAccessPolicy().sizeAllowList() != cnSysMeta.getAccessPolicy().sizeAllowList()) {
               return true;
            }
        } 
       
        if (newSysMeta.getReplicationPolicy() != null) {
            if (newSysMeta.getReplicationPolicy().getNumberReplicas() != cnSysMeta.getReplicationPolicy().getNumberReplicas()) {
                return true;
            }
            if (newSysMeta.getReplicationPolicy().sizePreferredMemberNodeList() != cnSysMeta.getReplicationPolicy().sizePreferredMemberNodeList()) {
                return true;
            }
            if (newSysMeta.getReplicationPolicy().sizeBlockedMemberNodeList() != cnSysMeta.getReplicationPolicy().sizeBlockedMemberNodeList()) {
                return true;
            }
        }

        // try the bullet-proof way to looking for differences: serialization

        StringBuffer message = new StringBuffer();
        try {
            if(!D1TypeUtils.serializedFormEquals(newSysMeta.getAccessPolicy(), cnSysMeta.getAccessPolicy())) {
               return true; 
            }
        } catch (JiBXException | IOException e1) {
            message.append("Problems serializing one of the AccessPolicies: " + e1.getMessage());
        }
        
        try {
            if(!D1TypeUtils.serializedFormEquals(newSysMeta.getReplicationPolicy(), cnSysMeta.getReplicationPolicy())) {
               return true; 
            }
        } catch (JiBXException | IOException e1) {
            message.append(" Could not compare serialied forms of the ReplicationPolicies");
        }
        if (message.length() > 0) {
            throw new ServiceFailure("-1","Problems comparing SystemMetadata: " + message.toString());
        }
        return false;
    }
        
   
    // this might be a better solution, in the logging statements, but it's a one-off, and a bit clunky
    private boolean customSerializationCompare(SystemMetadata newSysMeta, SystemMetadata cnSysMeta) throws ServiceFailure {
        ByteArrayOutputStream newAPos = null;
        ByteArrayOutputStream cnAPos = null;
        
        int progress = 0;
        boolean exceptionEncountered = false;
        try {
            if (newSysMeta.getAccessPolicy() != null) {

                newAPos = new ByteArrayOutputStream();
                cnAPos = new ByteArrayOutputStream();

                TypeMarshaller.marshalTypeToOutputStream(newSysMeta.getAccessPolicy(), newAPos);
                progress++;
                TypeMarshaller.marshalTypeToOutputStream(cnSysMeta.getAccessPolicy(), cnAPos);

                if (!Arrays.equals(newAPos.toByteArray(), cnAPos.toByteArray())) {
                    return true;
                }
            }
        } catch (JiBXException | IOException e) {
            if (progress == 0) {
                logger.error(task.taskLabel() + "Couldn't reserialize the AccessPolicy of the MN systemMetadata!", e);
            } else {
                logger.error(task.taskLabel() + "Couldn't reserialize the AccessPolicy of the CN systemMetadata!", e);
            }
            exceptionEncountered = true;
        } finally {
            IOUtils.closeQuietly(newAPos);
            IOUtils.closeQuietly(cnAPos);
        }
        
        ByteArrayOutputStream newRPos = null;
        ByteArrayOutputStream cnRPos = null;
        
        progress = 0;
        try {
            if (newSysMeta.getReplicationPolicy() != null) {
                newRPos = new ByteArrayOutputStream();
                cnRPos = new ByteArrayOutputStream();

                TypeMarshaller.marshalTypeToOutputStream(cnSysMeta.getAccessPolicy(), cnRPos);
                progress++;
                TypeMarshaller.marshalTypeToOutputStream(newSysMeta.getAccessPolicy(), newRPos);

                if (!Arrays.equals(newRPos.toByteArray(), cnRPos.toByteArray())) {
                    return true;
                }
            }
        } catch (JiBXException | IOException e) {
            if (progress == 0) {
                logger.error(task.taskLabel() + "Couldn't reserialize the ReplicationPolicy of the CN systemMetadata!", e);
            } else {
                logger.error(task.taskLabel() + "Couldn't reserialize the ReplicationPolicy of the MN systemMetadata!", e);
            }
            exceptionEncountered = true;
        } finally {
            IOUtils.closeQuietly(newRPos);
            IOUtils.closeQuietly(cnRPos);
        }
        if (exceptionEncountered) {
            throw new ServiceFailure("-1", "Could not validate changes.  Problem reserializing one " +
            		"or more AccessPolicies or ReplicationPolicies.");
        }
        
        
        return false;
    }

//                logger.info(task.taskLabel() + " Updated ObsoletedBy");
//
////            }
//
//            // (getArchived() returns a boolean)
//            // only process the update if the new sysmeta set it to true and the 
//            // existing value is null or false.  Cannot change the value from true
//            // to false.
//            if (((newSysMeta.getArchived() != null) && newSysMeta.getArchived())
//                    && ((cnSysMeta.getArchived() == null) || !cnSysMeta.getArchived())) {
//                logger.info(task.taskLabel() + " Updating Archived...");
//                nodeCommunications.getCnCore().archive(session, pid);
//                logger.info(task.taskLabel() + " Updated Archived");
//                foundValidMNChange = true;
//            }
//            if (foundValidMNChange) {
//                
//                auditReplicaSystemMetadata(pid);
//
//            } else {
//                // TODO: refactor to assume less about how we got here and whether or not to throw an exception
//                // 
//                // a simple reharvest may lead to getting to this point, so check
//                // the sysmeta modified date before throwing an exception
//                if (newSysMeta.getDateSysMetadataModified().after(
//                        cnSysMeta.getDateSysMetadataModified())) {
//                    // something has changed, and we should probably investigate,
//                    // but for now just assume that an out-of-bounds change was attempted.
//                    InvalidRequest invalidRequest = new InvalidRequest(
//                            "567123",
//                            "Synchronization unable to process the update request. Only archived and obsoletedBy may be updated");
//                    logger.error(task.taskLabel() + "\n" + invalidRequest.serialize(invalidRequest.FMT_XML));
//                    logger.warn(task.taskLabel() + " Ignoring update from MN. Only archived and obsoletedBy may be updated");
//                    throw SyncFailedTask.createSynchronizationFailed(pid.getValue(), invalidRequest);
//
//                }
//            }
//        } else { // source of metadata is a replica node
//            boolean performUpdate = true;
            // this may be an unrecorded replica
            // membernodes may have replicas of dataone objects that were created
            // before becoming a part of dataone
//            List<Replica> prevReplicaList = cnSystemMetadata.getReplicaList();
//            for (Replica replica : prevReplicaList) {
//                if (task.getNodeId().equals(replica.getReplicaMemberNode().getValue())) {
//                    performUpdate = false;
//                    break;
//                }
//            }
//            if (performUpdate) {
//                logger.info(task.taskLabel() + " Update Replica");
//                Replica mnReplica = new Replica();
//                NodeReference nodeReference = new NodeReference();
//                nodeReference.setValue(task.getNodeId());
//                mnReplica.setReplicaMemberNode(nodeReference);
//                mnReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
//                mnReplica.setReplicaVerified(new Date());

 
//                auditReplicaSystemMetadata(pid);
//                logger.info(task.taskLabel() + " Updated Replica");
//            } else {
//
//                // attempt to determine what the MN was updating and report back with
//                // synchronizationFailed
//                InvalidRequest invalidRequest = new InvalidRequest(
//                        "567123",
//                        "Not Authorized Node to perform Updates. Synchronization unable to process the update request. Only archived and obsoletedBy may be updated from Authorized Node");
//                logger.error(task.taskLabel() + "\n" + invalidRequest.serialize(invalidRequest.FMT_XML));
//                logger.warn(task.taskLabel() + " Ignoring update from Replica MN");
//                throw SyncFailedTask.createSynchronizationFailed(pid.getValue(), invalidRequest);
//
//            }
//        }
//        // perform audit of replicas to make certain they all are at the same serialVersion level, if no update ?
//    }


    /**
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
        IMap<NodeReference, Node> hzNodes = hazelcast.getMap(hzNodesName);
        SystemMetadata cnSystemMetadata = hzSystemMetaMap.get(pid);
        if (cnSystemMetadata != null) {
            List<Replica> prevReplicaList = cnSystemMetadata.getReplicaList();
            Session session = null;
            logger.info(task.taskLabel() + " auditReplicaSystemMetadata");
            for (Replica replica : prevReplicaList) {
                Node node = hzNodes.get(replica.getReplicaMemberNode());
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
                        } catch (NodeCommUnavailable e) {
                            throw new ServiceFailure("0000", e.getMessage());
                        }

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
                    }
                }
            }
        } else {
            logger.error(task.taskLabel() + " is null when get called from Hazelcast " 
                    + hzSystemMetaMapString + " Map");
        }
    }
}