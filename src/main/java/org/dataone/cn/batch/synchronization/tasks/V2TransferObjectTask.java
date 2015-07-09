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
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.cn.batch.exceptions.NodeCommUnavailable;
import org.dataone.cn.batch.exceptions.RetryableException;
import org.dataone.cn.batch.exceptions.UnrecoverableException;
import org.dataone.cn.batch.synchronization.D1TypeUtils;
import org.dataone.cn.batch.synchronization.NodeCommSyncObjectFactory;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.batch.synchronization.type.SystemMetadataValidator;
import org.dataone.cn.hazelcast.HazelcastInstanceFactory;
import org.dataone.cn.synchronization.types.SyncObject;
import org.dataone.configuration.Settings;
import org.dataone.ore.ResourceMapFactory;
import org.dataone.service.cn.impl.v2.ReserveIdentifierService;
import org.dataone.service.cn.v2.CNRead;
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
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.util.AuthUtils;
import org.dataone.service.types.v1.util.ChecksumUtil;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.ObjectFormat;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREParserException;

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
    
    /**
     * Attepts to process the item to be synchronized.  Retry logic is implemented
     * here such that the object is requeued if the thread couldn't get the lock on
     * the pid, or the V1 MN needs to refresh its systemMetadata due to end-user 
     * changes to the systemMetadata via the v1 CN API that haven't been reflected
     * in the MN yet.
     *
     * @return null
     * @throws Exception
     *
     */
    // TODO: pro-active sleeping when we need to try again ties up the thread by 
    // sleeping 10 seconds, then putting the task at the end of the queue.
    // Maybe timestamping the last attempt then sleeping the difference, instead 
    // would be more efficient, although accounting for different CN instance 
    // clocks might make this imprecise.  Also, maybe not worth it if this is 
    // a rare event.
    @Override
    public Void call() throws Exception {
        
        Lock hzPidLock = hazelcast.getLock(task.getPid());
        boolean isLockAcquired = false;
        
        logger.info(task.taskLabel() + " Locking task, attempt " + task.getAttempt());
        try {
            // TODO: consider replacing Lock with with IMap, for the automatic GC
            // see https://groups.google.com/forum/#!topic/hazelcast/9YFGh3xwe8I
            isLockAcquired = hzPidLock.tryLock(1, TimeUnit.SECONDS); // both parameters define the wait time
            if (isLockAcquired) {
                logger.info(task.taskLabel() + " Processing SyncObject");
                SystemMetadata mnSystemMetadata = retrieveMNSystemMetadata();
                logger.info(task.taskLabel() + " MN system metadata retrieved...");
                
                try {
                    processTask(mnSystemMetadata);
                } 
                catch (VersionMismatch ex) {
                    // XXX can do the refresh-retry for V1 MNs only
                    logger.warn(task.taskLabel()
                            + " Encountered a VersionMismatch between the MN version" +
                            " of systemMetadata and the CN (Hz) version....");
                    
                    if (task.getAttempt() == 1) {
                        logger.warn(task.taskLabel() + " ... Sending systemMetadataChanged to " +
                                "all holding Member Nodes..."); 
                        notifyReplicaNodes(D1TypeBuilder.buildIdentifier(task.getPid()));
                    }

                    if (task.getAttempt() < 6) {
                        logger.warn(task.taskLabel() + " ... pausing to give MN time to refresh their" +
                                " systemMetadata, and placing back on the hzSyncObjectQueue.  Attempt " + task.getAttempt());
                        interruptableSleep(10000L);
                        hazelcast.getQueue(synchronizationObjectQueue).put(task);
                        task.setAttempt(task.getAttempt() + 1);
                    } else {
                        logger.error(task.taskLabel() + "... failed to pick up refreshed systemMetadata." +
                                " Unable to process the request.");
                    }
                }
            } else { 
                // lock-retry handling
                try {
                    if (task.getAttempt() < 100) {
                        logger.warn(task.taskLabel()
                                + " Cannot lock Pid! Requeueing the task. Attempt " + task.getAttempt());

                        task.setAttempt(task.getAttempt() + 1);
                        Thread.sleep(1000L); // to give the lock-holder time before trying again
                        hazelcast.getQueue(synchronizationObjectQueue).put(task);
                    } else {
                        logger.error(task.taskLabel()
                                + " Cannot lock Pid! Reached Max attempts (100), abandoning processing of this pid.");
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
        } finally {
            if (isLockAcquired) {
                hzPidLock.unlock();
                logger.debug(task.taskLabel() + " Unlocked Pid.");
            }
        }
        return null;
    }

    /**
     * Retrieves the SystemMetadata from the target MemberNode.  
     * @return
     * @throws SynchronizationFailed - if it cannot get valid SystemMetadata
     *         (deserialization errors, or pid doesn't match that in the request)
     */
    private SystemMetadata retrieveMNSystemMetadata() throws SynchronizationFailed 
    {
        SystemMetadata systemMetadata = null;
        try {
            systemMetadata = getSystemMetadataHandleRetry(nodeCommunications.getMnRead(), D1TypeBuilder.buildIdentifier(task.getPid()));
            logger.info(task.taskLabel() + " Retrieved SystemMetadata Identifier:"
                    + systemMetadata.getIdentifier().getValue() + " from node " + task.getNodeId()
                    + " for ObjectInfo Identifier " + task.getPid());
            if (!systemMetadata.getIdentifier().getValue().contentEquals(task.getPid())) {
                // didn't get the right SystemMetadata after all
                throw new InvalidSystemMetadata(
                        "567100",
                        String.format(
                                "Identifier in the retrieved SystemMetadata (%s) is different from "
                                        + "the identifier used to retrieve the SystemMetadata (%s).)",
                                        task.getPid(),
                                        systemMetadata.getIdentifier().getValue()
                                )
                        );
            }
        } catch (BaseException ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(BaseException.FMT_XML));
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
     * has a retry strategy implemented for NotAuthorized and ServiceFailure.
     * It also verifies that the retrieved SysMeta has a matching PID.
     * @param readImpl - the MNRead or CNRead implementation for the source sysMeta
     * @return SystemMetadata
     * @throws NotAuthorized   - retries up to 2x
     * @throws ServiceFailure  - retired up to 6x
     * @throws InvalidToken
     * @throws NotImplemented
     * @throws NotFound
     */
    private SystemMetadata getSystemMetadataHandleRetry(Object readImpl, Identifier id) throws NotAuthorized, ServiceFailure, 
    InvalidToken, NotImplemented, NotFound {
        SystemMetadata retrievedSysMeta = null;
        boolean needSystemMetadata = true;
        int tryAgain = 0;
        do {
            try {
                if (readImpl instanceof MNRead) {
                    retrievedSysMeta = ((MNRead) readImpl).getSystemMetadata(null, id);
                    needSystemMetadata = false;
                } else if (readImpl instanceof CNRead) {
                    retrievedSysMeta = ((CNRead) readImpl).getSystemMetadata(null, id);
                    needSystemMetadata = false;
                } else if (readImpl instanceof org.dataone.service.mn.tier1.v1.MNRead) {
                    org.dataone.service.types.v1.SystemMetadata oldSystemMetadata = 
                            ((org.dataone.service.mn.tier1.v1.MNRead) readImpl).getSystemMetadata(null, id);
                    needSystemMetadata = false;
                    try {
                        retrievedSysMeta = TypeMarshaller.convertTypeFromType(oldSystemMetadata,
                                SystemMetadata.class);

                    } catch (Exception e) { // catches conversion issues
                        e.printStackTrace();
                        throw new ServiceFailure("-1", "Error converting v1.SystemMetadata to v2.SystemMetadata: " + e.getMessage());
                    }
                }
            } 
            // be persistent on a couple kinds of exceptions
            catch (NotAuthorized ex) {
                if (tryAgain < 2) {
                    ++tryAgain;
                    logger.error(task.taskLabel() + ": NotAuthorized. Sleeping 5s and retrying...\n" + ex.serialize(BaseException.FMT_XML));
                    interruptableSleep(5000L);
                } else {
                    throw ex;
                }
            } catch (ServiceFailure ex) {
                if (tryAgain < 6 && needSystemMetadata) {
                    ++tryAgain;
                    logger.error(task.taskLabel() + ": ServiceFailure. Sleeping 5s and retrying...\n" + ex.serialize(BaseException.FMT_XML));
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

    
    ///////////////////////////////////////////////////////////////////////////
    
    /**
     * Chooses and initiates either the new entry, updateSystemMetadata, or 
     * the "do nothing" path.
     *
     * @param SystemMetadata systemMetdata from the MN 
     * @throws VersionMismatch
     */
    private void processTask(SystemMetadata mnSystemMetadata) throws VersionMismatch, SynchronizationFailed {
        try {
            validateSeriesId(mnSystemMetadata);
            if (alreadyExists(mnSystemMetadata)) {
                processUpdates(mnSystemMetadata);
            } else {
                // TODO: review exception handling / wrapping
                processNewObject(mnSystemMetadata);
            }
        } catch (NotAuthorized ex) {
            // from validateSeriesId and alreadyExists, this catches problems
            // when the submitter doesn't have rights on the pid or sid 
            logger.warn(task.taskLabel() + "\n" + ex.serialize(BaseException.FMT_XML));
            throw SyncFailedTask.createSynchronizationFailed(mnSystemMetadata.getIdentifier().getValue(), ex);
        } catch (VersionMismatch ex) {
            logger.warn(task.taskLabel() + "\n" + ex.serialize(BaseException.FMT_XML));
            throw ex;
        } catch (BaseException be) {
            logger.error(task.taskLabel() + "\n" + be.serialize(BaseException.FMT_XML));
            throw SyncFailedTask.createSynchronizationFailed(mnSystemMetadata.getIdentifier().getValue(), be); 
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error(task.taskLabel() + "\n" + ex.getMessage());
            throw SyncFailedTask.createSynchronizationFailed(mnSystemMetadata.getIdentifier().getValue(), ex);
        }
    }
    
    /**
     * Handles processing new objects
     * @param mnSystemMetadata
     * @throws BaseException
     * @throws SynchronizationFailed
     */
    private void processNewObject(SystemMetadata mnSystemMetadata) throws BaseException, SynchronizationFailed {
        mnSystemMetadata = updateNewSystemMetadata(mnSystemMetadata);
        if (mnSystemMetadata != null) {
            createObject(mnSystemMetadata);
        }
    }
    /**
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
    private SystemMetadata updateNewSystemMetadata(SystemMetadata systemMetadata) throws SynchronizationFailed {

        try {
            logger.debug(task.taskLabel() + " Processing SystemMetadata");
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
                        + " is not DATA");
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
            logger.error(task.taskLabel() + "\n" + ex.serialize(BaseException.FMT_XML));
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), ex);

        } catch (NotFound ex) {
            logger.error(task.taskLabel() +  "\n" + ex.serialize(BaseException.FMT_XML));
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), ex);

        } catch (NotImplemented ex) {
            logger.error(task.taskLabel() + "\n" + ex.serialize(BaseException.FMT_XML));
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), ex);

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error(task.taskLabel() + "\n this didn't work", ex);
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), ex);

        }
        return systemMetadata;
    }

    /**
     * For any sync task, (update or newObject) the submitter needs to have control
     * over the seriesId, so it makes sense to filter out seriesId problems first.
     * Problems are filtered by throwing exceptions
     * @param sysMeta
     * @throws NotAuthorized - the seriesId is new and reserved by someone else OR is in use
     *                         by another rightsHolder
     * @throws UnrecoverableException - internal problems keep this validation from completing
     */
    private void validateSeriesId(SystemMetadata sysMeta) throws NotAuthorized, UnrecoverableException {

        Identifier sid = sysMeta.getSeriesId();

        if (sid == null || StringUtils.isBlank(sid.getValue()))
            return;

        try {
            Session verifySubmitter = new Session();
            verifySubmitter.setSubject(sysMeta.getSubmitter());
            if (!reserveIdentifierService.hasReservation(verifySubmitter, sysMeta.getSubmitter(), sysMeta.getIdentifier())) {
                throw new NotAuthorized("0000","someone else (other than submitter) holds the reservation on the seriesId!");
            }
            logger.info(task.taskLabel() + " SeriesId is reserved by sysmeta.submitter");
            return;  // ok
        } catch (NotFound ex) {
            // assume if reserveIdentifierService has thrown NotFound exception SystemMetadata does not exist
            logger.info(task.taskLabel() + " SeriesId doesn't exist as reservation or object on the CN...");
            return; // ok
        } catch (IdentifierNotUnique ex) {
            logger.info(task.taskLabel() + " SeriesId is in use....");
            try {
                SystemMetadata sidSysMeta = getSystemMetadataHandleRetry(nodeCommunications.getCnRead(), sid);
                if (!AuthUtils.isAuthorized(
                        Collections.singletonList(sysMeta.getSubmitter()),
                        Permission.CHANGE_PERMISSION, 
                        sidSysMeta))
                    throw new NotAuthorized("0000","Submitter does not have CHANGE rights on the SeriesId as determined by" +
                            " the current head of the Sid collection, whose pid is: " +
                            sidSysMeta.getIdentifier().getValue());
            } catch (InvalidToken|NotImplemented|ServiceFailure e) {
                String message = " couldn't access the CN /meta endpoint to check seriesId!! Reason: " + e.toString();
                logger.error(task.taskLabel() + message);
                e.printStackTrace();
                throw new UnrecoverableException(message, e);
            } catch (NotFound e) {
                logger.info(task.taskLabel() + " SeriesId doesn't exist for any object on the CN...");
                return; //ok
            }
        }
    }

    
    /**
     * Uses the reservation service (CNCore.hasReservation) to determine if the 
     * object exists or not.  The intentional side-effect of using hasReservation
     * to determine this is that the call validates the legitimacy of the registration
     * by ensuring that submitter either matches the reservation or there is no
     * reservation.
     * @param sysMeta
     * @return - true if the object is already registered, false otherwise
     * @throws NotAuthorized - when the sysmeta.submitter does not match the owner
     * of the identifier reservation.
     */
    private boolean alreadyExists(SystemMetadata sysMeta) throws NotAuthorized {
        // use the identity manager to determine if the PID already exists or is previously
        // reserved. 
        // If the PID is already created, hasReservation throws an IdentifierNotUnique
        // this means we should go through the update logic
        // If the PID has been reserved, then either NotAuthorized will be thrown
        // indicating that the PID was reserved by another user
        // or true is returned, indicating that the subject indeed has the reservation
        //
        Boolean exists = null;
        try {
            Session verifySubmitter = new Session();
            verifySubmitter.setSubject(sysMeta.getSubmitter());
            if (!reserveIdentifierService.hasReservation(verifySubmitter, sysMeta.getSubmitter(), sysMeta.getIdentifier())) {
                throw new NotAuthorized("0000","someone else (other than submitter) holds the reservation on the pid!");
            }
            logger.info(task.taskLabel() + " Pid is reserved by sysmeta.submitter");
            exists = false;
        } catch (NotFound ex) {
            // assume if reserveIdentifierService has thrown NotFound exception SystemMetadata does not exist
            logger.info(task.taskLabel() + " Pid doesn't exist as reservation or object.");
            exists = false;
        } catch (IdentifierNotUnique ex) {
            logger.info(task.taskLabel() + " Pid Exists. Must be a systemMetadata update.");
            exists = true;
        }
        return exists;
    }


    /**
     * Create the object if a resource or sci meta object. Only register the systemmetadata 
     * if a sci data object.
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

        SystemMetadataValidator.schemaValidateSystemMetadata(systemMetadata);
        validateChecksum(systemMetadata);

        if ((objectFormat != null) && !objectFormat.getFormatType().equalsIgnoreCase("DATA")) {
            // this input stream gets used as parameter to the create call.
            InputStream sciMetaStream = null;
            try {
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

    /**
     * Compares the checksum in the systemMetadata against the checksum returned
     * by an MNRead.getChecksum call to the source Member Node.
     * 
     * Large objects are given a bye according to the property 'Synchronization.checksum.verify.size.bypass.threshold'
     * 
     * @param systemMetadata
     * @throws InvalidSystemMetadata
     */
    //  XXX reviewed 6/20
    private void validateChecksum(SystemMetadata systemMetadata) throws InvalidSystemMetadata {
        
        if (systemMetadata.getSize().compareTo(CHECKSUM_VERIFICATION_SIZE_BYPASS_THRESHOLD) > 0) 
            return;
           
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
     * @throws UnrecoverableException - for problems with the Hz systemMetadata
     * @throws SynchronizationFailed - if we get any other exception talking to the MN (validating checksum)
     * @throws RetryableException - if we get ServiceFailure talking to the MN (validating checksum)
     */
    // TODO: Check that we are not updating V2 sysmeta with V1
    private void processUpdates(SystemMetadata newSystemMetadata) 
            throws RetryableException, UnrecoverableException, SynchronizationFailed {
        //XXX is cloning the identifier necessary?
        Identifier pid = D1TypeBuilder.cloneIdentifier(newSystemMetadata.getIdentifier());
        
        logger.info(task.taskLabel() + " Processing as an Update");
        logger.info(task.taskLabel() + " Getting sysMeta from HazelCast map");
        SystemMetadata hzSystemMetadata = hzSystemMetaMap.get(pid);
        try {
            SystemMetadataValidator validator = new SystemMetadataValidator(hzSystemMetadata);
            validator.validateEssentialProperties(newSystemMetadata,nodeCommunications.getMnRead());

            // if here, we know that the new system metadata is referring to the same
            // object, and we can consider updating other values.

            if (task.getNodeId().contentEquals(
                    hzSystemMetadata.getAuthoritativeMemberNode().getValue())) 
            {
                processAuthoritativeUpdate(newSystemMetadata, hzSystemMetadata);
            } else {
                processPossibleNewReplica(newSystemMetadata, hzSystemMetadata);
            }
        } catch (InvalidSystemMetadata e) {
            throw new UnrecoverableException("In processUpdates, bad SystemMetadata from the HzMap", e);
        } catch (IdentifierNotUnique | InvalidRequest | InvalidToken | NotAuthorized | NotImplemented | NotFound e) {
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), e);
        } catch (ServiceFailure e) {
            throw new RetryableException("In processUpdates, while validating the checksum:, e");
        }
    }
    

    /**
     * checks to see if this systemMetadata is from an existing replica or is 
     * an unknown source that should be registered as a replica.
     * 
     * @param newSystemMetadata
     * @param hzSystemMetadata
     * @throws RetryableException - can requeue these
     * @throws UnrecoverableException - for problems with updateReplicationMetadata
     */
    private void processPossibleNewReplica(SystemMetadata newSystemMetadata, SystemMetadata hzSystemMetadata) 
    throws RetryableException, UnrecoverableException 
    {
        
        for (Replica replica : hzSystemMetadata.getReplicaList()) {
            if (task.getNodeId().equals(replica.getReplicaMemberNode().getValue())) {
                logger.info(task.taskLabel() + " Non-authoritative source, existing replica.  No action needed");
                return;
            }
        }
        
        Replica mnReplica = new Replica();
        mnReplica.setReplicaMemberNode(D1TypeBuilder.buildNodeReference(task.getNodeId()));
        mnReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
        mnReplica.setReplicaVerified(new Date());
        // * status can be set to completed because we verified the checksum 
        
        logger.info(task.taskLabel() + " Non-authoritative source, adding the" +
                " node as a replica");

        try {
            nodeCommunications.getCnReplication().updateReplicationMetadata(session, newSystemMetadata.getIdentifier(),
                    mnReplica, hzSystemMetadata.getSerialVersion().longValue());
        } 
        catch (NotImplemented | NotAuthorized | InvalidRequest | InvalidToken e) {
            // can't fix these and are internal configuration problems
            throw  new UnrecoverableException("in processPossibleNewReplica: ", e);
        } 
        catch (ServiceFailure | NotFound | VersionMismatch e) {
            // these might resolve if we requeue?
            throw new RetryableException("from processPossibleNewReplica: ", e);
        }
    }

    /**
     * Validate the new system metadata against the existing and propagate any
     * updates as needed (to CN storage, to MN replica nodes)
     * @param mnSystemMetadata
     * @param hzSystemMetadata
     * @throws RetryableException
     * @throws UnrecoverableException 
     * @throws SynchronizationFailed
     */
    private void processAuthoritativeUpdate(SystemMetadata mnSystemMetadata, SystemMetadata hzSystemMetadata) 
    throws RetryableException, UnrecoverableException, SynchronizationFailed {
        
        boolean validated = false;
        try {
            SystemMetadataValidator validator = new SystemMetadataValidator(hzSystemMetadata);
            if (validator.hasValidUpdates(mnSystemMetadata)) {
                Identifier pid = mnSystemMetadata.getIdentifier();
                validated = true;
                // persist the new systemMetadata
                nodeCommunications.getCnCore().updateSystemMetadata(session, pid, mnSystemMetadata);
                // propagate the changes
                notifyReplicaNodes(pid);
                logger.info(task.taskLabel() + " Update with new SystemMetadata");
            } else {
                logger.info(task.taskLabel() + " No changes to update.");
            }
        } catch (ServiceFailure e) {
            if (validated)
                throw new RetryableException("from processAuthoritativeUpdate: ", e);
            else
                throw new UnrecoverableException("from processAuthoritativeUpdate: ", e);
            
        } catch (InvalidRequest e) {
            if (validated)
                throw new UnrecoverableException("from processAuthoritativeUpdate: ", e);
            else
                throw SyncFailedTask.createSynchronizationFailed(task.getPid(), e);
            
        } catch (NotFound e) {
            // TODO should we switch to trying registerSystemMetadata?
            throw new UnrecoverableException("from processAuthoritativeUpdate: ", e);
            
        } catch (NotImplemented|NotAuthorized|InvalidToken|InvalidSystemMetadata e) {
            throw new UnrecoverableException("from processAuthoritativeUpdate: ", e);
        }
    }


    /**
     * Inform Member Nodes that may have a copy to refresh their version of the system metadata
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
    private void notifyReplicaNodes(Identifier pid) throws InvalidToken, ServiceFailure,
            NotAuthorized, NotFound, InvalidRequest, NotImplemented {

        logger.info(task.taskLabel() + " Entering notifyReplicaNodes...");
        SystemMetadata cnSystemMetadata = hzSystemMetaMap.get(pid);
        if (cnSystemMetadata != null) {
            List<Replica> prevReplicaList = cnSystemMetadata.getReplicaList();

            for (Replica replica : prevReplicaList) {
                notifyReplicaNode(cnSystemMetadata, replica.getReplicaMemberNode());
            }
        } else {
            logger.error(task.taskLabel() + " is null when get called from Hazelcast " 
                    + hzSystemMetaMapString + " Map");
        }
    }
    
    /**
     * Notifies a single Member Node that the systemMetadata has changed.  Some
     * logic is included to avoid sending the call to nodes that cannot do anything
     * with the notification.  
     * (will only send to nodes that implement MNStorage)
     * @param cnSystemMetadata
     * @param nodeId
     * @throws InvalidToken
     * @throws NotAuthorized
     * @throws NotImplemented
     * @throws ServiceFailure
     * @throws NotFound
     * @throws InvalidRequest
     */
    // TODO: review why only Tier 3 nodes are sent the notification
    private void notifyReplicaNode(SystemMetadata cnSystemMetadata, NodeReference nodeId) 
            throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound, InvalidRequest {
        
        IMap<NodeReference, Node> hzNodes = hazelcast.getMap(hzNodesName);
        Node node = hzNodes.get(nodeId);
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
                logger.info(task.taskLabel() + " Notified " + nodeId.getValue());
            }
        }
    }
}