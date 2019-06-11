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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.itk.D1Client;
import org.dataone.cn.batch.exceptions.NodeCommUnavailable;
import org.dataone.cn.batch.exceptions.RetryableException;
import org.dataone.cn.batch.exceptions.UnrecoverableException;
import org.dataone.cn.batch.synchronization.D1TypeUtils;
import org.dataone.cn.batch.synchronization.NodeCommSyncObjectFactory;
import org.dataone.cn.batch.service.v2.IdentifierReservationQueryService;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.batch.synchronization.type.NodeCommState;
import org.dataone.cn.batch.synchronization.type.SyncObjectState;
import org.dataone.cn.batch.synchronization.type.SystemMetadataValidator;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.log.MetricEvent;
import org.dataone.cn.log.MetricLogClientFactory;
import org.dataone.cn.log.MetricLogEntry;
import org.dataone.cn.synchronization.types.SyncObject;
import org.dataone.configuration.Settings;
import org.dataone.ore.ResourceMapFactory;
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
import org.dataone.service.types.v1.Group;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectLocationList;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SubjectInfo;
import org.dataone.service.types.v1.util.ChecksumUtil;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;
import org.dataone.service.types.v2.ObjectFormat;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.types.v2.TypeFactory;
import org.dataone.service.types.v2.util.AuthUtils;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREParserException;

import com.hazelcast.client.HazelcastClient;
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
public class V2TransferObjectTask implements Callable<SyncObjectState> {

    private static final BigInteger CHECKSUM_VERIFICATION_SIZE_BYPASS_THRESHOLD
            = Settings.getConfiguration().getBigInteger(
                    "Synchronization.checksum.verify.size.bypass.threshold",
                    BigInteger.valueOf(10000000));

    private static final String[] VALIDATING_RESOURCE_FORMATS = new String[]{
        "http://www.openarchives.org/ore/terms"
    };

    static final Logger logger = Logger.getLogger(V2TransferObjectTask.class);
    private NodeComm nodeCommunications;
    private SyncObject task;
    private Session session = null;  // null defaults to configured certificate
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
    IdentifierReservationQueryService identifierReservationService;
    
    
    public V2TransferObjectTask(NodeComm nodeCommunications, SyncObject task) {
        this.nodeCommunications = nodeCommunications;
        this.task = task;
        this.hzSystemMetaMap = hzStorageClient.getMap(hzSystemMetaMapString);
        this.identifierReservationService = nodeCommunications.getReserveIdentifierService();
    }

    /**
     * A constructor that allows the caller to pass in the HazelcastProcessingInstance (a Member instance), and client
     * Session.
     *
     * Not intended for use in production.
     *
     * @param nodeCommunications
     * @param task
     * @param testHzProcessingInstance - a testHzProcessingInstance
     * @param clientSession - the session object to use when making client calls
     */
    V2TransferObjectTask(NodeComm nodeCommunications, SyncObject task, Session clientSession) {
        this.nodeCommunications = nodeCommunications;
        this.task = task;
        this.hzSystemMetaMap = hzStorageClient.getMap(hzSystemMetaMapString);
        this.identifierReservationService = nodeCommunications.getReserveIdentifierService();
        this.session = clientSession;
    }

    /**
     * Attempts to process the item to be synchronized. Retry logic is implemented here 
     * such that the object is requeued for whatever reason.  
     * (failure of the thread to get a lock on the PID,  sync waiting for v1 MN
     * to incorporate sysmeta updates made via v1 CN API, or client timeouts)
     * 
     * @return null
     * @throws Exception
     *
     */
    @Override
    public SyncObjectState call()  {

        SyncObjectState callState = SyncObjectState.STARTED;
        
        
        long remainingSleep = task.getSleepUntil() - System.currentTimeMillis();
        if (remainingSleep > 0) {
            try {
                Thread.sleep(remainingSleep);
            } catch (InterruptedException ex) {
                logger.warn(buildStandardLogMessage(null, " Interrupted task-remaining-sleep"));
            }
        }

        // TODO: consider replacing Lock with with IMap, for the automatic GC
        // see https://groups.google.com/forum/#!topic/hzProcessingClient/9YFGh3xwe8I
        Lock hzPidLock = hzProcessingClient.getLock(task.getPid());
        
        // this section must be thread-safe otherwise, we may skip unlocking a lock.
        // see http://docs.hazelcast.org/docs/3.5/manual/html/lock.html
        try {
            logger.info(buildStandardLogMessage(null, " Locking task, attempt " + task.getLockAttempt()));
            
            if (hzPidLock.tryLock(1, TimeUnit.SECONDS)) {
                // got lock
                try {
                    logger.info(buildStandardLogMessage(null,  " Processing SyncObject"));
                    SystemMetadata mnSystemMetadata = retrieveMNSystemMetadata();
                    logger.debug(task.taskLabel() + " MN system metadata retrieved...");

                    processTask(mnSystemMetadata);
                    callState = SyncObjectState.SUCCESS;
                    
                } catch (RetryableException ex) {
                    if (task.getAttempt() < 20) {
                        callState = SyncObjectState.RETRY;
                        
                        logger.warn(buildStandardLogMessage(ex, " RetryableException raised on attempt "
                                + task.getAttempt() + " of 20.  Sleeping and requeueing."));

                        task.setAttempt(task.getAttempt() + 1);
                        task.setSleepUntil(System.currentTimeMillis() + 5000L);

                        hzProcessingClient.getQueue(synchronizationObjectQueue).put(task);
                        
                    } else {
                        logger.error(buildStandardLogMessage(ex," Exceeded retry limit."
                                + " Unable to process the SyncObject. Converting to UnrecoverableException"),ex);
                        throw new UnrecoverableException(task.getPid() + ": retry limits reached without success.",
                                ex.getCause());
                    }
                } finally {
                    hzPidLock.unlock();
                    logger.info(buildStandardLogMessage(null,  " Unlocked Pid."));
                }
            } else {
                // lock-retry handling
                if (task.getLockAttempt() < 100) {
                    callState = SyncObjectState.RETRY;
                    
                    logger.warn(buildStandardLogMessage(null,
                            " Cannot lock Pid! Requeueing the task. Attempt " + task.getLockAttempt()));

                    task.setLockAttempt(task.getLockAttempt() + 1);
                    task.setSleepUntil(System.currentTimeMillis() + 1000L);
                    hzProcessingClient.getQueue(synchronizationObjectQueue).put(task);
                    
                } else {
                    callState = SyncObjectState.FAILED;
                    
                    String message = "Cannot lock Pid! Reached Max attempts (100), abandoning processing of this pid.";
                    logger.error(buildStandardLogMessage(null, message));
                    throw new SynchronizationFailed("5000",message);
                }
            }
        } catch (SynchronizationFailed e) {
            callState = SyncObjectState.FAILED;
            
            logger.error(buildStandardLogMessage(e.getCause(),"SynchronizationFailed: " + e.getMessage()),e);
            SyncFailedTask syncFailedTask = new SyncFailedTask(nodeCommunications, task);
            syncFailedTask.submitSynchronizationFailed(e);

        } catch (UnrecoverableException e) {
            callState = SyncObjectState.FAILED;
            // this is the proper location to decide whether or not to notify the MemberNode
            // (these are exceptions caused by internal problems)
            
            // in all cases, we need to log
            logger.error(buildStandardLogMessage(e.getCause(),"UnrecoverableException: " + e.getMessage()),e);
            
            // report to MN, for now

            SyncFailedTask syncFailedTask = new SyncFailedTask(nodeCommunications, task);
            if (e.getCause() instanceof BaseException)
                syncFailedTask.submitSynchronizationFailed(task.getPid(), null,(BaseException) e.getCause());
            
            else 
                syncFailedTask.submitSynchronizationFailed(task.getPid(), null,
                        new ServiceFailure("5000", this.buildStandardLogMessage(e.getCause(), null)));

        
        } catch (InterruptedException e) {
            callState = SyncObjectState.FAILED;
            // was handled as Exception before I split this out
            // don't know if we need to handle it any differently,
            logger.error(buildStandardLogMessage(e, "Interrupted: " + e.getMessage()), e);

        } catch (Exception e) {
            callState = SyncObjectState.FAILED;
            logger.error(this.buildStandardLogMessage(e,e.getMessage()),e);

        } finally {
            logger.info(buildStandardLogMessage(null, " exiting with callState: " + callState));
            MetricLogEntry metricLogEntry = new MetricLogEntry(MetricEvent.SYNCHRONIZATION_TASK_EXECUTION);
            metricLogEntry.setNodeId(TypeFactory.buildNodeReference(task.getNodeId()));
            metricLogEntry.setPid(TypeFactory.buildIdentifier(task.getPid()));
            metricLogEntry.setMessage("status=" + callState);
            MetricLogClientFactory.getMetricLogClient().logMetricEvent(metricLogEntry);
        }
        return callState;
    }

    /**
     * Retrieves the SystemMetadata from the target MemberNode.
     *
     * @return
     * @throws SynchronizationFailed - if it cannot get valid SystemMetadata (deserialization errors, or pid doesn't
     * match that in the request)
     * @throws RetryableException 
     */
    private SystemMetadata retrieveMNSystemMetadata() throws SynchronizationFailed, RetryableException {
        SystemMetadata systemMetadata = null;
        try {
            systemMetadata = getSystemMetadataHandleRetry(nodeCommunications.getMnRead(), D1TypeBuilder.buildIdentifier(task.getPid()));
            logger.info(buildStandardLogMessage(null, " Retrieved SystemMetadata Identifier:"
                    + systemMetadata.getIdentifier().getValue() + " from node " + task.getNodeId()
                    + " for ObjectInfo Identifier " + task.getPid()));
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
            logger.error(buildStandardLogMessage(ex, ex.getDescription()),ex);
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), null, ex);

//        } catch (RetryableException ex) {
//            throw ex;
//        } catch (Exception ex) {
//            ex.printStackTrace();
//            logger.error(task.taskLabel() + "\n this didn't work", ex);
//            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), ex);
        }
        return systemMetadata;
    }

    /**
     * Tries to get the SystemMetadata from the node specified in the task. It has a retry strategy implemented for
     * NotAuthorized and ServiceFailure. It also verifies that the retrieved SysMeta has a matching PID.
     *
     * @param readImpl - the MNRead or CNRead implementation for the source sysMeta
     * @return SystemMetadata
     * @throws NotAuthorized - retries up to 2x
     * @throws ServiceFailure - retries up to 6x
     * @throws InvalidToken
     * @throws NotImplemented
     * @throws NotFound
     * @throws RetryableException 
     */
    private SystemMetadata getSystemMetadataHandleRetry(Object readImpl, Identifier id) throws ServiceFailure,
            InvalidToken, NotImplemented, NotFound, RetryableException {
        SystemMetadata retrievedSysMeta = null;

            try {
                if (readImpl instanceof MNRead) {
                    retrievedSysMeta = ((MNRead) readImpl).getSystemMetadata(session, id);

                } else if (readImpl instanceof CNRead) {
                    retrievedSysMeta = ((CNRead) readImpl).getSystemMetadata(session, id);

                } else if (readImpl instanceof org.dataone.service.mn.tier1.v1.MNRead) {
                    org.dataone.service.types.v1.SystemMetadata oldSystemMetadata
                            = ((org.dataone.service.mn.tier1.v1.MNRead) readImpl).getSystemMetadata(session, id);
                    try {
                        retrievedSysMeta = TypeFactory.convertTypeFromType(oldSystemMetadata, SystemMetadata.class);
                    } catch (Exception e) { // catches conversion issues
                        e.printStackTrace();
                        throw new ServiceFailure("-1:conversionError", "Error converting v1.SystemMetadata to v2.SystemMetadata: " + e.getMessage());
                    }
                }
            } // be persistent on a couple kinds of exceptions
            catch (NotAuthorized ex) {
                throw new RetryableException("in getSystemMetadata, got NotAuthorized: " +
                        ex.getDescription(), ex);
            } 
            catch (ServiceFailure ex) {
                if (ex.getDetail_code() != null && ex.getDetail_code().equals("-1:conversionError")) {
                    throw ex;
                }
                throw new RetryableException("in getSystemMetadata, got ServiceFailure: " +
                        ex.getDescription(), ex);
            }
        return retrievedSysMeta;
    }

//    private void interruptableSleep(long millis) {
//        try {
//            Thread.sleep(millis);
//        } catch (InterruptedException ex) {
//            logger.warn(task.taskLabel() + "\n" + ex);
//        }
//    }

    ///////////////////////////////////////////////////////////////////////////
    /**
     * Chooses and initiates either the new entry, updateSystemMetadata, or the "do nothing" path.
     *
     * @param SystemMetadata systemMetdata from the MN
     * @throws VersionMismatch
     * @throws RetryableException 
     */
    private void processTask(SystemMetadata mnSystemMetadata) throws SynchronizationFailed, UnrecoverableException, RetryableException {
        logger.debug(task.taskLabel() + " entering processTask...");
        try {
            // this may be redundant, but it's good to start 
            // off with an explicit check that we got the systemMetadata
            if (mnSystemMetadata == null) 
                throw new UnrecoverableException(task.getPid() + 
                        "the retrieved SystemMetadata passed into processTask was null!");
            
            if (resolvable(mnSystemMetadata.getIdentifier(), "PID")) {
                processUpdates(mnSystemMetadata);
            } 
            else {
                // not resolvable, so process as new object
                processNewObject(mnSystemMetadata);
            }
        } catch (RetryableException ex) {
            logger.warn(buildStandardLogMessage(ex,"RetryableException "));
            throw ex;
        }
    }

    /**
     * Handles processing new objects
     *
     * @param mnSystemMetadata - expects non-Null
     * @throws BaseException
     * @throws SynchronizationFailed
     * @throws RetryableException 
     * @throws ServiceFailure 
     * @throws InvalidRequest 
     * @throws NotAuthorized 
     * @throws InvalidSystemMetadata 
     * @throws UnsupportedType 
     * @throws IdentifierNotUnique 
     * @throws InvalidToken 
     * @throws NotImplemented 
     * @throws InsufficientResources 
     * @throws NotFound 
     * @throws UnrecoverableException 
     */
    private void processNewObject(SystemMetadata mnSystemMetadata) throws SynchronizationFailed, 
    RetryableException, UnrecoverableException {

        // TODO: should new objects from replica nodes process or throw SyncFailed?
        // as per V1 logic (TransferObjectTask), this class currently does not check
        // the authoritativeMN field and allows the object to sync.
        logger.debug(task.taskLabel() + " entering processNewObject...");
       
        
        try {
        	validateSeriesId(mnSystemMetadata, null); 
        } catch (NotAuthorized e) {
            logger.error(buildStandardLogMessage(e, "NotAuthorized to claim the seriesId"), e);
            throw SyncFailedTask.createSynchronizationFailed(mnSystemMetadata.getIdentifier().getValue(),
                    "NotAuthorized to claim the seriesId", e);
        }
       
        try {

            // make sure the pid is not reserved by anyone else
            // not going through TLS, so need to build a Session, and will use the submitter
            // in the systemMetadata, since they should have access to their own reservation(?)
            Session verifySubmitter = new Session();
            verifySubmitter.setSubject(mnSystemMetadata.getSubmitter());
            identifierReservationService.hasReservation(verifySubmitter, mnSystemMetadata.getSubmitter(), mnSystemMetadata.getIdentifier());
            logger.debug(task.taskLabel() + " Pid is reserved by this object's submitter.");

        } catch (NotFound e) {
            logger.debug(task.taskLabel() + " Pid is not reserved by anyone.");
            // ok to continue...
        
        } catch (NotAuthorized | InvalidRequest e) {
            throw new UnrecoverableException(task.getPid() + " - from hasReservation",e);
        
        } catch (ServiceFailure e) {
            extractRetryableException(e);
            throw new UnrecoverableException(task.getPid() + " - from hasReservation",e);
        }

        mnSystemMetadata = populateInitialReplicaList(mnSystemMetadata);
        mnSystemMetadata.setSerialVersion(BigInteger.ONE);
        try {
            SystemMetadataValidator.validateCNRequiredNonNullFields(mnSystemMetadata);

            createObject(mnSystemMetadata);
        } catch (InvalidSystemMetadata e) {
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), null, e);
        }
        finally{};
    }


    /**
     * overwrites existing ReplicaList with a list consisting of the source Node and the CN (if not a DATA object)
     *
     * @param systemMetadata
     * @return
     * @throws SynchronizationFailed
     * @throws RetryableException 
     * @throws UnrecoverableException 
     */
    private SystemMetadata populateInitialReplicaList(SystemMetadata systemMetadata) throws SynchronizationFailed, RetryableException, UnrecoverableException {

        try {
            logger.debug(task.taskLabel() + " entering populateInitialReplicaList");
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
                if (logger.isDebugEnabled()) 
                    logger.debug(task.taskLabel()
                        + " Added CN as replica because formatType " + objectFormat.getFormatType()
                        + " is not DATA");
            }

        } catch (ServiceFailure ex) {
            logger.error(buildStandardLogMessage(ex, "serviceFailure while finding format:" + ex.getDescription()),ex);
            V2TransferObjectTask.extractRetryableException(ex);
            throw new UnrecoverableException(task.getPid() + " - from cn.getFormat", ex);

        } catch (NotFound ex) {
            logger.error(buildStandardLogMessage(ex, "format NotFound: " + ex.getDescription()),ex);
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), "cn.Core could not find the format",  ex);

        } catch (NotImplemented ex) {
            logger.error(buildStandardLogMessage(ex, ex.getDescription()),ex);
            throw new UnrecoverableException(task.getPid() + " - Unexpectedly, cn.getFormat returned NotImplemented!!", ex);

        } catch (Exception ex) {
            logger.error(buildStandardLogMessage(ex, ex.getMessage()),ex);
            throw new UnrecoverableException(task.getPid() + " - from cn.getFormat", ex);

        }
        return systemMetadata;
    }

    /**
     * For any sync task where the seriesId field is being set (any newObject task, and certain update tasks),
     * we need to disallow collisions by different users and different series. Because synchronization order
     * cannot be guaranteed, we can't rely on the obsoletes relationship, and so the checks will necessarily
     * be incomplete.  See method source code for specific rules. 
     * (see https://redmine.dataone.org/issues/7948)
     * 
     * @param sysMeta - requires non-null systemMetadata object
     * @throws NotAuthorized - the seriesId is new and reserved by someone else OR is in use by another rightsHolder
     * @throws UnrecoverableException - internal problems keep this validation from completing
     * @throws RetryableException 
     */
    private void validateSeriesId(SystemMetadata sysMeta, SystemMetadata previousSysMeta) throws NotAuthorized, UnrecoverableException, RetryableException {

        logger.debug(task.taskLabel() + " entering validateSeriesId...");
        
        // return early in cases where the seriesId value isn't changing   
        // or there's no sid to check
        Identifier sid = sysMeta.getSeriesId();
        Identifier prevSid = previousSysMeta == null ? null : previousSysMeta.getSeriesId();        
        
        if (D1TypeUtils.emptyEquals(sid, prevSid)) {
        	// nothing changed
            return;
        }
        
        if (D1TypeUtils.emptyEquals(sid, null)) {
        	// seriesId not being set, so nothing to validate.
            return;
        }
        

        // note, we don't check for the validity of removing a seriesId during a
        // systemMetadata update.  Those types of mutability rules are checked by 
        // SystemMetadataValidator.
        
        try { // resolving the SID

            Identifier headPid = resolve(sid,"SID");

            if (logger.isDebugEnabled()) 
                logger.debug(task.taskLabel() + " SeriesId is in use by " + headPid.getValue());

            SystemMetadata sidHeadSysMeta = this.hzSystemMetaMap.get(headPid);
            if (sysMeta != null && sidHeadSysMeta != null) {
                //Come from the same node, we trust them
                if(sysMeta.getAuthoritativeMemberNode() != null && sidHeadSysMeta.getAuthoritativeMemberNode() != null && 
                        D1TypeUtils.equals(sysMeta.getAuthoritativeMemberNode(), sidHeadSysMeta.getAuthoritativeMemberNode())) {
                    return;
                }
                //If rights holders  are still same, we trust it
                if(sysMeta.getRightsHolder() != null && sidHeadSysMeta.getRightsHolder() != null && D1TypeUtils.equals(sysMeta.getRightsHolder(), sidHeadSysMeta.getRightsHolder())) {
                    return;
                }
                //If submitters are still same, we trust it
                if (sysMeta.getSubmitter() != null && sidHeadSysMeta.getSubmitter() != null && D1TypeUtils.equals(sysMeta.getSubmitter(), sidHeadSysMeta.getSubmitter())) {
                    return;
                }
            
                // checking that the new object's submitter is authorized via SID head's accessPolicy
                //Get submitter's all subjects - groups and equivalent subjects.
                CNode cn = D1Client.getCN(); // TODO We should use the LDAP calls to get the identity service in order to get rid of API calls.
                SubjectInfo submitterInfo = cn.getSubjectInfo(null, sysMeta.getSubmitter());
                HashSet<Subject> submitterAllSubjects = new HashSet<Subject>();
                AuthUtils.findPersonsSubjects(submitterAllSubjects, submitterInfo, sysMeta.getSubmitter());
                if (!AuthUtils.isAuthorized(
                        submitterAllSubjects,
                        Permission.CHANGE_PERMISSION,
                        sidHeadSysMeta)) {
    
                    if (logger.isDebugEnabled()) 
                        logger.debug("Submitter doesn't have the change permission on the pid "
                                + headPid.getValue() + ". We will try if the rights holder has the permission.");
                    
                    if(!sysMeta.getSubmitter().equals(sysMeta.getRightsHolder())) {
                        SubjectInfo rightsHolderInfo = cn.getSubjectInfo(null, sysMeta.getRightsHolder());
                        Set<Subject> rightsHolderAllSubjects = findEquivalentSubjects(rightsHolderInfo, sysMeta.getRightsHolder());
                        if(!AuthUtils.isAuthorized(
                            rightsHolderAllSubjects,
                            Permission.CHANGE_PERMISSION,
                            sidHeadSysMeta)) {
                            throw new NotAuthorized("0000", "Neither the submitter nor rightsHolder have CHANGE rights on the SeriesId as determined by"
                                    + " the current head of the Sid collection, whose pid is: " + headPid.getValue());
                        }
                    } else {
                        throw new NotAuthorized("0000", "Neither the submitter nor rightsHolder have CHANGE rights on the SeriesId as determined by"
                                + " the current head of the Sid collection, whose pid is: " + headPid.getValue());
                    }
                }
            }
            // Note: we don't check to see that if the current head of the series
            // is obsoleted, that it is not obsoleted by a PID with a different SID,
            // because we leave this to the CN.storage component.
        } catch (ServiceFailure e) {
            extractRetryableException(e);
            String message = " couldn't access the CN /meta endpoint to check seriesId!! Reason: " + e.getDescription();

            logger.error(buildStandardLogMessage(e, message), e);
            throw new UnrecoverableException(message, e);
            
        } catch (InvalidToken | NotImplemented e) {
            String message = " couldn't access the CN /meta endpoint to check seriesId!! Reason: " + e.getDescription();
            logger.error(buildStandardLogMessage(e, message), e);
            throw new UnrecoverableException(message, e);
        
        } catch (NotFound e) {
            if (logger.isDebugEnabled()) 
                logger.debug(task.taskLabel() + String.format(
                        " SeriesId (%s) doesn't exist for any object on the CN, checking reservation service...", 
                        sid.getValue()));
            
            try { 
                Session verifySubmitter = new Session();
                verifySubmitter.setSubject(sysMeta.getSubmitter());
                if (!identifierReservationService.hasReservation(verifySubmitter, sysMeta.getSubmitter(), sid)) {
                    throw new NotAuthorized("0000", "someone else (other than submitter) holds the reservation on the seriesId! " 
                            + sid.getValue());
                }
                logger.debug(task.taskLabel() + " OK. SeriesId is reserved by this object's submitter or equivalent ID");
                return;
            } catch (NotFound e1) {
                logger.debug(task.taskLabel() + " OK. SeriesId is not reserved.");
            } catch (InvalidRequest e1) {
                String message = " Identifier Reservation Service threw unexpected InvalidRequest!! Reason: " + e1.getDescription();

                logger.error(buildStandardLogMessage(e, message), e1);
                throw new UnrecoverableException(message, e1);
            } catch (ServiceFailure e1) {
                extractRetryableException(e1);
                String message = " Identifier Reservation Service threw unexpected ServiceFailure!! Reason: " + e1.getDescription();
                logger.error(buildStandardLogMessage(e, message), e1);
                throw new UnrecoverableException(message, e1);
            }
        }
    }
    
    /**
     * Look for the equivalent authorization subjects for a given subject which can be either a person or a group.
     * For a given person subject, it will returns the person itself, all equivalent identities and all groups (include the child groups)
     * that itself and equivalent identities belong to.
     * For a given group subject, it will returns the group itself and its recursive all ancestor groups.
     * @param subjectInfo  the subject info for the given subject. 
     * @param targetSubject  the subject needs to be looked
     * @return the set of equivalent authorization subjects
     */
    private static Set<Subject> findEquivalentSubjects(SubjectInfo subjectInfo, Subject targetSubject) {
        Set<Subject> subjects = new HashSet<Subject>();
        //first to try if this is a person 
        AuthUtils.findPersonsSubjects(subjects, subjectInfo, targetSubject);
        if(subjects.isEmpty() || subjects.size() == 1) {
            //the return subjects from looking persons is o or 1. This means it can be group or a person without any groups.
            //let's try the group
            findEquivalentSubjectsForGroup(subjects, subjectInfo, targetSubject);
        }
        return subjects;
    }
    
    /**
     * Find all subjects which can be authorized as same as the given group subject (target). It is equivalent 
     * to the findPersonsSubjects method except that the target is a group subject.
     * For a given group subject, its all equivalent subjects are itself and its recursive all ancestor groups.
     * For example, group a is the parent group b, b is the parent group c and c is the parent of group d. If the given group project is c,
     * the foundSubjects should be c, b and a. 
     * @param foundSubjects the set which holds all subjects which can be authorized as same as the given group subject (target). It likes
     *                       the return value in the method.
     * @param subjectInfo the subject info object for the given group subject. 
     * @param targetGrouupSubject the group subject needs to be looked
     */
    private static void findEquivalentSubjectsForGroup(Set<Subject> foundSubjects, SubjectInfo subjectInfo, Subject targetGroupSubject) {
        if (targetGroupSubject != null && targetGroupSubject.getValue() != null && !targetGroupSubject.getValue().trim().equals("")) {
                //first, add it self
                foundSubjects.add(targetGroupSubject);
                // setting this up for subsequent searches in the loop
            List<Group> groupList = null;
                if(subjectInfo != null) {
                    groupList = subjectInfo.getGroupList();
                    if (groupList != null) {
                        for(Group group : groupList) {
                            if(group != null && group.getHasMemberList() != null && group.getSubject() != null) {
                                for(Subject subject : group.getHasMemberList()) {
                                    if(subject.getValue() != null && subject.getValue().equals(targetGroupSubject.getValue())) {
                                        //find the target group is in the hasMemberList. We need to put the parent group into the vector
                                        if(foundSubjects.contains(group.getSubject())) {
                                            //this means it is circular group. We should break the loop
                                            //System.out.println("in the loop, we need to break it =======================");
                                            return;
                                        } else {
                                            foundSubjects.add(group.getSubject());
                                            //recursive to find the parent groups
                                            findEquivalentSubjectsForGroup(foundSubjects, subjectInfo, group.getSubject());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
        }
        
    }

    /**
     * determines whether or not the identifier already exists, either as a 
     * persistent or series ID (PID or SID).
     * @param id - the Identifier
     * @param field - either PID or SID, used in logging statements for adding clarity
     * @return - true if the Identifier resolves, false otherwise
     * @throws UnrecoverableException - for configuration issues of various types
     * @throws RetryableException 
     */
    private boolean resolvable(Identifier id, String field) throws UnrecoverableException, RetryableException {
        try {
            resolve(id,field);
            return true;
        } catch (NotFound e) {
            return false;
        }
    }
    
    /**
     * resolve the identifier and return the Head pid
     * @param id - the Identifier
     * @param field - either PID or SID, used in logging statements for added clarity
     * @return - the head pid if the Identifier is a SID, the same PID otherwise
     * @throws NotFound - if the Identifier could not be resolved
     * @throws UnrecoverableException - for configuration issues for various types
     * @throws RetryableException 
     */
    private Identifier resolve(Identifier id, String field) throws NotFound, UnrecoverableException, RetryableException {
        
        logger.debug(task.taskLabel() + " entering resolve...");
        try {
            ObjectLocationList oll = ((CNRead)nodeCommunications.getCnRead()).resolve(session, id);
            if (logger.isDebugEnabled()) 
                logger.debug(task.taskLabel() + String.format(" %s %s exists on the CN.", field, id.getValue()));
            return oll.getIdentifier();

        } catch (NotFound ex) {
            // assume if identifierReservationService has thrown NotFound exception SystemMetadata does not exist
            if (logger.isDebugEnabled())  
                logger.debug(task.taskLabel() + String.format(" %s %s does not exist on the CN.", field, id.getValue()));
            throw ex;
            
        } catch (ServiceFailure e) {
            V2TransferObjectTask.extractRetryableException(e);
            throw new UnrecoverableException("Unexpected Exception from CN /resolve !" + e.getDescription(), e);

        } catch ( NotImplemented | InvalidToken | NotAuthorized e) {
            throw new UnrecoverableException("Unexpected Exception from CN /resolve ! " + e.getDescription(), e);
        }
    }

    /**
     * Create the object if a resource or sci meta object. Only register the systemmetadata if a sci data object.
     *
     *
     * @param SystemMetadata systemMetdata from the MN, expects non-null value
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
    //TODO: review sleep and exception handling
    private void createObject(SystemMetadata systemMetadata) throws RetryableException, UnrecoverableException, SynchronizationFailed {

        logger.info(buildStandardLogMessage(null, "Start CreateObject"));

        Identifier d1Identifier = new Identifier();
        d1Identifier.setValue(systemMetadata.getIdentifier().getValue());

        // All though this should take place when the object is processed, it needs to be
        // performed here due to the way last DateSysMetadataModified is used to
        // determine the next batch of records to retrieve from a MemberNode
        // 9-2-2015: rnahf: I don't think we should be bumping the modification date
        // in v2 (especially), and since we now can deactivate sync processing,
        // the bump might be a very big bump forward in time.
        
        // systemMetadata.setDateSysMetadataModified(new Date());
//
        try {       
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
                                sciMetaStream = ((MNRead) mnRead).get(session, systemMetadata.getIdentifier());
                                needSciMetadata = false;
                            } else if (mnRead instanceof org.dataone.service.mn.tier1.v1.MNRead) {
                                sciMetaStream = ((org.dataone.service.mn.tier1.v1.MNRead) mnRead).get(session,
                                        systemMetadata.getIdentifier());
                                needSciMetadata = false;
                            }
                        } catch (NotAuthorized ex) {
                            if (tryAgain < 2) {
                                ++tryAgain;
                                logger.error(buildStandardLogMessage(ex, " Got NotAuthorized on MNRead.get(), retrying..."));
                                //                            interruptableSleep(5000L);
                            } else {
                                // only way to get out of loop if NotAuthorized keeps getting thrown
                                throw ex;
                            }
                        } catch (NotFound ex) {
                            // this can happen if a MN has the system Metadata but stopped hosting
                            // the object.  (Mutable Member Nodes)
                            // We might be able to take advantage of this information
                            // (see https://redmine.dataone.org/issues/8049)
                            // but for now it's a deal-breaker for synchronization, so make sure 
                            // the cause of the sync failure is clear
                            
                            logger.warn(buildStandardLogMessage(ex, "Got NotFound from MNRead.get()"));
                            throw new NotFound("-1", "Got NotFound from MNRead.get(): " + ex.getDescription());

                        } catch (ServiceFailure ex) {
                        
                            if (tryAgain < 6) {
                                ++tryAgain;
                                logger.error(buildStandardLogMessage(ex, "Got ServiceFailure on MNRead.get(), retrying..."));
                                //(5000L);
                            } else {
                                // only way to get out of loop if ServiceFailure keeps getting thrown
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
                    logger.debug(task.taskLabel() + " Calling CNCreate...");
//                    cis = new CountingInputStream(sciMetaStream);
                    d1Identifier = nodeCommunications.getCnCore().create(session, d1Identifier, sciMetaStream,
                            systemMetadata);
                    logger.debug(task.taskLabel() + " ... CNCreate finished");
                } finally {
                    IOUtils.closeQuietly(sciMetaStream);
//                    IOUtils.closeQuietly(cis);
                }
            } else {
                logger.debug(task.taskLabel() + " Registering SystemMetadata...");
                nodeCommunications.getCnCore().registerSystemMetadata(session, d1Identifier,
                        systemMetadata);
                logger.debug(task.taskLabel() + " ... Registered SystemMetadata");
            }
            logger.info(buildStandardLogMessage(null,  "Completed CreateObject"));
        } catch (ServiceFailure e) {
            extractRetryableException(e);
            throw new UnrecoverableException(task.getPid() + " cn.createObject failed",e);
            
        } catch (BaseException e) {
            throw new UnrecoverableException(task.getPid() + " cn.createObject failed: " + e.getDescription(),e);
        }
    }

    /**
     * Compares the checksum in the systemMetadata against the checksum returned by an MNRead.getChecksum call to the
     * source Member Node.
     *
     * Large objects are given a bye according to the property 'Synchronization.checksum.verify.size.bypass.threshold'
     *
     * @param systemMetadata
     * @throws InvalidSystemMetadata
     */
    private void validateChecksum(SystemMetadata systemMetadata) throws InvalidSystemMetadata {

        logger.debug(task.taskLabel() + " entering validateChecksum...");

        if (systemMetadata.getSize().compareTo(CHECKSUM_VERIFICATION_SIZE_BYPASS_THRESHOLD) > 0) {
            return;
        }

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
            String actualChecksumValue = actualChecksum != null ? actualChecksum.getValue() : null;
            String expectedChecksumValue = expectedChecksum != null ? expectedChecksum.getValue() : null;
            String errorMessage = "The checksum for pid: " + pid
                    + " does not match the actual checksum supplied by the member node: "
                    + task.getNodeId() + ".  Actual checksum: "
                    + actualChecksumValue + ". System metadata checksum: "
                    + expectedChecksumValue;
            InvalidSystemMetadata be = new InvalidSystemMetadata("000", errorMessage);
            if (checksumException != null) {
                be.initCause(checksumException);
            }
            logger.error(buildStandardLogMessage(null,  errorMessage));
            throw be;
        }
    }

    /**
     * Validate the ResourceMap by parsing it with ResourceMapFactory if the format has a way of validating.
     *
     * @param format - cannot be null or throws NPE
     * @param reourceInputStream - cannot be null
     * @return
     * @throws UnsupportedType
     * @throws InsufficientResources
     */
    private void validateResourceMap(ObjectFormat format, InputStream sciMetaStream) throws UnsupportedType, InsufficientResources {

        logger.debug(task.taskLabel() + " entering validateResourceMap...");
        boolean attemptValidation = false;

        if (format != null && format.getFormatType().equalsIgnoreCase("RESOURCE")) {
            for (int i = 0; i < VALIDATING_RESOURCE_FORMATS.length; i++) {
                if (format.getFormatId().getValue().contentEquals(VALIDATING_RESOURCE_FORMATS[i])) {
                    attemptValidation = true;
                }
            }
        }

        if (attemptValidation) {

            // first clone the inputStream so it can be used later
            byte[] resourceBytes = null;
            try {
                resourceBytes = IOUtils.toByteArray(sciMetaStream);
            } catch (IOException e) {
                throw new InsufficientResources("413",
                        "Could not validate Resource Map: Unable to create ByteArrayInputStream for pid: "
                        + task.getPid() + " with message: "
                        + e.getMessage());
            } finally {
                IOUtils.closeQuietly(sciMetaStream);
            }

            if (resourceBytes == null) {
                throw new UnsupportedType("Invalid Resource Map", "input byte[] was null");
            }

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
     * To be called if the Object is already created. Updates can have 4 final outcomes: 1) changes not being allowed,
     * 2) no changes to apply, 3) updates applied, or 4) addition of a replica.
     * <br/>
     * The first outcome is reached when the new system metadata contains one or more field values that cannot replace
     * existing ones OR the CN is not allowed to update the systemMetadata via synchronization (Replica differences are
     * ignored)
     * <br/>
     * The second outcome occurs when there aren't any differences, and usually occurs when an object is reharvested
     * during a full-reharvest, or if an object is synchronized by CN.synchronize ahead of it's scheduled harvest.
     * <br/>
     * The third outcome is considered the normal case, which happens when the authoritative Member Node updates its
     * system metadata.
     * <br/>
     * The fourth outcome occurs when the new system metadata is coming from a replica node. This can happen if two
     * Member Nodes are part of a different replication network (as is the case with several Metacat nodes) and the
     * replica's copy is synchronized before the CNs learn of the replica from the authoritative Member Node.
     *
     * @param SystemMetadata systemMetdata from the MN
     * @throws UnrecoverableException - for problems with the Hz systemMetadata
     * @throws SynchronizationFailed - if we get any other exception talking to the MN (validating checksum)
     * @throws RetryableException - if we get ServiceFailure talking to the MN (validating checksum)
     */
    // TODO: Check that we are not updating V2 sysmeta with V1
    private void processUpdates(SystemMetadata newSystemMetadata)
            throws RetryableException, UnrecoverableException, SynchronizationFailed {

        logger.debug(task.taskLabel() + " entering processUpdates...");

        //XXX is cloning the identifier necessary?
        Identifier pid = D1TypeBuilder.cloneIdentifier(newSystemMetadata.getIdentifier());

        logger.info(buildStandardLogMessage(null, "Start ProcessUpdate"));
        logger.debug(task.taskLabel() + " Getting sysMeta from HazelCast map");
        // TODO: assume that if hasReservation indicates the id exists, that 
        // hzSystemMetadata will not be null...can we make this assumption?
        SystemMetadata hzSystemMetadata = hzSystemMetaMap.get(pid);
        SystemMetadataValidator validator = null;
        String errorTracker = "";
        try {
            errorTracker = "validateSysMeta";
        	validator = new SystemMetadataValidator(hzSystemMetadata);
            validator.validateEssentialProperties(newSystemMetadata, nodeCommunications.getMnRead());

            // if here, we know that the new system metadata is referring to the same
            // object, and we can consider updating other values.
            NodeList nl = nodeCommunications.getNodeRegistryService().listNodes();
            boolean isV1Object = AuthUtils.isCNAuthorityForSystemMetadataUpdate(nl, newSystemMetadata);
            errorTracker = "processUpdate";
            if (task.getNodeId().contentEquals(hzSystemMetadata.getAuthoritativeMemberNode().getValue())) {

                if (isV1Object) {
                	// (no seriesId in V1 to validate)
                    processV1AuthoritativeUpdate(newSystemMetadata, hzSystemMetadata);
                
                } else {
                	validateSeriesId(newSystemMetadata,hzSystemMetadata);
                	processV2AuthoritativeUpdate(newSystemMetadata, validator);
                    
                }
            } else {
            	// not populating anything other than replica information
                processPossibleNewReplica(newSystemMetadata, hzSystemMetadata, isV1Object);
            }
            logger.info(buildStandardLogMessage(null,  " Completed ProcessUpdate"));
        } catch (InvalidSystemMetadata e) {
            if (validator == null) {
                throw new UnrecoverableException("In processUpdates, bad SystemMetadata from the HzMap", e);
            } else {
                throw new UnrecoverableException("In processUpdates, could not find authoritativeMN in the NodeList", e);
            }
        } catch (NotAuthorized e) {
        	if (errorTracker.equals("validateSysMeta")) {
        		throw SyncFailedTask.createSynchronizationFailed(task.getPid(), "In processUpdates, while validating the checksum", e);
        	} else {
        		throw SyncFailedTask.createSynchronizationFailed(task.getPid(), "NotAuthorized to use the seriesId. ", e);
        	}
        } catch (IdentifierNotUnique | InvalidRequest | InvalidToken  | NotFound e) {
        
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), "In processUpdates, while validating the checksum", e);
        } catch (ServiceFailure e) {
            extractRetryableException(e);
            throw new UnrecoverableException("In processUpdates, while validating the checksum:, e");
        } catch (NotImplemented e) {
            throw new UnrecoverableException("In processUpdates, while validating the checksum:, e");
        }
    }

    /**
     * checks to see if this systemMetadata is from an existing replica or is an unknown source that should be
     * registered as a replica.
     *
     * @param newSystemMetadata
     * @param hzSystemMetadata
     * @throws RetryableException - can requeue these
     * @throws UnrecoverableException - for problems with updateReplicationMetadata
     */
    private void processPossibleNewReplica(SystemMetadata newSystemMetadata, SystemMetadata hzSystemMetadata, boolean isV1Object)
            throws RetryableException, UnrecoverableException {
        logger.debug(task.taskLabel() + " entering processPossibleNewReplica...");
        for (Replica replica : hzSystemMetadata.getReplicaList()) {
            if (task.getNodeId().equals(replica.getReplicaMemberNode().getValue())) {
                logger.debug(task.taskLabel() + " Non-authoritative source, existing replica.  No action needed");
                return;
            }
        }

        Replica mnReplica = new Replica();
        mnReplica.setReplicaMemberNode(D1TypeBuilder.buildNodeReference(task.getNodeId()));
        mnReplica.setReplicationStatus(ReplicationStatus.COMPLETED);
        mnReplica.setReplicaVerified(new Date());
        // * status can be set to completed because we verified the checksum 

        logger.info(buildStandardLogMessage(null, "Non-authoritative source, adding the"
                + " node as a replica"));

        try {
            nodeCommunications.getCnReplication().updateReplicationMetadata(session, newSystemMetadata.getIdentifier(),
                    mnReplica, hzSystemMetadata.getSerialVersion().longValue());
            notifyReplicaNodes(TypeFactory.buildIdentifier(task.getPid()), isV1Object);
        } 
        catch (NotImplemented | NotAuthorized | InvalidRequest | InvalidToken e) {
            // can't fix these and are internal configuration problems
            throw new UnrecoverableException("failed to add syncObject as discovered replica", e);
        } 
        catch (ServiceFailure e) {
            extractRetryableException(e);
            throw new UnrecoverableException("failed to add syncObject as discovered replica", e);
        } 
        catch ( NotFound | VersionMismatch e) {
            // how did we get a NotFound if this was deemed a replica?
            throw new UnrecoverableException("failed to add syncObject as discovered replica", e);
        }
    }

    /**
     * Validate the new system metadata against the existing and propagate any updates as needed (to CN storage, to MN
     * replica nodes)
     *
     * @param mnSystemMetadata
     * @param hzSystemMetadata
     * @throws RetryableException
     * @throws UnrecoverableException
     * @throws SynchronizationFailed
     */
    private void processV1AuthoritativeUpdate(SystemMetadata mnSystemMetadata, SystemMetadata cnSystemMetadata)
            throws RetryableException, SynchronizationFailed, UnrecoverableException {

        logger.debug(task.taskLabel() + " entering processV1AuthoritativeUpdate...");
        try {
            // this is an update from the authoritative memberNode
            // so look for fields with valid changes
            boolean foundValidMNChange = false;

            // obsoletedBy can be updated to a value only if its value hasn't
            // already been set.  Once set, it cannot change.
            if ((cnSystemMetadata.getObsoletedBy() == null)
                    && (mnSystemMetadata.getObsoletedBy() != null)) {
                logger.debug(task.taskLabel() + " Updating ObsoletedBy...");

                nodeCommunications.getCnCore().setObsoletedBy(session, TypeFactory.buildIdentifier(task.getPid()),
                        mnSystemMetadata.getObsoletedBy(),
                        cnSystemMetadata.getSerialVersion().longValue());
                //                auditReplicaSystemMetadata(pid);
                // serial version will be updated at this point, so get the new version
                logger.debug(task.taskLabel() + " Updated ObsoletedBy");
                foundValidMNChange = true;
            }

            // (getArchived() returns a boolean)
            // only process the update if the new sysmeta set it to true and the 
            // existing value is null or false.  Cannot change the value from true
            // to false.
            if (((mnSystemMetadata.getArchived() != null) && mnSystemMetadata.getArchived())
                    && ((cnSystemMetadata.getArchived() == null) || !cnSystemMetadata.getArchived())) {
                logger.debug(task.taskLabel() + " Updating Archived...");
                nodeCommunications.getCnCore().archive(session, TypeFactory.buildIdentifier(task.getPid()));
                //                auditReplicaSystemMetadata(pid);
                // serial version will be updated at this point, so get the new version
                logger.debug(task.taskLabel() + " Updated Archived");
                foundValidMNChange = true;
            }

            if (foundValidMNChange) {
                notifyReplicaNodes(TypeFactory.buildIdentifier(task.getPid()), true); // true notifies the authMN, too
            } else {
                // TODO: refactor to assume less about how we got here and whether or not to throw an exception
                // 
                // a simple reharvest may lead to getting to this point, so check
                // the sysmeta modified date before throwing an exception
                if (mnSystemMetadata.getDateSysMetadataModified().after(
                        cnSystemMetadata.getDateSysMetadataModified())) {
                    // something has changed, and we should probably investigate,
                    // but for now just assume that an out-of-bounds change was attempted.
                    InvalidRequest invalidRequest = new InvalidRequest(
                            "567123",
                            "Synchronization unable to process the update request. Only archived and obsoletedBy may be updated");
                    logger.error(buildStandardLogMessage(invalidRequest, "Ignoring update from MN. Only archived and obsoletedBy may be updated"));
                    throw SyncFailedTask.createSynchronizationFailed(task.getPid(), null, invalidRequest);
                }
            }
        } 
        catch (ServiceFailure e) {
            V2TransferObjectTask.extractRetryableException(e);
            throw new UnrecoverableException("Failed to update cn with new SystemMetadata.", e);
        } 
        catch (InvalidRequest e) {
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(),
                    "From processV1AuthoritativeUpdate: Could not update cn with new valid SystemMetadata!", e);
        }
        catch (VersionMismatch e) {
            if (task.getAttempt() == 1) {
                try {
                    notifyReplicaNode(cnSystemMetadata, TypeFactory.buildNodeReference(task.getNodeId()));
                } catch (InvalidToken | NotAuthorized | NotImplemented
                        | ServiceFailure | NotFound | InvalidRequest e1) {
                    throw new UnrecoverableException("Could not notify the source MN to update their SystemMetadata in response to " +
                            "encountering a VersionMismatch during V1-style system metadata update", e1);
                }
            }
            throw new RetryableException("Cannot update systemMetadata due to VersionMismatch", e, 5000L);
        }
        
        catch ( NotFound | NotImplemented | NotAuthorized | InvalidToken e) {
            throw new UnrecoverableException("Unexpected failure when trying to update v1-permitted fields (archived, obsoletedBy).", e);

        }
    }

    /**
     * Validate the new system metadata against the existing and propagate any updates as needed (to CN storage, to MN
     * replica nodes)
     *
     * @param mnSystemMetadata
     * @param hzSystemMetadata
     * @throws RetryableException
     * @throws UnrecoverableException
     * @throws SynchronizationFailed
     */
    private void processV2AuthoritativeUpdate(SystemMetadata mnSystemMetadata, SystemMetadataValidator validator)
            throws RetryableException, UnrecoverableException, SynchronizationFailed {

        logger.debug(task.taskLabel() + " entering processV2AuthoritativeUpdate...");
        boolean validated = false;
        try {
            if (validator.hasValidUpdates(mnSystemMetadata)) {
                Identifier pid = mnSystemMetadata.getIdentifier();
                validated = true;

                if (CollectionUtils.isNotEmpty(validator.getReferenceSystemMetadata().getReplicaList())) {
                    // copy the replica information from the CN copy to the new copy
                    // (the CN is authoritative for this property and the MNs aren't expected
                    // to maintain them.)
                    mnSystemMetadata.setReplicaList(validator.getReferenceSystemMetadata().getReplicaList());
                    logger.debug(task.taskLabel() + " Copied over existing Replica section from CN..");
                } else {
                    // if somehow the replica section is empty, recreate it based on
                    // this authoritative replica.
                    // (replication depends on at least the authoritativeMN to
                    // be listed in the replica section with a status of COMPLETED)
                    mnSystemMetadata = populateInitialReplicaList(mnSystemMetadata);
                    logger.debug(task.taskLabel() + " replica section empty, so initialized new ReplicaList");
                }

                // copy over the SerialVersion from the CN version or initialize
                BigInteger cnSerialVersion = validator.getReferenceSystemMetadata().getSerialVersion();
                if (cnSerialVersion == null) {
                    cnSerialVersion = BigInteger.ONE;
                    logger.debug(task.taskLabel() + " serialVersion empty, so initialized to 1.");
                }
                mnSystemMetadata.setSerialVersion(cnSerialVersion);

                logFormatTypeChanges(mnSystemMetadata.getFormatId(), validator.getReferenceSystemMetadata().getFormatId());

                // persist the new systemMetadata
                nodeCommunications.getCnCore().updateSystemMetadata(session, pid, mnSystemMetadata);

                logger.debug(task.taskLabel() + " Updated CN with new SystemMetadata");
                // propagate the changes
                notifyReplicaNodes(pid, false);
                
            } else {
                logger.info(buildStandardLogMessage(null,  " No changes to update."));
            }
        } catch (ServiceFailure e) {
            if (validated) {
                // from cn.updateSystemMetadata
                V2TransferObjectTask.extractRetryableException(e);
                throw new UnrecoverableException("Failed to update CN with new valid SystemMetadata!", e);
            } else {
                // could be content problem in new sysmeta, or an IO problem.
                throw SyncFailedTask.createSynchronizationFailed(task.getPid(), "Problems validating the new SystemMetadata!", e);
            }

        } catch (InvalidRequest e) {
            if (validated) {
                // from cn.updateSystemMetadata, shouldn't ever get here if validated, so not an MN fault
                throw new UnrecoverableException("Failed to update CN with new valid SystemMetadata!", e);
            } else {
                // thrown because of invalid changes
                throw SyncFailedTask.createSynchronizationFailed(task.getPid(), "The new SystemMetadata contains invalid changes.", e);
            }

        } catch (InvalidSystemMetadata e) {
            // from cn.updateSystemMetadata
            throw SyncFailedTask.createSynchronizationFailed(task.getPid(), "The new SystemMetadata was rejected as invalid by the CN.", e);
        } catch (NotImplemented | NotAuthorized | InvalidToken e) {
            // from cn.updateSystemMetadata
            throw new UnrecoverableException("Failed to update CN with new valid SystemMetadata!", e);
        }
    }

    /**
     * Emit a log WARN if the formatId changes, and the FormatType changes. see https://redmine.dataone.org/issues/7371
     * Other failures in this method result in other log statements, and don't interrupt task outcome
     * @param mnFormatId
     * @param cnFormatId
     */
    private void logFormatTypeChanges(ObjectFormatIdentifier mnFormatId, ObjectFormatIdentifier cnFormatId) {

        try {
            if (mnFormatId.getValue().equals(cnFormatId.getValue())) {
                return;
            }

            String mnType = nodeCommunications.getCnCore().getFormat(mnFormatId).getFormatType();
            String cnType = nodeCommunications.getCnCore().getFormat(cnFormatId).getFormatType();

            if (mnType.equals(cnType)) {
                return;
            }

            // if one and only one (XOR) is data, there's been a loggable change 
            if (!mnType.equalsIgnoreCase(cnType)) {
                logger.warn(buildStandardLogMessage(null, String.format("Format type for %s has changed from %s to %s",
                        task.getPid(), cnFormatId.getValue().toUpperCase(), mnFormatId.getValue().toUpperCase())));
            }

        } catch (ServiceFailure | NotFound | NotImplemented | InvalidRequest e) {
            logger.error(buildStandardLogMessage(e,String.format("Format type change for %s could not be determined while looking up the ObjectFormat.",
                    task.getPid())), e);
        } catch (Exception e) {
            logger.error(buildStandardLogMessage(e,String.format("Format type change for %s could not be determined.",
                    task.getPid())),
                    e);
        }
    }

    /**
     * Inform Member Nodes that may have a copy to refresh their version of the system metadata
     * Problems notifying replica nodes results in a warning in the logs, but does not
     * interfere with sync outcome.
     * 
     * @param Identifier pid
     *
     */
    private void notifyReplicaNodes(Identifier pid, boolean notifyAuthNode) {

        logger.info(buildStandardLogMessage(null,  " Entering notifyReplicaNodes..."));
        SystemMetadata cnSystemMetadata = hzSystemMetaMap.get(pid);
        if (cnSystemMetadata != null) {
            List<Replica> prevReplicaList = cnSystemMetadata.getReplicaList();

            for (Replica replica : prevReplicaList) {
                NodeReference replicaNodeId = replica.getReplicaMemberNode();
                try {
                    if (notifyAuthNode) {
                        // notify all nodes
                        notifyReplicaNode(cnSystemMetadata, replicaNodeId);
                    } else {
                        // only notify MNs that are not the AuthMN
                        if (!replicaNodeId.getValue().equals(task.getNodeId())) {
                            notifyReplicaNode(cnSystemMetadata, replicaNodeId);
                        }
                    }
                } catch (InvalidToken | NotAuthorized | NotImplemented |
                        ServiceFailure | NotFound | InvalidRequest be) {
                    // failed notifications shouldn't result in a sync failure
                    // nor a retry, so we will log the exception and keep going
                    logger.error(buildStandardLogMessage(be, "Failed to notify replica member node " +
                            replicaNodeId.getValue()), be);
                }
            }
        } else {
            logger.error(buildStandardLogMessage(null,"null returned from Hazelcast "
                    + hzSystemMetaMapString + " Map"));
        }
    }

    /**
     * Notifies a single Member Node that the systemMetadata has changed. Some logic is included to avoid sending the
     * call to nodes that cannot do anything with the notification. (will only send to nodes that implement MNStorage)
     *
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

        // Characterize the replica MemberNode
        Node replicaNode = nodeCommunications.getNodeRegistryService().getNode(nodeId);
        if (!replicaNode.getType().equals(NodeType.MN)) 
            return; //quietly
        
        
        boolean isV1Tier3 = false;
        boolean isV2Tier3 = false;
        boolean hasTier3 = false;
        // Find out if a tier 3 node, if not then do not callback since it is not implemented
        for (Service service : replicaNode.getServices().getServiceList()) {
            if (service.getName() != null && service.getName().equals("MNStorage") && service.getAvailable()) {
                if (service.getVersion().equalsIgnoreCase("V1")) {
                    isV1Tier3 = true;
                    hasTier3 = true;
                } else if (service.getVersion().equalsIgnoreCase("V2")) {
                    isV2Tier3 = true;
                    hasTier3 = true;
                }
            }
        }
        if (!hasTier3)
            return; //quietly
        
        // If this far, notify the replicaNode
        
        // removed the sanity checks on notification - there isn't a clear reason for them, 
        // and more likely to fail for trivial reasons (NPEs, IOExceptions) and cause some trouble.  
        // (was added 11/11/2011, part of #1979, today is 10/22/2015)
        // (the code used to retrieve the systemMetadata from the replica node
        //  and make sure the modified time was different)
        
        
        NodeComm nodeComm = null;
        try {
            // get a nodeComm to occupy one of the communication slots in the pool
            // but it's going to be simpler logic to ignore existing MnRead from
            // the NodeComm, and get an MNode from a D1Client instead
            // TODO: this should be refactored to allow use of mock objects for
            // unit testing (we shouldn't be going outside of the NodeComm bundle)
            nodeComm = NodeCommSyncObjectFactory.getInstance().getNodeComm(nodeId);
            
            if (isV2Tier3) {
                org.dataone.client.v2.itk.D1Client.getMN(replicaNode.getBaseURL())
                .systemMetadataChanged(session, 
                        cnSystemMetadata.getIdentifier(),
                        cnSystemMetadata.getSerialVersion().longValue(), 
                        cnSystemMetadata.getDateSysMetadataModified());
                logger.info(buildStandardLogMessage(null," Notified (v2) " + nodeId.getValue()));
            } 
            else if (isV1Tier3) {
                org.dataone.client.v1.itk.D1Client.getMN(replicaNode.getBaseURL())
                .systemMetadataChanged(session, 
                        cnSystemMetadata.getIdentifier(),
                        cnSystemMetadata.getSerialVersion().longValue(), 
                        cnSystemMetadata.getDateSysMetadataModified());
                logger.info(buildStandardLogMessage(null," Notified (v1) " + nodeId.getValue()));
            }
        } catch (NodeCommUnavailable e) {
            throw new ServiceFailure("0000", "In notifyReplicaNode: " + e.getMessage());
        } finally {
            if (nodeComm != null)
                nodeComm.setState(NodeCommState.AVAILABLE);
        }
    }
    
    /**
     * This method is used to re-wrap the Exceptions that affect the processing
     * of a TransferObjectTask, particularly various types of time out exceptions
     * that we would want to retry instead of ending in SynchronizationFailed.
     * @param sf
     * @throws RetryableException
     */
    protected static void extractRetryableException(ServiceFailure sf) throws RetryableException {
        Throwable cause = null;
        if (sf != null && sf.getCause() != null) {
            // all of the client-server communication exceptions are wrapped by
            // ClientSideException by libclient_java
            if (sf.getCause() instanceof ClientSideException) {
                if (sf.getCause().getCause() instanceof org.apache.http.conn.ConnectTimeoutException) {
                    throw new RetryableException("retryable exception discovered (ConnectTimeout)", sf.getCause());
                }
                if (sf.getCause().getCause() instanceof java.net.SocketTimeoutException) {
                    throw new RetryableException("retryable exception discovered (SocketTimeout)", sf.getCause());
                }
//                if (sf.getCause().getCause() instanceof java.net.SocketTimeoutException) {
//                    throw new RetryableException("retryable exception discovered", sf.getCause());
//                }
            }
        }
    }
    
    
    protected String buildStandardLogMessage(Throwable th, String introMessage) {
        
        if (th == null) 
            return String.format("%s - %s",
                    task.taskLabel(),
                    introMessage);

        if (th instanceof BaseException) 
            return String.format("%s - %s - %s - %s",
                    task.taskLabel(),
                    introMessage,
                    th.getClass().getSimpleName(),
                    ((BaseException) th).getDescription()
                    );
        
        return String.format("%s - %s - %s - %s",
                task.taskLabel(),
                introMessage,
                th.getClass().getSimpleName(),
                th.getMessage());
    }
}
