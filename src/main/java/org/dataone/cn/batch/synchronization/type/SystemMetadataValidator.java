package org.dataone.cn.batch.synchronization.type;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.dataone.exceptions.MarshallingException;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.synchronization.D1TypeUtils;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.mn.tier1.v2.MNRead;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;

/**
 * This class contains methods for validating a SystemMetata instance against
 * a reference systemMetadata instance (the one it intends to replace), and against
 * its schema.  
 * 
 * @author rnahf
 *
 */
public class SystemMetadataValidator {

    static final Logger logger = Logger.getLogger(SystemMetadataValidator.class);
    
    private SystemMetadata referenceSysMeta;
 
    
    /** 
     * Creates a new instance of a SystemMetadataValidator with the reference
     * SystemMetadata (the sysmeta to validate against) as the required parameter.
     * 
     * @param referenceSystemMetadata - the systemMetadata to validate changes against
     * @ throws IllegalArgumentException
     * @throws InvalidSystemMetadata 
     */
    public SystemMetadataValidator(SystemMetadata referenceSystemMetadata) throws InvalidSystemMetadata {
        if (referenceSystemMetadata == null) {
            throw new IllegalArgumentException("Reference SystemMetadata parameter cannot be null.");
        }
        SystemMetadataValidator.schemaValidateSystemMetadata(referenceSystemMetadata);
        this.referenceSysMeta = referenceSystemMetadata;
    }
    
    public SystemMetadata getReferenceSystemMetadata() {
        return referenceSysMeta;
    }

    /**
     * Makes sure the system metadata is valid against the schema.
     * @param sysmeta
     * @throws InvalidSystemMetadata - with pid from sysmeta if it can get it
     */
    public static void schemaValidateSystemMetadata(SystemMetadata sysmeta) throws InvalidSystemMetadata {

        logger.info("Entering schemaValidateSysMeta method...");
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            TypeMarshaller.marshalTypeToOutputStream(sysmeta, os);
            os.close();
        } catch (MarshallingException | IOException e) {
            String pid = "null";
            if (sysmeta != null && !D1TypeUtils.valueEquals(sysmeta.getIdentifier(),null)) {
                pid = sysmeta.getIdentifier().getValue();
            }
            String errorMessage = "The SystemMetadata for pid: " + pid + " is not schema valid";
            InvalidSystemMetadata be = new InvalidSystemMetadata("000", errorMessage);
            be.initCause(e);
            logger.error(errorMessage, be);
            throw be;
        }
        finally {
            IOUtils.closeQuietly(os);
        }
    }
    
    
    /**
     * Checks to make sure that the properties that need to be set prior to 
     * initial synchronization are not null.
     * @param sysmeta
     * @throws InvalidSystemMetadata
     */
    public static void validateCNRequiredNonNullFields(SystemMetadata sysmeta) throws InvalidSystemMetadata {


        List<String> illegalNullFields = new LinkedList<String>(); 
        if (sysmeta.getIdentifier() == null)
            illegalNullFields.add("identifier");
        
        
        if (sysmeta.getOriginMemberNode() == null)
            illegalNullFields.add("originMemberNode");
        
        if (sysmeta.getAuthoritativeMemberNode() == null)
            illegalNullFields.add("authoritativeMemberNode");
        
        if (sysmeta.getDateUploaded() == null)
            illegalNullFields.add("dateUploaded");
        
        if (sysmeta.getDateSysMetadataModified() == null)
            illegalNullFields.add("dateSysMetadataModified");
        
        if (sysmeta.getSubmitter() == null)
            illegalNullFields.add("submitter");
        
        if (illegalNullFields.size() > 0) {
            throw new InvalidSystemMetadata("-1", "The following properties cannot be null: "
                    + StringUtils.join(illegalNullFields, ", "));
        }
    }
 
    /**
     * Validates the essential properties of the system metadata that determine
     * whether or not the two are describing the same object.  These properties
     * are the identifier and the checksum.  If any of these are different, it 
     * can't be certain that they are describing the same object.
     * 
     * @param newSysMeta
     * @param authoritativeNodeReadImpl
     * @throws IdentifierNotUnique - if the newSysMeta's identifier or checksum doesn't match
     * @throws ServiceFailure - if the reference sysMeta's checksum is null
     * @throws NotFound - if authMNImpl is used and the pid was not found
     * @throws NotImplemented - if the getChecksum method was not available
     * @throws NotAuthorized  - if the client couldn't access getChecksum
     * @throws InvalidToken  - if the client couldn't access getChecksum
     * @throws InvalidRequest - if the checksum algorithm requested wasn't availabe.
     */
    public void validateEssentialProperties(SystemMetadata newSysMeta, Object authoritativeNodeReadImpl) 
            throws IdentifierNotUnique, ServiceFailure, InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, NotFound {
        
        if (!referenceSysMeta.getIdentifier().equals(newSysMeta.getIdentifier()))
            throw new IdentifierNotUnique("-1","Identifier does not match the reference SystemMetadata's!");
        
        if (referenceSysMeta.getChecksum() == null) {
            String message = "Reference systemMetadata's checksum is null!";
            logger.error(message);
            throw new ServiceFailure("00-SystemMetadataValidator",message);
        }

        String cnCsAlgorithm = referenceSysMeta.getChecksum().getAlgorithm();
        Checksum newChecksum = newSysMeta.getChecksum();
        
        if (!cnCsAlgorithm.equalsIgnoreCase(newChecksum.getAlgorithm())) {
            
            // we can't check algorithms that do not match, so get MN to recalculate using provided algorithm
            logger.info("Try to retrieve a checksum from " +
                    "membernode that matches the algorithm of existing systemMetadata");
            
            
            if (authoritativeNodeReadImpl instanceof MNRead) {
                newChecksum = ((MNRead) authoritativeNodeReadImpl)
                        .getChecksum(null, newSysMeta.getIdentifier(), cnCsAlgorithm);

            } else if (authoritativeNodeReadImpl instanceof org.dataone.service.mn.tier1.v1.MNRead) {
                newChecksum = ((org.dataone.service.mn.tier1.v1.MNRead) authoritativeNodeReadImpl)
                        .getChecksum(null, newSysMeta.getIdentifier(), cnCsAlgorithm);
            } else {
                throw new NotImplemented("0000", "Do not support authoritativeNodeReadImpl Object type (" +
                        authoritativeNodeReadImpl.getClass().getCanonicalName() + "), so could" +
                        		"not compare checksums (needed the MN to recalculate).");
            }
        }
        
        if (!newChecksum.getValue().contentEquals(referenceSysMeta.getChecksum().getValue())) {
            logger.info("Submitted checksum doesn't match the existing one!");
            throw new IdentifierNotUnique("-1",
                    "Checksum does not match existing object with same pid.");
        }
        
        logger.info("The submitted checksum matches existing one");
    }
    
    
    
    /**
     * Compares the passed in SystemMetadata to the reference SystemMetadata to 
     * determine if the new SystemMetadata represents valid updates of the 
     * reference sysmeta.  It assumes the caller validating the systemMetadata 
     * has authority to perform the update.
     * </br>
     * It throws InvalidRequest if there are any disallowed changes introduced.
     * <br/>
     * @see https://jenkins-ucsb-1.dataone.org/job/API%20Documentation%20-%20trunk/ws/api-documentation/build/htmldesign/SystemMetadata.html
     * for specification on ownership and editability of properties.
     * 
     * @param proposedSystemMetadata - the systemMetadata to validate
     * @return - true if there are valid changes; false if there are no changes
     * @throws InvalidRequest - if changes do not validate
     * @throws ServiceFailure - if an AccessPolicy or ReplicationPolicy couldn't be re-serialized for comparison
     */
    public boolean hasValidUpdates(SystemMetadata proposedSystemMetadata) 
    throws InvalidRequest, ServiceFailure {

        // TODO: how to handle the case where the source is an already registered
        // replica with out of date values? We don't change the systemMetadata,
        // but need to trigger an audit.
        int changes = validateRestrictedFields(proposedSystemMetadata, this.referenceSysMeta);
        if (changes > 0) {
            return true;
        }
        if (findChangesInUnrestrictedFields(proposedSystemMetadata, this.referenceSysMeta)) {
            return true;
        }
        return false;
    }

    /**
     * Ensures that fields that can't change or have restrictions on the types
     * of changes (one-way setting of properties) validate, and that fields that
     * need to be initialized (either by client or MN) are not null.
     * @param newSysMeta
     * @param cnSysMeta
     * @return - count of valid, restricted changes
     * @throws InvalidRequest - if any fields are illegally changed
     */
    private int validateRestrictedFields(SystemMetadata newSysMeta, SystemMetadata cnSysMeta) throws InvalidRequest {

        int changes = 0;

        List<String> illegalChangeFields = new LinkedList<String>();

        // serialVersion:  it changes rapidly during replication so it is
        // unreasonable to expect the MN or Client to keep up with it.
//        if (!(newSysMeta.getSerialVersion().compareTo(cnSysMeta.getSerialVersion()) == 0)) {
//            illegalChangeFields.add("serialVersion");
//        }

        
        // identifier - already checked with validateEssentialProperties

        // formatID - cannot be null, but validated by schema validation

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
            // values are different: null, true, or false
            if ((Boolean) ObjectUtils.defaultIfNull(cnSysMeta.getArchived(), Boolean.FALSE)) {
                // since not equal, newSysMeta must be either false or null,
                // which is disallowed
                logger.debug(String.format("starting archived value: '%s', new value '%s'", 
                        cnSysMeta.getArchived(), newSysMeta.getArchived() ));
                illegalChangeFields.add("archived");
            } else {
                changes++;
            }
        }

        // dateUploaded - immutable  //TODO: should this be part of validateEssentialProperties?
        /*if (!ObjectUtils.equals(cnSysMeta.getDateUploaded(), newSysMeta.getDateUploaded())) {
            illegalChangeFields.add("dateUploaded");
        }*/
        if( (cnSysMeta.getDateUploaded() == null && newSysMeta.getDateUploaded() != null) ||
                (cnSysMeta.getDateUploaded() != null && newSysMeta.getDateUploaded() == null) ||
                (cnSysMeta.getDateUploaded() != null && newSysMeta.getDateUploaded() != null && 
                cnSysMeta.getDateUploaded().getTime() != newSysMeta.getDateUploaded().getTime())
                ) {
            illegalChangeFields.add("dateUploaded");
        }

        // originMemberNode - immutable 
        if (!ObjectUtils.equals(cnSysMeta.getOriginMemberNode(), newSysMeta.getOriginMemberNode())) {
            illegalChangeFields.add("originMemberNode");
        }
        
        
        // authoritativeMemberNode - cannot be changed via synchronization (only changeable by manual process)
        if (!ObjectUtils.equals(cnSysMeta.getAuthoritativeMemberNode(), newSysMeta.getAuthoritativeMemberNode())) {
            illegalChangeFields.add("authoritativeMemberNode");
        }
        
        // seriesId:   can only go from null => a value
        if (!D1TypeUtils.equals(cnSysMeta.getSeriesId(), newSysMeta.getSeriesId())) {
            if (cnSysMeta.getSeriesId() == null) {
                changes++;
            } else {
                illegalChangeFields.add("seriesId");
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

        // TODO: detector for 'filename'
        
        // TODO: detector for 'mediaType'
        
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
        } catch (MarshallingException | IOException e1) {
            message.append("Problems serializing one of the AccessPolicies: " + e1.getMessage());
        }

        try {
            if(!D1TypeUtils.serializedFormEquals(newSysMeta.getReplicationPolicy(), cnSysMeta.getReplicationPolicy())) {
                return true; 
            }
        } catch (MarshallingException | IOException e1) {
            message.append(" Could not compare serialized forms of the ReplicationPolicies");
        }
        if (message.length() > 0) {
            throw new ServiceFailure("-1","Problems comparing SystemMetadata: " + message.toString());
        }
        return false;
    }
    
    /**
     * 
     * @param newSysMeta
     * @return - true means there are differences
     * @throws ServiceFailure
     */
    protected boolean customSerializationCompareComplexFields(SystemMetadata newSysMeta) 
    throws ServiceFailure {
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
                TypeMarshaller.marshalTypeToOutputStream(referenceSysMeta.getAccessPolicy(), cnAPos);

                if (!Arrays.equals(newAPos.toByteArray(), cnAPos.toByteArray())) {
                    return true;
                }
            }
        } catch (MarshallingException | IOException e) {
            if (progress == 0) {
                logger.error("Couldn't reserialize the AccessPolicy of the new systemMetadata!", e);
            } else {
                logger.error("Couldn't reserialize the AccessPolicy of the reference systemMetadata!", e);
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

                TypeMarshaller.marshalTypeToOutputStream(referenceSysMeta.getAccessPolicy(), cnRPos);
                progress++;
                TypeMarshaller.marshalTypeToOutputStream(newSysMeta.getAccessPolicy(), newRPos);

                if (!Arrays.equals(newRPos.toByteArray(), cnRPos.toByteArray())) {
                    return true;
                }
            }
        } catch (MarshallingException | IOException e) {
            if (progress == 0) {
                logger.error("Couldn't reserialize the ReplicationPolicy of the reference systemMetadata!", e);
            } else {
                logger.error("Couldn't reserialize the ReplicationPolicy of the new systemMetadata!", e);
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
}

