package org.dataone.cn.batch.synchronization.type;

import static org.junit.Assert.*;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Date;

import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.TypeFactory;
import org.dataone.service.types.v2.SystemMetadata;
import org.jibx.runtime.JiBXException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SystemMetadataValidatorTest {
    
    static SystemMetadata standardSysMeta;
    SystemMetadata cnSystemMetadata;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        standardSysMeta = new SystemMetadata();
        standardSysMeta.setAuthoritativeMemberNode(TypeFactory.buildNodeReference("urn:node:FOO"));
        standardSysMeta.setChecksum(new Checksum());
        standardSysMeta.getChecksum().setAlgorithm("MD5");
        standardSysMeta.getChecksum().setValue("3pciovdflkn34p9fivskdfjnw45tp2uoiwv");
        standardSysMeta.setDateUploaded(new Date());
        standardSysMeta.setDateSysMetadataModified(new Date());
        standardSysMeta.setFormatId(TypeFactory.buildFormatIdentifier("text/xml"));
        standardSysMeta.setIdentifier(TypeFactory.buildIdentifier("crumb"));
        standardSysMeta.setOriginMemberNode(TypeFactory.buildNodeReference("urn:node:FOO"));
        standardSysMeta.setRightsHolder(TypeFactory.buildSubject("memememememememe"));
        standardSysMeta.setSize(BigInteger.TEN);
        standardSysMeta.setSubmitter(TypeFactory.buildSubject("memememememememe"));
    }

    @Before
    public void setUp() throws Exception {
        cnSystemMetadata = TypeFactory.clone(standardSysMeta);
    }

    @Test
    public void testHasValidUpdates_archivedTrue2False()
            throws InvalidSystemMetadata, InstantiationException, IllegalAccessException, 
            JiBXException, IOException, ServiceFailure, InvalidRequest {
        
        cnSystemMetadata.setArchived(true);
        SystemMetadataValidator v = new SystemMetadataValidator(cnSystemMetadata);
        
        SystemMetadata newSysmeta = TypeFactory.clone(standardSysMeta);
        
        try {
            newSysmeta.setArchived(false);
            v.hasValidUpdates(newSysmeta);
            fail("Should have thrown an InvalidRequest exception");
        } catch (InvalidRequest e) {
            ;
        }
    }
    
    @Test
    public void testHasValidUpdates_archivedTrue2Null()
            throws InvalidSystemMetadata, InstantiationException, IllegalAccessException, 
            JiBXException, IOException, ServiceFailure, InvalidRequest {
        
        cnSystemMetadata.setArchived(true);
        SystemMetadataValidator v = new SystemMetadataValidator(cnSystemMetadata);
        
        SystemMetadata newSysmeta = TypeFactory.clone(standardSysMeta);
        
        try {
 //           newSysmeta.setArchived(false);
            v.hasValidUpdates(newSysmeta);
            fail("Should have thrown an InvalidRequest exception");
        } catch (InvalidRequest e) {
            ;
        }
    }
    
    @Test
    public void testHasValidUpdates_archivedFalse2True()
            throws InvalidSystemMetadata, InstantiationException, IllegalAccessException, 
            JiBXException, IOException, ServiceFailure, InvalidRequest {
        
        cnSystemMetadata.setArchived(false);
        SystemMetadataValidator v = new SystemMetadataValidator(cnSystemMetadata);
        
        SystemMetadata newSysmeta = TypeFactory.clone(standardSysMeta);
        
        newSysmeta.setArchived(true);
        v.hasValidUpdates(newSysmeta);
    }
    
    @Test
    public void testHasValidUpdates_archivedNull2True()
            throws InvalidSystemMetadata, InstantiationException, IllegalAccessException, 
            JiBXException, IOException, ServiceFailure, InvalidRequest {
        
        SystemMetadataValidator v = new SystemMetadataValidator(cnSystemMetadata);
        
        SystemMetadata newSysmeta = TypeFactory.clone(standardSysMeta);
        
        newSysmeta.setArchived(true);
        v.hasValidUpdates(newSysmeta);
    }
    
    @Test
    public void testHasValidUpdates_archivedTrue2True()
            throws InvalidSystemMetadata, InstantiationException, IllegalAccessException, 
            JiBXException, IOException, ServiceFailure, InvalidRequest {
        
        cnSystemMetadata.setArchived(true);
        SystemMetadataValidator v = new SystemMetadataValidator(cnSystemMetadata);
        
        SystemMetadata newSysmeta = TypeFactory.clone(standardSysMeta);
        
        newSysmeta.setArchived(true);
        v.hasValidUpdates(newSysmeta);
    }
    
    
    @Test
    public void testHasValidUpdates_archivedFalse2False()
            throws InvalidSystemMetadata, InstantiationException, IllegalAccessException, 
            JiBXException, IOException, ServiceFailure, InvalidRequest {
        
        cnSystemMetadata.setArchived(false);
        SystemMetadataValidator v = new SystemMetadataValidator(cnSystemMetadata);
        
        SystemMetadata newSysmeta = TypeFactory.clone(standardSysMeta);
        
        newSysmeta.setArchived(false);
        v.hasValidUpdates(newSysmeta); 
    }
    
    @Test
    public void testHasValidUpdates_archivedNull2Null()
            throws InvalidSystemMetadata, InstantiationException, IllegalAccessException, 
            JiBXException, IOException, ServiceFailure, InvalidRequest {
        
        SystemMetadataValidator v = new SystemMetadataValidator(cnSystemMetadata);
        
        SystemMetadata newSysmeta = TypeFactory.clone(standardSysMeta);
        
        v.hasValidUpdates(newSysmeta);
    }
    
    @Test
    public void testHasValidUpdates_archivedFalse2Null()
            throws InvalidSystemMetadata, InstantiationException, IllegalAccessException, 
            JiBXException, IOException, ServiceFailure, InvalidRequest {
        
        cnSystemMetadata.setArchived(false);
        SystemMetadataValidator v = new SystemMetadataValidator(cnSystemMetadata);
        
        SystemMetadata newSysmeta = TypeFactory.clone(standardSysMeta);
        
        v.hasValidUpdates(newSysmeta);
    }
    
    @Test
    public void testHasValidUpdates_archivedNull2False()
            throws InvalidSystemMetadata, InstantiationException, IllegalAccessException, 
            JiBXException, IOException, ServiceFailure, InvalidRequest {
        
        SystemMetadataValidator v = new SystemMetadataValidator(cnSystemMetadata);
        
        SystemMetadata newSysmeta = TypeFactory.clone(standardSysMeta);
        
        newSysmeta.setArchived(false);
        v.hasValidUpdates(newSysmeta);
    }
    


//    @Test
    public void testValidateEssentialProperties() {
        fail("Not yet implemented");
    }

}
