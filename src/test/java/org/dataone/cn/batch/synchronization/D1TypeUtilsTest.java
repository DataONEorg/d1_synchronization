package org.dataone.cn.batch.synchronization;

import static org.junit.Assert.*;

import java.io.IOException;

import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.cn.batch.synchronization.D1TypeUtils;
import org.dataone.service.types.v1.AccessPolicy;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.util.AccessUtil;
import org.dataone.exceptions.MarshallingException;
import org.junit.Test;

public class D1TypeUtilsTest {

    @Test
    public void testEquals_Identifier() {
        
        assertTrue("Same-valued Identifiers should be equal", 
                D1TypeUtils.equals(D1TypeBuilder.buildIdentifier("biff"), 
                        D1TypeBuilder.buildIdentifier("biff")));
        assertFalse("Different Identifiers should not be equal", 
                D1TypeUtils.equals(D1TypeBuilder.buildIdentifier("biff"), 
                        D1TypeBuilder.buildIdentifier("zip")));
        assertFalse("Identifier vs null should not be equal", 
                D1TypeUtils.equals(D1TypeBuilder.buildIdentifier("biff"), 
                        null));
        assertFalse("Null vs Identifier should not be equal", 
                D1TypeUtils.equals(null, 
                        D1TypeBuilder.buildIdentifier("zip")));
        assertTrue("Null vs. null should be equal", 
                D1TypeUtils.equals((Identifier)null,(Identifier)null));
        assertFalse("null vs. null-value Identifier should return false", 
                D1TypeUtils.equals((Identifier)null,D1TypeBuilder.buildIdentifier(null)));
       
    }
    @Test
    public void testEquals_ObjectFormatIdentifier() {
        
        assertTrue("Same-valued ObjectFormatIdentifiers should be equal", 
                D1TypeUtils.equals(D1TypeBuilder.buildFormatIdentifier("biff"), 
                        D1TypeBuilder.buildFormatIdentifier("biff")));
        assertFalse("Different ObjectFormatIdentifiers should not be equal", 
                D1TypeUtils.equals(D1TypeBuilder.buildFormatIdentifier("biff"), 
                        D1TypeBuilder.buildFormatIdentifier("zip")));
        assertFalse("ObjectFormatIdentifier vs null should not be equal", 
                D1TypeUtils.equals(D1TypeBuilder.buildFormatIdentifier("biff"), 
                        null));
        assertFalse("Null vs ObjectFormatIdentifier should not be equal", 
                D1TypeUtils.equals(null, 
                        D1TypeBuilder.buildFormatIdentifier("zip")));
        assertTrue("Null vs. null should be equal", 
                D1TypeUtils.equals((ObjectFormatIdentifier)null,(ObjectFormatIdentifier)null));
        assertFalse("null vs. null-value ObjectFormatIdentifier should return false", 
                D1TypeUtils.equals((ObjectFormatIdentifier)null,D1TypeBuilder.buildFormatIdentifier(null)));
       
    }
    @Test
    public void testEquals_Subject() {
        
        assertTrue("Same-valued Subjects should be equal", 
                D1TypeUtils.equals(D1TypeBuilder.buildSubject("biff"), 
                        D1TypeBuilder.buildSubject("biff")));
        assertFalse("Different Subjects should not be equal", 
                D1TypeUtils.equals(D1TypeBuilder.buildSubject("biff"), 
                        D1TypeBuilder.buildSubject("zip")));
        assertFalse("Subject vs null should not be equal", 
                D1TypeUtils.equals(D1TypeBuilder.buildSubject("biff"), 
                        null));
        assertFalse("Null vs Subject should not be equal", 
                D1TypeUtils.equals(null, 
                        D1TypeBuilder.buildSubject("zip")));
        assertTrue("Null vs. null should be equal", 
                D1TypeUtils.equals((Subject)null,(Subject)null));
        assertFalse("null vs. null-value Subject should return false", 
                D1TypeUtils.equals((Subject)null,D1TypeBuilder.buildSubject(null)));
       
    }
    @Test
    public void testEquals_NodeReference() {
        
        assertTrue("Same-valued NodeReferences should be equal", 
                D1TypeUtils.equals(D1TypeBuilder.buildNodeReference("biff"), 
                        D1TypeBuilder.buildNodeReference("biff")));
        assertFalse("Different NodeReferences should not be equal", 
                D1TypeUtils.equals(D1TypeBuilder.buildNodeReference("biff"), 
                        D1TypeBuilder.buildNodeReference("zip")));
        assertFalse("NodeReference vs null should not be equal", 
                D1TypeUtils.equals(D1TypeBuilder.buildNodeReference("biff"), 
                        null));
        assertFalse("Null vs NodeReference should not be equal", 
                D1TypeUtils.equals(null, 
                        D1TypeBuilder.buildNodeReference("zip")));
        assertTrue("Null vs. null should be equal", 
                D1TypeUtils.equals((NodeReference)null,(NodeReference)null));
        assertFalse("null vs. null-value NodeReference should return false", 
                D1TypeUtils.equals((NodeReference)null,D1TypeBuilder.buildNodeReference(null)));
       
    }
    
    @Test
    public void testEquals_SerializedForm_Identifier() throws MarshallingException, IOException {
        
        assertTrue("Identifiers should be equal", 
                D1TypeUtils.serializedFormEquals(D1TypeBuilder.buildIdentifier("biff"), 
                        D1TypeBuilder.buildIdentifier("biff")));
        assertFalse("Identifiers should not be equal", 
                D1TypeUtils.serializedFormEquals(D1TypeBuilder.buildIdentifier("biff"), 
                        D1TypeBuilder.buildIdentifier("zip")));
        assertFalse("Identifiers should not be equal", 
                D1TypeUtils.serializedFormEquals(D1TypeBuilder.buildIdentifier("biff"), 
                        null));
        assertFalse("Identifiers should not be equal", 
                D1TypeUtils.serializedFormEquals(null, 
                        D1TypeBuilder.buildIdentifier("zip")));
        assertTrue("Identifiers should be equal", 
                D1TypeUtils.serializedFormEquals((Identifier)null,(Identifier)null));
    }
    
    @Test
    public void testEquals_SerializedForm_AccessPolicy() throws MarshallingException, IOException {
        
        AccessPolicy ap1a = AccessUtil.createSingleRuleAccessPolicy(
                new String[]{"alice","bob","eve"}, new Permission[]{Permission.CHANGE_PERMISSION});
        AccessPolicy ap1b = AccessUtil.createSingleRuleAccessPolicy(
                new String[]{"alice","bob","eve"}, new Permission[]{Permission.CHANGE_PERMISSION});

        AccessPolicy ap2 = AccessUtil.createSingleRuleAccessPolicy(
                new String[]{"alice","eve","bob"}, new Permission[]{Permission.CHANGE_PERMISSION});
        AccessUtil.addPublicAccess(ap2);
        
        assertTrue("1. APs should be equal", 
                D1TypeUtils.serializedFormEquals(ap1a, ap1b));

        assertFalse("2. APs should not be equal", 
                D1TypeUtils.serializedFormEquals(ap1a, ap2));
        
        assertFalse("APs should not be equal with second param null", 
                D1TypeUtils.serializedFormEquals(ap1b, null));

        assertFalse("APs should not be equal with first param mull", 
                D1TypeUtils.serializedFormEquals(null, ap2));

        assertTrue("APs should be equal if both params null", 
                D1TypeUtils.serializedFormEquals((AccessPolicy)null,(AccessPolicy)null));
    }
    
    @Test
    public void testEquals_SerializedForm_ReplicationPolicy() throws MarshallingException, IOException {
        
        ReplicationPolicy rp1a = new ReplicationPolicy();
        rp1a.setNumberReplicas(2);
        rp1a.addBlockedMemberNode(D1TypeBuilder.buildNodeReference("myNode"));
        rp1a.addBlockedMemberNode(D1TypeBuilder.buildNodeReference("yourNode"));
        
        ReplicationPolicy rp1b = new ReplicationPolicy();
        rp1b.setNumberReplicas(2);
        rp1b.addBlockedMemberNode(D1TypeBuilder.buildNodeReference("myNode"));
        rp1b.addBlockedMemberNode(D1TypeBuilder.buildNodeReference("yourNode"));

        ReplicationPolicy rp2 = new ReplicationPolicy();
        rp2.addBlockedMemberNode(D1TypeBuilder.buildNodeReference("myNode"));
        rp2.addBlockedMemberNode(D1TypeBuilder.buildNodeReference("yourNode"));
        rp2.addPreferredMemberNode(D1TypeBuilder.buildNodeReference("bestNodeEver"));
        
        assertTrue("1. RPs should be equal", 
                D1TypeUtils.serializedFormEquals(rp1a, rp1b));

        assertFalse("2. RPs should not be equal", 
                D1TypeUtils.serializedFormEquals(rp1a, rp2));
        
        assertFalse("RPs should not be equal with second param null", 
                D1TypeUtils.serializedFormEquals(rp1b, null));

        assertFalse("RPs should not be equal with first param mull", 
                D1TypeUtils.serializedFormEquals(null, rp2));

        assertTrue("RPs should be equal if both params null", 
                D1TypeUtils.serializedFormEquals((AccessPolicy)null,(AccessPolicy)null));
    }
    
    @Test
    public void testValueEquals_Identifier() {
        
        assertTrue("Same-valued Identifiers should be equal", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildIdentifier("biff"), 
                        D1TypeBuilder.buildIdentifier("biff")));
        assertFalse("Different Identifiers should not be equal", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildIdentifier("biff"), 
                        D1TypeBuilder.buildIdentifier("zip")));
        assertFalse("Identifier vs null should not be equal", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildIdentifier("biff"), 
                        null));
        assertFalse("Null vs Identifier should not be equal", 
                D1TypeUtils.valueEquals(null, 
                        D1TypeBuilder.buildIdentifier("zip")));
        assertTrue("Null vs. null should be equal", 
                D1TypeUtils.valueEquals((Identifier)null,(Identifier)null));
        
        assertTrue("null vs. null-value Identifier should return true", 
                D1TypeUtils.valueEquals((Identifier)null,D1TypeBuilder.buildIdentifier(null)));
        
        assertTrue("null-value Identifier vs. null should return true", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildIdentifier(null), (Identifier)null));
        
        assertTrue("null-value Identifier vs. null-value Identifier should return true", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildIdentifier(null),D1TypeBuilder.buildIdentifier(null)));
       
    }
    @Test
    public void testValueEquals_Subject() {
        
        assertTrue("Same-valued Subjects should be equal", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildSubject("biff"), 
                        D1TypeBuilder.buildSubject("biff")));
        assertFalse("Different Subjects should not be equal", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildSubject("biff"), 
                        D1TypeBuilder.buildSubject("zip")));
        assertFalse("Subject vs null should not be equal", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildSubject("biff"), 
                        null));
        assertFalse("Null vs Subject should not be equal", 
                D1TypeUtils.valueEquals(null, 
                        D1TypeBuilder.buildSubject("zip")));
        assertTrue("Null vs. null should be equal", 
                D1TypeUtils.valueEquals((Subject)null,(Subject)null));
        
        assertTrue("null vs. null-value Subject should return true", 
                D1TypeUtils.valueEquals((Subject)null,D1TypeBuilder.buildSubject(null)));
        
        assertTrue("null-value Subject vs. null should return true", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildSubject(null), (Subject)null));
        
        assertTrue("null-value Subject vs. null-value Subject should return true", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildSubject(null),D1TypeBuilder.buildSubject(null)));
       
    }
    @Test
    public void testValueEquals_ObjectFormatIdentifier() {
        
        assertTrue("Same-valued ObjectFormatIdentifiers should be equal", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildFormatIdentifier("biff"), 
                        D1TypeBuilder.buildFormatIdentifier("biff")));
        assertFalse("Different ObjectFormatIdentifiers should not be equal", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildFormatIdentifier("biff"), 
                        D1TypeBuilder.buildFormatIdentifier("zip")));
        assertFalse("ObjectFormatIdentifier vs null should not be equal", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildFormatIdentifier("biff"), 
                        null));
        assertFalse("Null vs ObjectFormatIdentifier should not be equal", 
                D1TypeUtils.valueEquals(null, 
                        D1TypeBuilder.buildFormatIdentifier("zip")));
        assertTrue("Null vs. null should be equal", 
                D1TypeUtils.valueEquals((ObjectFormatIdentifier)null,(ObjectFormatIdentifier)null));
        
        assertTrue("null vs. null-value ObjectFormatIdentifier should return true", 
                D1TypeUtils.valueEquals((ObjectFormatIdentifier)null,D1TypeBuilder.buildFormatIdentifier(null)));
        
        assertTrue("null-value ObjectFormatIdentifier vs. null should return true", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildFormatIdentifier(null), (ObjectFormatIdentifier)null));
        
        assertTrue("null-value ObjectFormatIdentifier vs. null-value ObjectFormatIdentifier should return true", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildFormatIdentifier(null),D1TypeBuilder.buildFormatIdentifier(null)));
       
    }
    @Test
    public void testValueEquals_NodeReference() {
        
        assertTrue("Same-valued NodeReferences should be equal", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildNodeReference("biff"), 
                        D1TypeBuilder.buildNodeReference("biff")));
        assertFalse("Different NodeReferences should not be equal", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildNodeReference("biff"), 
                        D1TypeBuilder.buildNodeReference("zip")));
        assertFalse("NodeReference vs null should not be equal", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildNodeReference("biff"), 
                        null));
        assertFalse("Null vs NodeReference should not be equal", 
                D1TypeUtils.valueEquals(null, 
                        D1TypeBuilder.buildNodeReference("zip")));
        assertTrue("Null vs. null should be equal", 
                D1TypeUtils.valueEquals((NodeReference)null,(NodeReference)null));
        
        assertTrue("null vs. null-value NodeReference should return true", 
                D1TypeUtils.valueEquals((NodeReference)null,D1TypeBuilder.buildNodeReference(null)));
        
        assertTrue("null-value NodeReference vs. null should return true", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildNodeReference(null), (NodeReference)null));
        
        assertTrue("null-value NodeReference vs. null-value NodeReference should return true", 
                D1TypeUtils.valueEquals(D1TypeBuilder.buildNodeReference(null),D1TypeBuilder.buildNodeReference(null)));
       
    }
    
    @Test
    public void testEmptyEquals_NodeReference() {
        
        assertTrue("Same-valued NodeReferences should be equal", 
                D1TypeUtils.emptyEquals(D1TypeBuilder.buildNodeReference("biff"), 
                        D1TypeBuilder.buildNodeReference("biff")));
        assertFalse("Different NodeReferences should not be equal", 
                D1TypeUtils.emptyEquals(D1TypeBuilder.buildNodeReference("biff"), 
                        D1TypeBuilder.buildNodeReference("zip")));
        assertFalse("NodeReference vs null should not be equal", 
                D1TypeUtils.emptyEquals(D1TypeBuilder.buildNodeReference("biff"), 
                        null));
        assertFalse("Null vs NodeReference should not be equal", 
                D1TypeUtils.emptyEquals(null, 
                        D1TypeBuilder.buildNodeReference("zip")));
        assertTrue("Null vs. null should be equal", 
                D1TypeUtils.emptyEquals((NodeReference)null,(NodeReference)null));
        
        assertTrue("null vs. null-value NodeReference should return true", 
                D1TypeUtils.emptyEquals((NodeReference)null,D1TypeBuilder.buildNodeReference(null)));
        
        assertTrue("null-value NodeReference vs. null should return true", 
                D1TypeUtils.emptyEquals(D1TypeBuilder.buildNodeReference(null), (NodeReference)null));
        
        assertTrue("null-value NodeReference vs. null-value NodeReference should return true", 
                D1TypeUtils.emptyEquals(D1TypeBuilder.buildNodeReference(null),D1TypeBuilder.buildNodeReference(null)));

        assertTrue("null vs. empty NodeReference should return true", 
                D1TypeUtils.emptyEquals((NodeReference)null,D1TypeBuilder.buildNodeReference("")));
        
        assertTrue("empty NodeReference vs. null should return true", 
                D1TypeUtils.emptyEquals(D1TypeBuilder.buildNodeReference(""), (NodeReference)null));
        
        assertTrue("empty NodeReference vs. empty NodeReference should return true", 
                D1TypeUtils.emptyEquals(D1TypeBuilder.buildNodeReference(""),D1TypeBuilder.buildNodeReference("")));
       
    }
    
    
}
