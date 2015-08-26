package org.dataone.cn.batch.synchronization.tasks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.LinkedList;

import javax.mail.util.ByteArrayDataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.dataone.client.D1Node;
import org.dataone.client.D1NodeFactory;
import org.dataone.client.NodeLocator;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.v2.impl.NodeListNodeLocator;
import org.dataone.client.v2.itk.D1Object;
import org.dataone.client.v2.types.D1TypeBuilder;
import org.dataone.cn.batch.harvest.mock.InMemoryCNReadCore;
import org.dataone.cn.batch.harvest.mock.MockReserveIdentifierService;
import org.dataone.cn.batch.synchronization.type.IdentifierReservationQueryService;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.synchronization.types.SyncObject;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.v2.CNCore;
import org.dataone.service.cn.v2.CNRead;
import org.dataone.service.cn.v2.CNReplication;
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
import org.dataone.service.mn.tier3.v2.MNStorage;
import org.dataone.service.types.v1.Event;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.util.AuthUtils;
import org.dataone.service.types.v2.Log;
import org.dataone.service.types.v2.LogEntry;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.types.v2.TypeFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class TransferObjectTaskTest {

    private static HazelcastInstance hzMember;
    private HazelcastInstance hzClient;

    String synchronizationObjectQueue =
            Settings.getConfiguration().getString("dataone.hazelcast.synchronizationObjectQueue");
    String hzNodesName =
            Settings.getConfiguration().getString("dataone.hazelcast.nodes");
    String hzSystemMetaMapString =
            Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");

    static NodeLocator nodeLoc;

    static NodeReference theCN = D1TypeBuilder.buildNodeReference("urn:node:theCN");
    static NodeReference authMN = D1TypeBuilder.buildNodeReference("urn:node:authMN");
    static NodeReference preRepMN = D1TypeBuilder.buildNodeReference("urn:node:preRepMN");
    static NodeReference replicaMN = D1TypeBuilder.buildNodeReference("urn:node:replicaMN");
    static NodeReference otherMN = D1TypeBuilder.buildNodeReference("urn:node:otherMN");
    static Session cnClientSession;
    {
        cnClientSession = new Session();
        cnClientSession.setSubject(D1TypeBuilder.buildSubject("cnClient"));
        /* this property can't be left null, or it creates invalid replicas in the CN systemMetadata */
        Settings.getConfiguration().setProperty("cn.router.nodeId", theCN.getValue());
    }



    @After
    public void tearDown() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Before
    public void setUpContext() throws ClientSideException {

        Config hzConfig = new ClasspathXmlConfig("org/dataone/configuration/hazelcast.xml");

        System.out.println("Hazelcast Group Config:\n" + hzConfig.getGroupConfig());
        System.out.print("Hazelcast Maps: ");
        for (String mapName : hzConfig.getMapConfigs().keySet()) {
            System.out.print(mapName + " ");
        }

        System.out.println();
        hzMember = Hazelcast.newHazelcastInstance(hzConfig);
        System.out.println("Hazelcast member hzMember name: " + hzMember.getName());


        // need a set of nodes for the nodeComm
        // theCN
        Object theCNode =
                D1NodeFactory.buildNode(org.dataone.service.cn.v2.CNRead.class, null,
                URI.create("java:org.dataone.cn.batch.harvest.mock.InMemoryCNReadCore#Subject=cnAdmin&Subject=cnClient"));
        assert theCNode instanceof D1Node;
        assert theCNode instanceof CNCore;
        IMap<Identifier,SystemMetadata> sMap = hzMember.getMap(hzSystemMetaMapString);
        ((InMemoryCNReadCore)theCNode).setHzSysMetaMap(sMap);

        // authMN
        org.dataone.client.v2.MNode authMNode =
                D1NodeFactory.buildNode(org.dataone.client.v2.MNode.class, null,
                URI.create("java:org.dataone.client.v2.impl.InMemoryMNode#Subject=authMnAdmin&Subject=cnClient&NodeReference=urn:node:theCN"));

        // preRepMN
        org.dataone.client.v2.MNode preRepMNode =
                D1NodeFactory.buildNode(org.dataone.client.v2.MNode.class, null,
                URI.create("java:org.dataone.cn.batch.harvest.mock.InMemoryMockReplicaMNode#Subject=preRepMnAdmin&Subject=cnClient&NodeReference=urn:node:theCN"));

        // replicaMN
        org.dataone.client.v2.MNode replicaMNode =
                D1NodeFactory.buildNode(org.dataone.client.v2.MNode.class, null,
                URI.create("java:org.dataone.cn.batch.harvest.mock.InMemoryMockReplicaMNode#Subject=replicaMnAdmin&Subject=cnClient&NodeReference=urn:node:theCN"));

        // otherMN
        org.dataone.client.v2.MNode otherMNode =
                D1NodeFactory.buildNode(org.dataone.client.v2.MNode.class, null,
                URI.create("java:org.dataone.client.v2.impl.InMemoryMNode#Subject=otherMnAdmin&Subject=cnClient&NodeReference=urn:node:theCN"));

     // and put them into a NodeLocator
        nodeLoc = new NodeListNodeLocator(null, null);

        nodeLoc.putNode(theCN, (D1Node)theCNode);
        nodeLoc.putNode(authMN, authMNode);
        nodeLoc.putNode(preRepMN, preRepMNode);
        nodeLoc.putNode(replicaMN, replicaMNode);
        nodeLoc.putNode(otherMN, otherMNode);
    }
    
    @Test
    public void testHazelcastSysMetaMapIsReadWrite() {
        IMap<Identifier, SystemMetadata> hzMap = hzMember.getMap(hzSystemMetaMapString);
        Identifier testId = D1TypeBuilder.buildIdentifier("foo");
        SystemMetadata s = new SystemMetadata();
        s.setIdentifier(testId);
        hzMap.put(testId, s);
        
        SystemMetadata ss = hzMap.get(testId);
        assertEquals("should get back the sysMeta", s.getIdentifier().getValue(),ss.getIdentifier().getValue());
        System.out.println(String.format("s = %s", s));
        System.out.println(String.format("ss = %s", ss));
    }


    @Test
    public void testCantRetrieveMNsSysMeta() throws Exception {

        Identifier pidToSync = D1TypeBuilder.buildIdentifier("foooo");
        Subject sysMetaSubmitter = D1TypeBuilder.buildSubject("groucho");

        hzClient = hzMember;
        NodeComm nc = new NodeComm(
                nodeLoc.getNode(authMN),
                nodeLoc.getNode(theCN), (CNCore)nodeLoc.getNode(theCN), (CNReplication)nodeLoc.getNode(theCN),
                this.createMockReserveIdService(pidToSync, sysMetaSubmitter, false, true),
                hzClient);
        SyncObject so = new SyncObject(authMN, pidToSync);

        Date fromFilter = new Date();
        V2TransferObjectTask task = new V2TransferObjectTask(nc, so, hzMember, cnClientSession);
        task.call();
        
        Log events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(authMN)).getLogRecords(cnClientSession, fromFilter, null, Event.SYNCHRONIZATION_FAILED.toString(), pidToSync.getValue(), null, null);
        outputLogEntries(events);
        assertEquals("Task should submit a Sync Failed when it can't retrieve the systemMetadata from the MN", 1, events.getLogEntryList().size());
        
    }

    private IdentifierReservationQueryService createMockReserveIdService(Identifier knownIdentifier, Subject reservationHolder, boolean alreadyCreated, boolean acceptSession) {
        return new MockReserveIdentifierService( knownIdentifier,  reservationHolder,  alreadyCreated,  acceptSession);
    }

    /**
     * tests that only a data object's systemMetadata is uploaded to the CN. 
     * @throws Exception
     */
    @Test
    public void testSyncNewObject_v2MN_unreserved_DATA_object() throws Exception {

        Subject submitter = D1TypeBuilder.buildSubject("groucho");
        Identifier pidToSync = createTestObjectOnMN(submitter, authMN, true).getIdentifier();
        
        Date fromFilter = new Date();
        syncTheObject(this.createMockReserveIdService(null, null, false, true),
                pidToSync, authMN);

        Log events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(authMN)).getLogRecords(cnClientSession, fromFilter, null, Event.SYNCHRONIZATION_FAILED.toString(), pidToSync.getValue(), null, null);
        outputLogEntries(events);
    
        assertEquals("Task should not generate a synchronizationFailed", 0, events.getLogEntryList().size());

        CNRead cnRead = (CNRead)nodeLoc.getNode(theCN);

        try {
            SystemMetadata sysmeta = cnRead.getSystemMetadata(cnClientSession, pidToSync);
        } catch (NotFound e) {
            fail("Should be able to retrieve sysmeta from CN after synchronization.");
        }
        
        InputStream is = null;
        try {
            is = cnRead.get(cnClientSession, pidToSync);
            fail("Should NOT be able to retrieve object bytes from the CN for DATA format.");
        } catch (NotFound e) {
            // that's good
        } finally {
            IOUtils.closeQuietly(is);
        }
    }
    
    /** 
     * tests that a metadata object syncs and is uploaded to the CN, under the condition
     * of not having the identifier being reserved
     * @throws Exception
     */
    @Test
    public void testSyncNewObject_v2MN_unreserved_METADATA_object() throws Exception {

        Subject submitter = D1TypeBuilder.buildSubject("groucho");
        Identifier pidToSync = createTestObjectOnMN(submitter, authMN, false).getIdentifier();
        
        Date fromFilter = new Date();
        syncTheObject(this.createMockReserveIdService(null, null, false, true),
                pidToSync, authMN);

        Log events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(authMN)).getLogRecords(cnClientSession, fromFilter, null, Event.SYNCHRONIZATION_FAILED.toString(), pidToSync.getValue(), null, null);
        outputLogEntries(events);
        
        assertEquals("Task should not generate a synchronizationFailed", 0, events.getLogEntryList().size());

        CNRead cnRead = (CNRead)nodeLoc.getNode(theCN);

        try {
            SystemMetadata sysmeta = cnRead.getSystemMetadata(cnClientSession, pidToSync);
        } catch (NotFound e) {
            fail("Should be able to retrieve sysmeta from CN after synchronization.");
        }
        
        InputStream is = null;
        try {
            is = cnRead.get(cnClientSession, pidToSync);
        } catch (NotFound e) {
            fail("Should be able to retrieve object bytes from the CN for DATA format.");
        } finally {
            IOUtils.closeQuietly(is);
        }
    }
    
    
    /**
     * tests that object correctly syncs when the submitter holds a reservation
     * on the identifier (tests the correct behavior of the reservation check)
     * @throws Exception
     */
    //TODO: note that if the submitter is not set by the MemberNode, the V2TranserObjectTask will
    //percolate an NPE out of the alreadyExists helper method.
    @Test
    public void testSyncNewObject_v2MN_reserved_DATA_object() throws Exception {

        Subject submitter = D1TypeBuilder.buildSubject("groucho");
        Identifier pidToSync = createTestObjectOnMN(submitter, authMN, true).getIdentifier();
        
        Date fromFilter = new Date();
        syncTheObject(this.createMockReserveIdService(pidToSync, submitter, false, true),
                pidToSync, authMN);
        
        Log events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(authMN)).getLogRecords(cnClientSession, fromFilter, null, Event.SYNCHRONIZATION_FAILED.toString(), pidToSync.getValue(), null, null);
        outputLogEntries(events);
        
        assertEquals("Task should not generate a synchronizationFailed", 0, events.getLogEntryList().size());

        CNRead cnRead = (CNRead)nodeLoc.getNode(theCN);
        
        
        try {
            SystemMetadata sysmeta = cnRead.getSystemMetadata(cnClientSession, pidToSync);
        } catch (NotFound e) {
            fail("Should be able to retrieve sysmeta from CN after synchronization.");
        }
        
        InputStream is = null;
        try {
            is = cnRead.get(cnClientSession, pidToSync);
            fail("Should NOT be able to retrieve object bytes from the CN for DATA format.");
        } catch (NotFound e) {
            // that's good
        } finally {
            IOUtils.closeQuietly(is);
        }
    }
    
    
    /**
     * Tests that an object with an id reserved by another subject generates a syncFailure
     * @throws Exception
     */
    @Test
    public void testSyncNewObject_v2MN_reservedByOther_DATA_object() throws Exception {

        Subject submitter = D1TypeBuilder.buildSubject("groucho");
        Identifier pidToSync = createTestObjectOnMN(submitter, authMN, true).getIdentifier();
        
        Date fromFilter = new Date();
        syncTheObject(this.createMockReserveIdService(pidToSync, D1TypeBuilder.buildSubject("zeppo"), false, true),
                pidToSync, authMN);

        
        Log events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(authMN)).getLogRecords(cnClientSession, fromFilter, null, Event.SYNCHRONIZATION_FAILED.toString(), pidToSync.getValue(), null, null);
        outputLogEntries(events);
        
        assertEquals("Task should generate a synchronizationFailed", 1, events.getLogEntryList().size());

        CNRead cnRead = (CNRead)nodeLoc.getNode(theCN);
        
        try {
            SystemMetadata sysmeta = cnRead.getSystemMetadata(cnClientSession, pidToSync);
            fail("Should NOT be able to retrieve sysmeta from CN after synchronization.");
        } catch (NotFound e) {
            ;  //
        }
    }
    
    /**
     * Tests that an object with an id reserved by another subject generates a syncFailure
     * @throws Exception
     */
    @Test
    public void testSyncNewObject_v2MN_pid_collision() throws Exception {

        Subject submitter = D1TypeBuilder.buildSubject("groucho");
        Identifier pidToSync = createTestObjectOnMN(submitter, authMN, true).getIdentifier();
        
        Date sync1Date = new Date();
        syncTheObject(this.createMockReserveIdService(null, null, false, true),
                pidToSync, authMN);

        Log events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(authMN)).getLogRecords(cnClientSession, sync1Date, null, Event.SYNCHRONIZATION_FAILED.toString(), pidToSync.getValue(), null, null);
        outputLogEntries(events);
        
        assertEquals("First Sync should NOT generate a synchronizationFailed", 0, events.getLogEntryList().size());

        CNRead cnRead = (CNRead)nodeLoc.getNode(theCN);
        
        try {
            SystemMetadata sysmeta = cnRead.getSystemMetadata(cnClientSession, pidToSync);
        } catch (NotFound e) {
            fail("Should be able to retrieve sysmeta from CN after first synchronization.");;  //
        }
        
        Subject otherSubmitter = D1TypeBuilder.buildSubject("gummo");
        D1Object o = new D1Object(pidToSync, new ByteArrayDataSource("123,456,789".getBytes(),"text/csv"),
                D1TypeBuilder.buildFormatIdentifier("text/csv"),
                otherSubmitter,
                otherMN);
        
        Session otherSubmitterSession = new Session();
        otherSubmitterSession.setSubject(otherSubmitter);
        // create it on the MN
        ((org.dataone.client.v2.MNode)nodeLoc.getNode(otherMN)).create(
                otherSubmitterSession, pidToSync, o.getDataSource().getInputStream(), o.getSystemMetadata());
 
        Date sync2Date = new Date();
        syncTheObject(this.createMockReserveIdService(null, null, true /*already created*/, true),
                pidToSync, otherMN);
        
        events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(otherMN)).getLogRecords(cnClientSession, sync2Date, null, Event.SYNCHRONIZATION_FAILED.toString(), pidToSync.getValue(), null, null);
        outputLogEntries(events);
        
        assertEquals("Second Sync should generate a synchronizationFailed", 1, events.getLogEntryList().size());

        
        try {
            SystemMetadata sysmeta = cnRead.getSystemMetadata(cnClientSession, pidToSync);
            assertTrue("Submitter should still be the submitter of the first object",sysmeta.getSubmitter().equals(submitter));
        } catch (NotFound e) {
            fail("Should be able to retrieve sysmeta from CN after first synchronization.");;  //
        }

    }
    
    @Test
    public void testSyncNewObject_nonAuthoritativeNode() throws Exception
    {
        Subject submitter = D1TypeBuilder.buildSubject("groucho");
        Identifier pidToSync = createTestObjectOnMN(submitter, authMN, true).getIdentifier();
        
        Date sync1Date = new Date();
        syncTheObject(this.createMockReserveIdService(null, null, false, true),
                pidToSync, authMN);

        Log events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(authMN)).getLogRecords(cnClientSession, sync1Date, null, Event.SYNCHRONIZATION_FAILED.toString(), pidToSync.getValue(), null, null);
        outputLogEntries(events);
        
        assertEquals("First Sync should NOT generate a synchronizationFailed", 0, events.getLogEntryList().size());

        CNRead cnRead = (CNRead)nodeLoc.getNode(theCN);
        
        try {
            SystemMetadata sysmeta = cnRead.getSystemMetadata(cnClientSession, pidToSync);
        } catch (NotFound e) {
            fail("Should be able to retrieve sysmeta from CN after first synchronization.");;  //
        }
        
        Subject otherSubmitter = D1TypeBuilder.buildSubject("gummo");
        D1Object o = new D1Object(pidToSync, new ByteArrayDataSource("123,456,789".getBytes(),"text/csv"),
                D1TypeBuilder.buildFormatIdentifier("text/csv"),
                otherSubmitter,
                otherMN);
        
        Session otherSubmitterSession = new Session();
        otherSubmitterSession.setSubject(otherSubmitter);
        // create it on the MN
        ((org.dataone.client.v2.MNode)nodeLoc.getNode(otherMN)).create(
                otherSubmitterSession, pidToSync, o.getDataSource().getInputStream(), o.getSystemMetadata());
 
        Date sync2Date = new Date();
        syncTheObject(this.createMockReserveIdService(null, null, true /*already created*/, true),
                pidToSync, otherMN);
        
        events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(otherMN)).getLogRecords(cnClientSession, sync2Date, null, Event.SYNCHRONIZATION_FAILED.toString(), pidToSync.getValue(), null, null);
        outputLogEntries(events);
        
        assertEquals("Second Sync should generate a synchronizationFailed", 1, events.getLogEntryList().size());

        
        try {
            SystemMetadata sysmeta = cnRead.getSystemMetadata(cnClientSession, pidToSync);
            assertTrue("Submitter should still be the submitter of the first object",sysmeta.getSubmitter().equals(submitter));
        } catch (NotFound e) {
            fail("Should be able to retrieve sysmeta from CN after first synchronization.");;  //
        }
        
    }
    
    
    /**
     * Replicas will be (re)synchronized if they update their sysmeta as requested by
     * the CN, and the new dateSysMetaModified is more recent than that nodes
     * latest scheduled sync.  The behavior should be to treat it as a no-op.
     * (after confirming it is the same object (validates essential properties)
     */
    @Test
    public void testSyncReplica() throws Exception
    {
        Subject submitter = D1TypeBuilder.buildSubject("groucho");
        D1Object authObject = createTestObjectOnMN(submitter, authMN, true);
        
        Date sync1Date = new Date();
        
        syncTheObject(this.createMockReserveIdService(null, null, false, true),
                authObject.getIdentifier(), authMN);

        Log events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(authMN)).getLogRecords(
                cnClientSession, sync1Date, null, Event.SYNCHRONIZATION_FAILED.toString(), 
                authObject.getIdentifier().getValue(), null, null);
        outputLogEntries(events);
        
        assertEquals("First Sync should NOT generate a synchronizationFailed", 0, events.getLogEntryList().size());

        CNRead cnRead = (CNRead)nodeLoc.getNode(theCN);
        
        try {
            SystemMetadata sysmeta = cnRead.getSystemMetadata(cnClientSession, authObject.getIdentifier());
        } catch (NotFound e) {
            fail("Should be able to retrieve sysmeta from CN after first synchronization.");;  //
        }
        
        SystemMetadata modifiedSysMeta = TypeFactory.clone(authObject.getSystemMetadata());
        
        
        // push the replica to the replicaMN
        ((org.dataone.cn.batch.harvest.mock.InMemoryMockReplicaMNode)nodeLoc.getNode(replicaMN)).pushReplica(
                authObject.getDataSource().getInputStream(), modifiedSysMeta);
 
        Date sync2Date = new Date();
        syncTheObject(this.createMockReserveIdService(null, null, true /*already created*/, true),
                authObject.getIdentifier(), replicaMN);
        
        events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(otherMN)).getLogRecords(
                cnClientSession, sync2Date, null, Event.SYNCHRONIZATION_FAILED.toString(), authObject.getIdentifier().getValue(), null, null);
        outputLogEntries(events);
        
        assertEquals("Second Sync should NOT generate a synchronizationFailed", 0, events.getLogEntryList().size());

        
        try {
            SystemMetadata sysmeta = cnRead.getSystemMetadata(cnClientSession, authObject.getIdentifier());
            assertTrue("Submitter should still be the submitter of the first object",sysmeta.getSubmitter().equals(submitter));
        } catch (NotFound e) {
            fail("Should be able to retrieve sysmeta from CN after first synchronization.");;  //
        }
    }

    
    /**
     * Tests that resynchronizing an object (with the same systemMetadata) doesn't 
     * throw a syncFailed
     * @throws Exception
     */
    @Test
    public void testResync_No_Op() throws Exception
    {
        Subject submitter = D1TypeBuilder.buildSubject("groucho");
        Identifier pidToSync = createTestObjectOnMN(submitter, authMN, true).getIdentifier();
        
        Date firstSync = new Date();
        syncTheObject(this.createMockReserveIdService(null ,null, false /*not created*/, true),
                pidToSync, authMN);

        System.out.println(StringUtils.join(hzMember.getMap(hzSystemMetaMapString).keySet(), ", "));
        
        Date secondSync = new Date();
        syncTheObject(this.createMockReserveIdService(null ,null, true /*already created*/, true),
                pidToSync, authMN);

        Log events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(authMN)).getLogRecords(cnClientSession, firstSync, null, Event.SYNCHRONIZATION_FAILED.toString(), pidToSync.getValue(), null, null);
        outputLogEntries(events);
        
        assertEquals("Task should NOT generate a synchronizationFailed", 0, events.getLogEntryList().size());

        CNRead cnRead = (CNRead)nodeLoc.getNode(theCN);
        
        try {
            SystemMetadata sysmeta = cnRead.getSystemMetadata(cnClientSession, pidToSync); 
        } catch (NotFound e) {
            fail("Should  be able to retrieve sysmeta from CN after synchronization.");
        }
    }
    
    /**
     * Some MemberNodes already replicate objects between themselves (some Metacat
     * communities), so the first time the CN sees an object, it could be from the
     * non-authoritative, replica MN.
     * 
     * We expect in these situations that the two sysmetas will be identical, but
     * don't have any guarantee (It could have the wrong checksum, for example, and
     * in that case, the authoritative copy would be blocked from being registered.)
     * 
     * The CN should get the SystemMetadata from the origin or authoritative MN
     * and compare the two.  A sync failed should be returned to the Replica MN
     * if their copy is determined to be faulty.
     * 
     * @throws Exception
     */
//    @Test
    public void testSyncNewObject_preReplica() throws Exception
    {
        Subject submitter = D1TypeBuilder.buildSubject("groucho");
        D1Object authObject = createTestObjectOnMN(submitter, authMN, true);
        
        Date sync1Date = new Date();
        
        syncTheObject(this.createMockReserveIdService(null, null, false, true),
                authObject.getIdentifier(), authMN);

        Log events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(authMN)).getLogRecords(
                cnClientSession, sync1Date, null, Event.SYNCHRONIZATION_FAILED.toString(), 
                authObject.getIdentifier().getValue(), null, null);
        outputLogEntries(events);
        
        assertEquals("First Sync should NOT generate a synchronizationFailed", 0, events.getLogEntryList().size());

        CNRead cnRead = (CNRead)nodeLoc.getNode(theCN);
        
        try {
            SystemMetadata sysmeta = cnRead.getSystemMetadata(cnClientSession, authObject.getIdentifier());
        } catch (NotFound e) {
            fail("Should be able to retrieve sysmeta from CN after first synchronization.");;  //
        }
        
        SystemMetadata modifiedSysMeta = TypeFactory.clone(authObject.getSystemMetadata());
        
        
        // push the replica to the replicaMN
        ((org.dataone.cn.batch.harvest.mock.InMemoryMockReplicaMNode)nodeLoc.getNode(replicaMN)).pushReplica(
                authObject.getDataSource().getInputStream(), authObject.getSystemMetadata());
 
        Date sync2Date = new Date();
        syncTheObject(this.createMockReserveIdService(null, null, true /*already created*/, true),
                authObject.getIdentifier(), replicaMN);
        
        events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(otherMN)).getLogRecords(
                cnClientSession, sync2Date, null, Event.SYNCHRONIZATION_FAILED.toString(), authObject.getIdentifier().getValue(), null, null);
        outputLogEntries(events);
        
        assertEquals("Second Sync should NOT generate a synchronizationFailed", 0, events.getLogEntryList().size());

        
        try {
            SystemMetadata sysmeta = cnRead.getSystemMetadata(cnClientSession, authObject.getIdentifier());
            assertTrue("Submitter should still be the submitter of the first object",sysmeta.getSubmitter().equals(submitter));
        } catch (NotFound e) {
            fail("Should be able to retrieve sysmeta from CN after first synchronization.");;  //
        }
    }

    
    @Test
    public void testSyncUpdate() throws Exception
    {
        //create, sync from authNode
        Subject submitter = D1TypeBuilder.buildSubject("groucho");
        Session submitterSession = new Session();
        submitterSession.setSubject(submitter);
        D1Object d1o = createTestObjectOnMN(submitter, authMN, true);
        
        Date sync1Date = new Date();
        syncTheObject(this.createMockReserveIdService(null, null, false, true),
                d1o.getIdentifier(), authMN);
        
        Subject newAllowed = D1TypeBuilder.buildSubject("oprah");
        d1o.getAccessPolicyEditor().addAccess(new Subject[]{newAllowed}, Permission.READ);
        
        //update the sysmeta on the authNode
        Boolean success = ((MNStorage)nodeLoc.getNode(authMN)).updateSystemMetadata(
                submitterSession, d1o.getIdentifier(), d1o.getSystemMetadata());
        
        //sync again from the authNode
        Date sync2Date = new Date();
        syncTheObject(this.createMockReserveIdService(null, null, true /* already exists */, true),
                d1o.getIdentifier(), authMN);
        
        // shouldn't be any errors
        Log events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(otherMN)).getLogRecords(
                cnClientSession, sync2Date, null, Event.SYNCHRONIZATION_FAILED.toString(), d1o.getIdentifier().getValue(), null, null);
        outputLogEntries(events);
        assertEquals("Second Sync should NOT generate a synchronizationFailed", 0, events.getLogEntryList().size());

        // should reflect new sysmeta from the authMN
        CNRead cnRead = (CNRead)nodeLoc.getNode(theCN);
        try {
            SystemMetadata cnSysmeta = cnRead.getSystemMetadata(cnClientSession, d1o.getIdentifier());
            LinkedList<Subject> subjects = new LinkedList<Subject>();
            subjects.add(newAllowed);
            assertTrue("Synced sysmeta should contain the updated AccessPolicy",
                    AuthUtils.isAuthorized(subjects, Permission.READ, cnSysmeta));
        } catch (NotFound e) {
            fail("Should be able to retrieve sysmeta from CN after first synchronization.");;  //
        }
    }
    
//  @Test
    public void testSyncUpdate_NonAuthoritativeNode() throws Exception
    {

      

        
      //update the sysmeta on the replica
      
      //sync again from a replica node
        
      // should yield a synFailed
      // should NOT reflect new SysMeta from the replica node
        
        //create, sync from authNode
        Subject submitter = D1TypeBuilder.buildSubject("groucho");
        Session submitterSession = new Session();
        submitterSession.setSubject(submitter);
        D1Object d1o = createTestObjectOnMN(submitter, authMN, true);
        
        // push a replica 
        
        Date sync1Date = new Date();
        syncTheObject(this.createMockReserveIdService(null, null, false, true),
                d1o.getIdentifier(), authMN);
        
        Subject newAllowed = D1TypeBuilder.buildSubject("oprah");
        d1o.getAccessPolicyEditor().addAccess(new Subject[]{newAllowed}, Permission.READ);
        
        //update the sysmeta on the authNode
        Boolean success = ((MNStorage)nodeLoc.getNode(authMN)).updateSystemMetadata(
                submitterSession, d1o.getIdentifier(), d1o.getSystemMetadata());
        
        //sync again from the authNode
        Date sync2Date = new Date();
        syncTheObject(this.createMockReserveIdService(null, null, true /* already exists */, true),
                d1o.getIdentifier(), authMN);
        
        // shouldn't be any errors
        Log events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(otherMN)).getLogRecords(
                cnClientSession, sync2Date, null, Event.SYNCHRONIZATION_FAILED.toString(), d1o.getIdentifier().getValue(), null, null);
        outputLogEntries(events);
        assertEquals("Second Sync should NOT generate a synchronizationFailed", 0, events.getLogEntryList().size());

        // should reflect new sysmeta from the authMN
        CNRead cnRead = (CNRead)nodeLoc.getNode(theCN);
        try {
            SystemMetadata cnSysmeta = cnRead.getSystemMetadata(cnClientSession, d1o.getIdentifier());
            LinkedList<Subject> subjects = new LinkedList<Subject>();
            subjects.add(newAllowed);
            assertTrue("Synced sysmeta should contain the updated AccessPolicy",
                    AuthUtils.isAuthorized(subjects, Permission.READ, cnSysmeta));
        } catch (NotFound e) {
            fail("Should be able to retrieve sysmeta from CN after first synchronization.");;  //
        }
    }


///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

    
    private D1Object createTestObjectOnMN(Subject submitter, NodeReference mn, boolean isDataType) 
            throws NoSuchAlgorithmException, NotFound, InvalidRequest, IOException, IdentifierNotUnique, InsufficientResources, InvalidSystemMetadata, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, UnsupportedType, ClientSideException {
        // build the 'who'
        Session submitterSession = new Session();
        submitterSession.setSubject(submitter);

        String mediaType = null;
        ObjectFormatIdentifier formatId = null;
        if (isDataType) {
            formatId = D1TypeBuilder.buildFormatIdentifier("text/csv");
            mediaType = "text/csv";
        } else {
            formatId = D1TypeBuilder.buildFormatIdentifier("eml://ecoinformatics.org/eml-2.1.0");
            mediaType = "text/xml";
        }
        
        // build the 'what'
        String idString = String.format("SyncUnitTest-%s", new Date().getTime());
        Identifier pidToSync = D1TypeBuilder.buildIdentifier(idString);
        D1Object d1o = new D1Object(pidToSync, new ByteArrayDataSource("a,b,c,d".getBytes(),mediaType),
                formatId,
                submitter,
                mn);
        
        // create it on the MN
        ((org.dataone.client.v2.MNode)nodeLoc.getNode(mn)).create(
                submitterSession, pidToSync, d1o.getDataSource().getInputStream(), d1o.getSystemMetadata());
        return d1o;
    }
    
    private void syncTheObject(IdentifierReservationQueryService hasRes, Identifier pidToSync, NodeReference sourceNode) throws Exception {
        NodeComm nc = new NodeComm(
                nodeLoc.getNode(sourceNode),
                nodeLoc.getNode(theCN), (CNCore)nodeLoc.getNode(theCN), (CNReplication)nodeLoc.getNode(theCN),
                hasRes,
                hzMember);

        SyncObject so = new SyncObject(sourceNode, pidToSync);
        V2TransferObjectTask task = new V2TransferObjectTask(nc, so, hzMember, cnClientSession);
        task.call();
    }
    
    
    private void outputLogEntries(Log entries) {
        for (LogEntry le : entries.getLogEntryList()) {
            System.out.print("eventDate=" + le.getDateLogged());
            System.out.print(" : type=" + le.getEvent());
            System.out.print(" : id=" + le.getIdentifier().getValue());
            System.out.println(" : nodeId=" + le.getNodeIdentifier().getValue()); 
        }
    }


}