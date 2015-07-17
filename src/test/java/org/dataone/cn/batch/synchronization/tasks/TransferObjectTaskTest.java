package org.dataone.cn.batch.synchronization.tasks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Date;

import javax.mail.util.ByteArrayDataSource;

import org.apache.commons.io.IOUtils;
import org.dataone.client.D1Node;
import org.dataone.client.D1NodeFactory;
import org.dataone.client.NodeLocator;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.client.v2.impl.NodeListNodeLocator;
import org.dataone.client.v2.itk.D1Object;
import org.dataone.cn.batch.harvest.mock.MockReserveIdentifierService;
import org.dataone.cn.batch.synchronization.type.IdentifierReservationQueryService;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.synchronization.types.SyncObject;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.v2.CNCore;
import org.dataone.service.cn.v2.CNRead;
import org.dataone.service.cn.v2.CNReplication;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.types.v1.Event;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v2.Log;
import org.dataone.service.types.v2.LogEntry;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

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

        // authMN
        org.dataone.client.v2.MNode authMNode =
                D1NodeFactory.buildNode(org.dataone.client.v2.MNode.class, null,
                URI.create("java:org.dataone.client.v2.impl.InMemoryMNode#Subject=authMnAdmin&Subject=cnClient&NodeReference=urn:node:theCN"));

        // preRepMN
        org.dataone.client.v2.MNode preRepMNode =
                D1NodeFactory.buildNode(org.dataone.client.v2.MNode.class, null,
                URI.create("java:org.dataone.client.v2.impl.InMemoryMNode#Subject=preRepMnAdmin&Subject=cnClient&NodeReference=urn:node:theCN"));

        // replicaMN
        org.dataone.client.v2.MNode replicaMNode =
                D1NodeFactory.buildNode(org.dataone.client.v2.MNode.class, null,
                URI.create("java:org.dataone.client.v2.impl.InMemoryMNode#Subject=replicaMnAdmin&Subject=cnnClient&NodeReference=urn:node:theCN"));

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


    @Test
    public void testSyncNewObject_v2MN_unreserved_DATA_object() throws Exception {

        // build the 'who'
        Subject submitter = D1TypeBuilder.buildSubject("groucho");
        Session submitterSession = new Session();
        submitterSession.setSubject(submitter);

        // build the 'what'
        Identifier pidToSync = D1TypeBuilder.buildIdentifier("foooo");
        D1Object o = new D1Object(pidToSync, new ByteArrayDataSource("a,b,c,d".getBytes(),"text/csv"),
                D1TypeBuilder.buildFormatIdentifier("text/csv"),
                submitter,
                D1TypeBuilder.buildNodeReference("urn:node:authMN"));
        
        // create it on the authMN
        ((org.dataone.client.v2.MNode)nodeLoc.getNode(authMN)).create(
                submitterSession, pidToSync, o.getDataSource().getInputStream(), o.getSystemMetadata());

//      IMap<String,SystemMetadata> sysMetaMap = hzMember.getMap(hzSystemMetaMapString);
//      sysMetaMap.put(o.getIdentifier().getValue(),o.getSystemMetadata());

        
        // sync it
        hzClient = hzMember;
        NodeComm nc = new NodeComm(
                nodeLoc.getNode(authMN),
                nodeLoc.getNode(theCN), (CNCore)nodeLoc.getNode(theCN), (CNReplication)nodeLoc.getNode(theCN),
                this.createMockReserveIdService(null, null, false, true),
                hzClient);
        SyncObject so = new SyncObject(authMN, pidToSync);

        Date fromFilter = new Date();
        V2TransferObjectTask task = new V2TransferObjectTask(nc, so, hzMember, cnClientSession);
        task.call();
        
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
    
    @Test
    public void testSyncNewObject_v2MN_unreserved_METADATA_object() throws Exception {

        // build the 'who'
        Subject submitter = D1TypeBuilder.buildSubject("groucho");
        Session submitterSession = new Session();
        submitterSession.setSubject(submitter);

        // build the 'what'
        Identifier pidToSync = D1TypeBuilder.buildIdentifier("foooo2");
        D1Object o = new D1Object(pidToSync, new ByteArrayDataSource("a,b,c,d,e".getBytes(),"text/xml"),
                D1TypeBuilder.buildFormatIdentifier("eml://ecoinformatics.org/eml-2.1.0"),
                submitter,
                D1TypeBuilder.buildNodeReference("urn:node:authMN"));
        
        // create it on the authMN
        ((org.dataone.client.v2.MNode)nodeLoc.getNode(authMN)).create(
                submitterSession, pidToSync, o.getDataSource().getInputStream(), o.getSystemMetadata());

//      IMap<String,SystemMetadata> sysMetaMap = hzMember.getMap(hzSystemMetaMapString);
//      sysMetaMap.put(o.getIdentifier().getValue(),o.getSystemMetadata());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        TypeMarshaller.marshalTypeToOutputStream(o.getSystemMetadata(), baos);
        System.out.println(baos.toString("UTF-8"));
        baos.close();
        
        // sync it
        hzClient = hzMember;
        NodeComm nc = new NodeComm(
                nodeLoc.getNode(authMN),
                nodeLoc.getNode(theCN), (CNCore)nodeLoc.getNode(theCN), (CNReplication)nodeLoc.getNode(theCN),
                this.createMockReserveIdService(null, null, false, true),
                hzClient);
        SyncObject so = new SyncObject(authMN, pidToSync);

        Date fromFilter = new Date();
        V2TransferObjectTask task = new V2TransferObjectTask(nc, so, hzMember, cnClientSession);
        task.call();
        
        Log events = ((org.dataone.client.v2.MNode)nodeLoc.getNode(authMN)).getLogRecords(cnClientSession, fromFilter, null, Event.SYNCHRONIZATION_FAILED.toString(), pidToSync.getValue(), null, null);
        outputLogEntries(events);
        
//        System.out.println(System.getProperty("java.version"));
        
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

    public void buildSyncObject(String identifier, boolean onMN, boolean onCN, boolean hasReplicas) {
        // create systemMetadata

        // add sysmeta to Hz map if required

        // add sysmeta to CN if required

        // add sysmeta to authMN

        // add sysmeta to replicaMN

        // add sysmeta to preReplicated MN


    }
    
    void outputLogEntries(Log entries) {
        for (LogEntry le : entries.getLogEntryList()) {
            System.out.print("eventDate=" + le.getDateLogged());
            System.out.print(" : type=" + le.getEvent());
            System.out.print(" : id=" + le.getIdentifier().getValue());
            System.out.println(" : nodeId=" + le.getNodeIdentifier().getValue()); 
        }
    }


}