package org.dataone.cn.batch.synchronization;

import static org.junit.Assert.*;

import java.net.URI;

import org.dataone.client.D1Node;
import org.dataone.client.D1NodeFactory;
import org.dataone.client.NodeLocator;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.client.v2.impl.NodeListNodeLocator;
import org.dataone.cn.batch.harvest.mock.MockReserveIdentifierService;
import org.dataone.cn.batch.synchronization.tasks.TransferObjectTask;
import org.dataone.cn.batch.synchronization.tasks.V2TransferObjectTask;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.hazelcast.HazelcastInstanceFactory;
import org.dataone.cn.synchronization.types.SyncObject;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.impl.v2.ReserveIdentifierService;
import org.dataone.service.cn.v2.CNCore;
import org.dataone.service.cn.v2.CNReplication;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v2.NodeList;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.core.HazelcastInstance;

public class TransferObjectTaskTest {

    private HazelcastInstance hazelcast = HazelcastInstanceFactory.getProcessingInstance();
//    String cnIdentifier =
//            Settings.getConfiguration().getString("cn.router.nodeId");
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

    @Test
    public void testTrue() {

    }

    @BeforeClass
    public static void setUpContext() throws ClientSideException {
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
                URI.create("java:org.dataone.client.v2.impl.InMemoryMNode#Subject=authMnAdmin&Subject=authMnClient&NodeReference=theCN"));

        // preRepMN
        org.dataone.client.v2.MNode preRepMNode =
                D1NodeFactory.buildNode(org.dataone.client.v2.MNode.class, null,
                URI.create("java:org.dataone.client.v2.impl.InMemoryMNode#Subject=preRepMnAdmin&Subject=preRepMnClient&NodeReference=urn:node:theCN"));

        // replicaMN
        org.dataone.client.v2.MNode replicaMNode =
                D1NodeFactory.buildNode(org.dataone.client.v2.MNode.class, null,
                URI.create("java:org.dataone.client.v2.impl.InMemoryMNode#Subject=replicaMnAdmin&Subject=replicaMnClient&NodeReference=urn:node:theCN"));

        // otherMN
        org.dataone.client.v2.MNode otherMNode =
                D1NodeFactory.buildNode(org.dataone.client.v2.MNode.class, null,
                URI.create("java:org.dataone.client.v2.impl.InMemoryMNode#Subject=otherMnAdmin&Subject=otherMnClient&NodeReference=urn:node:theCN"));

     // and put them into a NodeLocator
        nodeLoc = new NodeListNodeLocator(null, null);

        nodeLoc.putNode(theCN, (D1Node)theCNode);
        nodeLoc.putNode(authMN, authMNode);
        nodeLoc.putNode(preRepMN, preRepMNode);
        nodeLoc.putNode(replicaMN, replicaMNode);
        nodeLoc.putNode(otherMN, otherMNode);
    }


//    @Test
    public void testRequeue_cannot_lock() throws Exception {

        Identifier pidToSync = D1TypeBuilder.buildIdentifier("foooo");
        Subject sysMetaSubmitter = D1TypeBuilder.buildSubject("groucho");

        NodeComm nc = new NodeComm(
                nodeLoc.getNode(authMN),
                nodeLoc.getNode(theCN), (CNCore)nodeLoc.getNode(theCN), (CNReplication)nodeLoc.getNode(theCN),
                this.createMockReserveIdService(pidToSync, sysMetaSubmitter, false, true),
                hazelcast);
        SyncObject so = new SyncObject(authMN, pidToSync);

        V2TransferObjectTask task = new V2TransferObjectTask(nc, so);
        task.call();

    }

    private ReserveIdentifierService createMockReserveIdService(Identifier knownIdentifier, Subject reservationHolder, boolean alreadyCreated, boolean acceptSession) {
        return new MockReserveIdentifierService( knownIdentifier,  reservationHolder,  alreadyCreated,  acceptSession);
    }


//    @Test
    public void testRequeue_Other() throws Exception {

        NodeComm nc = null;
        SyncObject so = null;
        V2TransferObjectTask task = new V2TransferObjectTask(nc, so);
        task.call();
    }

    public void buildSyncObject(String identifier, boolean onMN, boolean onCN, boolean hasReplicas) {
        // create systemMetadata

        // add sysmeta to Hz map if required

        // add sysmeta to CN if required

        // add sysmeta to authMN

        // add sysmeta to replicaMN

        // add sysmeta to preReplicated MN


    }


}