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
package org.dataone.cn.batch.synchronization;

import com.hazelcast.core.HazelcastInstance;
import java.io.File;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.itk.D1Client;
import org.dataone.cn.batch.exceptions.NodeCommUnavailable;
import org.dataone.cn.batch.synchronization.type.IdentifierReservationQueryService;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.batch.synchronization.type.NodeCommState;
import org.dataone.cn.batch.synchronization.type.NodeRegistryQueryService;
import org.dataone.cn.ldap.NodeAccess;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.impl.v2.NodeRegistryService;
import org.dataone.service.cn.impl.v2.ReserveIdentifierService;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;

/**
 * Creates/maintains a NodeComm (node communications) pool for use by the TransferObjectTask A NodeComm is memberNode
 * specific
 *
 * Sets up instances that should be reused by the TransferObjectTask Assume that most of the instances are not
 * thread-safe, in other words the instances created by this factory will be re-used by threads, but no two concurrent
 * threads should access the same instances (with the exception of hazelcast client instance)
 *
 * @author waltz
 */
public class NodeCommSyncObjectFactory implements NodeCommFactory {

    public final static Log logger = LogFactory.getLog(NodeCommSyncObjectFactory.class);
    private static HazelcastInstance hzclient;
    private String clientCertificateLocation
            = Settings.getConfiguration().getString("D1Client.certificate.directory")
            + File.separator + Settings.getConfiguration().getString("D1Client.certificate.filename");

    private Integer maxNumberOfClientsPerMemberNode = Settings.getConfiguration().getInteger("Synchronization.SyncObjectTask.maxMemberNodeCommThreads", 5);
    // maintain a List of NodeComms for each membernode that will not
    // exceed the maxNumberOfClientsPerMemberNode
    // each NodeComm will have a state, NodeCommState, that
    // will indicate if it is available for use by a future task
    private static Map<NodeReference, List<NodeComm>> initializedMemberNodes = new HashMap<NodeReference, List<NodeComm>>();
    private static NodeCommFactory nodeCommFactory = null;

    private NodeCommSyncObjectFactory() {

    }

    public static NodeCommFactory getInstance() {
        if (nodeCommFactory == null) {
            nodeCommFactory = new NodeCommSyncObjectFactory();
        }
        return nodeCommFactory;
    }

    @Override
    public NodeComm getNodeComm(NodeReference mnNodeId) throws ServiceFailure, NodeCommUnavailable {

        NodeComm nodeCommunications = null;

        // grab a membernode client off of the stack of initialized clients
        if (initializedMemberNodes.containsKey(mnNodeId)) {
            List<NodeComm> nodeCommList = initializedMemberNodes.get(mnNodeId);
            // find a node comm that is not currently in use
            for (NodeComm nodeComm : nodeCommList) {
                if (nodeComm.getState().equals(NodeCommState.AVAILABLE)) {
                    nodeCommunications = nodeComm;
                    nodeCommunications.setState(NodeCommState.RUNNING);
                    nodeCommunications.setRunningStartDate(new Date());
                    break;
                }
            }
            if (nodeCommunications == null) {
                // no node Communication is available, see if we can create a new one
                if (nodeCommList.size() <= maxNumberOfClientsPerMemberNode) {
                    // create and add a new one
                    nodeCommunications = createNodeComm(mnNodeId);

                    nodeCommunications.setState(NodeCommState.RUNNING);
                    nodeCommunications.setNumber(nodeCommList.size() + 1);
                    nodeCommunications.setRunningStartDate(new Date());
                    nodeCommList.add(nodeCommunications);

                } else {
                    throw new NodeCommUnavailable("No Comm Nodes Available: only allow maximum of " 
                            + maxNumberOfClientsPerMemberNode);
                }
            }
        } else {
            // The memberNode hash does not contain an array
            // that is assigned to this MemberNode
            // create it, get a node comm, and put it in the hash
            List<NodeComm> nodeCommList = new ArrayList<NodeComm>();
            nodeCommunications = createNodeComm(mnNodeId);
            nodeCommunications.setState(NodeCommState.RUNNING);
            nodeCommunications.setNumber(nodeCommList.size() + 1);
            nodeCommunications.setRunningStartDate(new Date());
            nodeCommList.add(nodeCommunications);
            initializedMemberNodes.put(mnNodeId, nodeCommList);
        }
        return nodeCommunications;
    }

    private NodeComm createNodeComm(NodeReference mnNodeId) throws ServiceFailure {

        CNode cNode = null;
        try {
            cNode = D1Client.getCN();
        } catch (NotImplemented e) {
            throw new ServiceFailure("0000", e.getMessage());
        }

        // create an IdentifierReservationQueryService comm object
        IdentifierReservationQueryService reserveIdentifierService
                = new IdentifierReservationQueryService() {

                    private ReserveIdentifierService serviceImpl
                    = new ReserveIdentifierService();

                    @Override
                    public boolean hasReservation(Session session, Subject subject, Identifier pid)
                    throws NotAuthorized, NotFound, InvalidRequest {

                        return serviceImpl.hasReservation(session, subject, pid);
                    }
                };

        // create a NodeRegistryQueryService comm object
        NodeRegistryQueryService nodeRegistryService = new NodeRegistryQueryService() {
            private NodeRegistryService serviceImpl
                    = new NodeRegistryService();

            @Override
            public NodeList listNodes()
                    throws ServiceFailure, NotImplemented {

                return serviceImpl.listNodes();
            }

            @Override
            public Node getNode(NodeReference nodeId) throws NotFound,
                    ServiceFailure {
                return serviceImpl.getNode(nodeId);
            }

            @Override
            public NodeAccess getNodeAccess() {
                return serviceImpl.getNodeAccess();
            }
        };
        
        // create the MemberNode comm object
        // figure out what client impl to use for this node.  Try V2, and if CN
        // doesn't have V2 services registered, fallback to V1
        Object mNode = null;
        try {
            Node node = nodeRegistryService.getNode(mnNodeId);
            for (Service service : node.getServices().getServiceList()) {
                if (service.getVersion().equals("v2")) {
                    mNode = org.dataone.client.v2.itk.D1Client.getMN(mnNodeId);
                    break;
                }
            }
        } catch (NotFound ex) {
            throw new ServiceFailure("0000", ex.getDescription());
        }
        if (mNode == null) {
            mNode = org.dataone.client.v1.itk.D1Client.getMN(mnNodeId);
        }

        NodeComm nodeComm = new NodeComm(mNode, cNode, nodeRegistryService, cNode, cNode, reserveIdentifierService);
        return nodeComm;
    }
}
