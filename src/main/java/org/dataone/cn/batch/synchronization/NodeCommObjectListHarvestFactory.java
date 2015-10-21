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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.batch.exceptions.NodeCommUnavailable;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.batch.synchronization.type.NodeRegistryQueryService;

import org.dataone.cn.ldap.NodeAccess;
import org.dataone.service.cn.impl.v2.NodeRegistryService;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Service;

/**
 *
 * Creates a NodeComm (node communications) pool for use by the ObjectListHarvestTask. A NodeComm object is memberNode
 * specific
 *
 * Sets up instances that should be reused by the ObjectListHarvestTask Assume that most of the pooled instances
 * (mNode/nodeRegistry) are not thread-safe, in other words the instances created by this factory will be re-used by
 * threads, but no two concurrent threads should access the same instances (with the exception of hazelcast client
 * instance)
 *
 * @author waltz
 */
public class NodeCommObjectListHarvestFactory implements NodeCommFactory {

    public final static Log logger = LogFactory.getLog(NodeCommObjectListHarvestFactory.class);

    private static ConcurrentMap<NodeReference, NodeComm> initializedMemberNodes = new ConcurrentHashMap<NodeReference, NodeComm>();
    private static NodeCommFactory nodeCommFactory = null;

    private NodeCommObjectListHarvestFactory() {
    }

    public static NodeCommFactory getInstance() {
        if (nodeCommFactory == null) {
            nodeCommFactory = new NodeCommObjectListHarvestFactory();
        }
        return nodeCommFactory;
    }

    @Override
    public NodeComm getNodeComm(NodeReference mnNodeId) throws ServiceFailure, NodeCommUnavailable {

        if (initializedMemberNodes.containsKey(mnNodeId)) {
            return initializedMemberNodes.get(mnNodeId);
        } else {

            // figure out what client impl to use for this node, default to v1
            Object mNode = org.dataone.client.v1.itk.D1Client.getMN(mnNodeId);
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
            Node node = null;
            boolean hasCore = false;
            boolean hasRead = false;
            boolean hasV2 = false;
            try {
                node = nodeRegistryService.getNode(mnNodeId);
                if  (node.getServices() == null || node.getServices().getServiceList() == null) {
                    throw new NodeCommUnavailable(mnNodeId + " does not have any services");
                }
                for (Service service : node.getServices().getServiceList()) {
                    if (service.getVersion().equals("v2")) {
                        hasV2 = true;
                    }
                    if (service.getName().equals("MNCore")) {
                        hasCore = true;
                    }
                    if (service.getName().equals("MNRead")) {
                        hasRead = true;
                    }
                    if (hasV2 && hasCore && hasRead) {
                        break;
                    }
                }
            } catch (NotFound ex) {
                throw new NodeCommUnavailable(ex.getDescription());
            }
            if (!hasRead) {
                throw new NodeCommUnavailable(mnNodeId + " does not have MNRead Service");
            }
            if (!hasCore) {
                throw new NodeCommUnavailable(mnNodeId + " does not have MNCore Service");
            }
            if (hasV2) {
                mNode = org.dataone.client.v2.itk.D1Client.getMN(mnNodeId);
            }
            NodeComm nodeComm = new NodeComm(mNode, nodeRegistryService);
            initializedMemberNodes.putIfAbsent(mnNodeId, nodeComm);
            return nodeComm;
        }
    }
}
