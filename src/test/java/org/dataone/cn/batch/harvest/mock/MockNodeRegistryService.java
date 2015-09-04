package org.dataone.cn.batch.harvest.mock;

import org.dataone.cn.batch.synchronization.type.NodeRegistryQueryService;
import org.dataone.cn.ldap.NodeAccess;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;

public class MockNodeRegistryService implements NodeRegistryQueryService {

    private NodeList theNodeList;
    
    public MockNodeRegistryService(NodeList nodeList) {
        theNodeList = nodeList;
    }
    
    public NodeList listNodes() {
        return theNodeList;
    }
    
    public Node getNode(NodeReference nodeId) throws NotFound {
        for (Node n : theNodeList.getNodeList()) {
            if (n.getIdentifier().equals(nodeId)) {
                return n;
            }
        }
        throw new NotFound("zzz","No Node named " + nodeId.getValue() + " was found.");
    }

    public NodeAccess getNodeAccess() {
        return null;
    }
}
