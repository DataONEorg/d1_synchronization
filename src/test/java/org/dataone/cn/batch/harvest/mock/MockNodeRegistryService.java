package org.dataone.cn.batch.harvest.mock;

import java.util.Date;
import org.dataone.cn.batch.service.v2.NodeRegistrySyncService;
import org.dataone.cn.ldap.NodeAccess;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;

public class MockNodeRegistryService implements NodeRegistrySyncService {

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

    @Override
    public void setDateLastHarvested(NodeReference nodeIdentifier, Date lastDateNodeHarvested) throws ServiceFailure {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Date getDateLastHarvested(NodeReference nodeIdentifier) throws ServiceFailure {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
