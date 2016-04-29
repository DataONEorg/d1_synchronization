package org.dataone.cn.batch.service.v2;

import java.util.Date;
import org.dataone.cn.batch.synchronization.type.*;
import org.dataone.cn.ldap.NodeAccess;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;

public interface NodeRegistrySyncService {

    public NodeList listNodes() 
            throws ServiceFailure, NotImplemented;
    
    public Node getNode(NodeReference nodeId) 
            throws NotFound, ServiceFailure;
    
    public void setDateLastHarvested(NodeReference nodeIdentifier, Date lastDateNodeHarvested) throws ServiceFailure;
    
    public  Date getDateLastHarvested(NodeReference nodeIdentifier) throws ServiceFailure;
}
