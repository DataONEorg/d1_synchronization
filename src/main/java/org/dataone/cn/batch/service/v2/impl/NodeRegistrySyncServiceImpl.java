/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.service.v2.impl;

import java.util.Date;
import org.dataone.cn.batch.service.v2.NodeRegistrySyncService;
import org.dataone.cn.ldap.NodeRegistrySyncFacade;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;

/**
 *
 * @author waltz
 */
public class NodeRegistrySyncServiceImpl implements NodeRegistrySyncService {

    private NodeRegistrySyncFacade serviceImpl
            = new NodeRegistrySyncFacade();

    @Override
    public NodeList listNodes()
            throws ServiceFailure, NotImplemented {

        return serviceImpl.getApprovedNodeList();
    }

    @Override
    public Node getNode(NodeReference nodeId) throws NotFound,
            ServiceFailure {
        return serviceImpl.getNode(nodeId);
    }

    @Override
    public void setDateLastHarvested(NodeReference nodeIdentifier, Date lastDateNodeHarvested) throws ServiceFailure {
        serviceImpl.setDateLastHarvested(nodeIdentifier, lastDateNodeHarvested);
    }

    @Override
    public Date getDateLastHarvested(NodeReference nodeIdentifier) throws ServiceFailure {
        return serviceImpl.getDateLastHarvested(nodeIdentifier);
    }

}
