/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.ldap;

import java.util.Date;
import java.util.Map;
import javax.naming.directory.DirContext;
import org.apache.log4j.Logger;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;

/**
 * Extend the NodeFacade class in d1_cn_noderegistery to allow
 * for synchronization specific behaviour
 * 
 * Expose public access to protected methods in NodeAccess
 *
 * The public methods will also control the borrowing and returning of LDAPContexts to the LDAP Pool
 *
 * @author waltz
 */
public class NodeRegistrySyncFacade extends NodeFacade {

    static final Logger logger = Logger.getLogger(NodeRegistrySyncFacade.class);


    /*
     * Set the synchronization last harvested date in the node referenced by the nodeIdentifier
     *
     */
    public void setDateLastHarvested(NodeReference nodeIdentifier, Date lastDateNodeHarvested) throws ServiceFailure {
        DirContext dirContext = null;
        Map<String, String> nodeIdList = null;
        try {
            dirContext = getDirContextProvider().borrowDirContext();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceFailure("16801", ex.getMessage());
        }
        if (dirContext == null) {
            throw new ServiceFailure("16801", "Context is null.Unable to retrieve LDAP Directory Context from pool. Please try again.");
        }
        try {
            getNodeAccess().setDateLastHarvested(dirContext, nodeIdentifier, lastDateNodeHarvested);
        } finally {
            getDirContextProvider().returnDirContext(dirContext);
        }
    }
    /*
     * Get the synchronization last harvested date in the node referenced by the nodeIdentifier
     *
     */
    public Date getDateLastHarvested(NodeReference nodeIdentifier) throws ServiceFailure {
        DirContext dirContext = null;
        Date dateLastHarvested;
        try {
            dirContext = getDirContextProvider().borrowDirContext();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceFailure("16802", ex.getMessage());
        }
        if (dirContext == null) {
            throw new ServiceFailure("16802", "Context is null.Unable to retrieve LDAP Directory Context from pool. Please try again.");
        }
        try {
            dateLastHarvested = getNodeAccess().getDateLastHarvested(dirContext, nodeIdentifier);
        } finally {
            getDirContextProvider().returnDirContext(dirContext);
        }
        return dateLastHarvested;
    }
    
}
