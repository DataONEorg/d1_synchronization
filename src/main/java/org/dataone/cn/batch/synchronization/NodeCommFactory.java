/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.synchronization;

import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.service.exceptions.ServiceFailure;

/**
 *
 * @author waltz
 */
public interface NodeCommFactory {

    public NodeComm getNodeComm(String mnUrl) throws ServiceFailure;
    public NodeComm getNodeComm(String mnUrl, String hzConfigLocation) throws ServiceFailure;
}
