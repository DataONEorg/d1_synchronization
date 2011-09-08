/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.synchronization;

import org.dataone.cn.batch.type.NodeComm;

/**
 *
 * @author waltz
 */
public interface NodeCommFactory {

    public NodeComm getNodeComm(String mnUrl);
}
