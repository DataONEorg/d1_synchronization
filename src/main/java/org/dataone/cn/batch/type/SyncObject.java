/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.type;

import java.io.Serializable;

/**
 *
 * @author waltz
 */
public class SyncObject implements Serializable {

    private String nodeId;
    private String pid;

    public SyncObject(String nodeId, String pid) {
        this.nodeId = nodeId;
        this.pid = pid;
    }
    public String getNodeId() {
        return nodeId;
    }

    public String getPid() {
        return pid;
    }
    
}