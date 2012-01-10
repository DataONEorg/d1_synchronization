/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.synchronization.type;

import java.io.Serializable;

/**
 *
 * @author waltz
 */
public class SyncObject implements Serializable {

    private String nodeId;
    private String pid;
    private Integer attempt = 1;

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

    // Number of times this object has been attempted to be synchronized
    // it may be attempted multiple times due to a lock being present
    // or another process changing the systemMetadata before
    // the call to the CN for updating succeedsd
    public Integer getAttempt() {
        return attempt;
    }

    public void setAttempt(Integer attempt) {
        this.attempt = attempt;
    }
    
}
