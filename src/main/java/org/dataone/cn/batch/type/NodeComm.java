/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.type;

import java.util.Date;
import org.dataone.service.cn.v1.CNCore;
import org.dataone.service.cn.v1.CNRead;
import org.dataone.service.mn.tier1.v1.MNRead;

/**
 * Assemble and manage communication channels used by TransferObjectTask
 *
 * @author waltz
 */
public class NodeComm {

    private MNRead mnRead;
    private CNCore cnCore;
    private CNRead cnRead;

    // helpful for debugging
    private Integer number;

    // keeps track of whether a thread is actively using this comm node
    private NodeCommState state;
    // help to determine if thread is blocking, used as timeout
    private Date runningStartDate;


    public NodeComm(MNRead mnRead, CNCore cncore, CNRead cnread) {
        this.mnRead = mnRead;
        this.cnCore = cncore;
        this.cnRead = cnread;
    }

    public MNRead getMnRead() {
        return mnRead;
    }

    public void setMnRead(MNRead mnRead) {
        this.mnRead = mnRead;
    }

    public NodeCommState getState() {
        return state;
    }

    public void setState(NodeCommState state) {
        this.state = state;
    }

    public CNCore getCnCore() {
        return cnCore;
    }

    public void setCnCore(CNCore cnCore) {
        this.cnCore = cnCore;
    }

    public CNRead getCnRead() {
        return cnRead;
    }

    public void setCnRead(CNRead cnRead) {
        this.cnRead = cnRead;
    }


    public Integer getNumber() {
        return number;
    }

    public void setNumber(Integer number) {
        this.number = number;
    }

    public Date getRunningStartDate() {
        return runningStartDate;
    }

    public void setRunningStartDate(Date runningStartDate) {
        this.runningStartDate = runningStartDate;
    }

    
}
