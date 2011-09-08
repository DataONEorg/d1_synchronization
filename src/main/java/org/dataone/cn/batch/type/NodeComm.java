/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.type;

import org.dataone.service.cn.v1.CNCore;
import org.dataone.service.cn.v1.CNRead;
import org.dataone.service.mn.tier1.v1.MNRead;

/**
 *
 * @author waltz
 */
public class NodeComm {
        MNRead mnRead;
        CNCore cnCore;
        CNRead cnRead;
        MemberNodeReaderState state;

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

        public MemberNodeReaderState getState() {
            return state;
        }

        public void setState(MemberNodeReaderState state) {
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
      
}
