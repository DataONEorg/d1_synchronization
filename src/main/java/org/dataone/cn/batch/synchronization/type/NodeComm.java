/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.batch.synchronization.type;

import com.hazelcast.core.HazelcastInstance;
import java.util.Date;
import org.dataone.service.cn.impl.v1.NodeRegistryService;
import org.dataone.service.cn.impl.v1.ReserveIdentifierService;
import org.dataone.service.cn.v1.CNCore;
import org.dataone.service.cn.v1.CNIdentity;
import org.dataone.service.cn.v1.CNReplication;
import org.dataone.service.mn.tier1.v1.MNRead;

/**
 * Assemble and manage communication channels used by TransferObjectTask
 *
 * @author waltz
 */
public class NodeComm {

    private MNRead mnRead;
    private CNCore cnCore;
    private CNReplication cnReplication;
    private HazelcastInstance hzClient;
    private ReserveIdentifierService reserveIdentifierService;
    private NodeRegistryService nodeRegistryService;
    // helpful for debugging
    private Integer number;

    // keeps track of whether a thread is actively using this comm node
    private NodeCommState state;
    // help to determine if thread is blocking, used as timeout
    private Date runningStartDate;
    public NodeComm(NodeRegistryService nodeRegistryService, HazelcastInstance hzClient) {
        this.mnRead = null;
        this.cnCore = null;
        this.cnReplication = null;
        this.reserveIdentifierService = null;
        this.nodeRegistryService = nodeRegistryService;
        this.hzClient = hzClient;
    }

    public NodeComm(MNRead mnRead, CNCore cnCore, CNReplication cnReplication, ReserveIdentifierService reserveIdentifierService, HazelcastInstance hzClient) {
        this.mnRead = mnRead;
        this.cnCore = cnCore;
        this.cnReplication = cnReplication;
        this.reserveIdentifierService = reserveIdentifierService;
        this.hzClient = hzClient;
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

    public HazelcastInstance getHzClient() {
        return hzClient;
    }

    public void setHzClient(HazelcastInstance hzClient) {
        this.hzClient = hzClient;
    }

    public CNReplication getCnReplication() {
        return cnReplication;
    }

    public void setCnReplication(CNReplication cnReplication) {
        this.cnReplication = cnReplication;
    }

    public ReserveIdentifierService getReserveIdentifierService() {
        return reserveIdentifierService;
    }

    public void setReserveIdentifierService(ReserveIdentifierService reserveIdentifierService) {
        this.reserveIdentifierService = reserveIdentifierService;
    }

    public NodeRegistryService getNodeRegistryService() {
        return nodeRegistryService;
    }

    public void setNodeRegistryService(NodeRegistryService nodeRegistryService) {
        this.nodeRegistryService = nodeRegistryService;
    }

    
}
