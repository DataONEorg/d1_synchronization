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
import org.dataone.service.cn.impl.v2.NodeRegistryService;
import org.dataone.service.cn.impl.v2.ReserveIdentifierService;
import org.dataone.service.cn.v2.CNCore;
import org.dataone.service.cn.v2.CNReplication;

/**
 * Assemble and manage communication channels used by TransferObjectTask
 *
 * @author waltz
 */
public class NodeComm {

    private Object mnRead;
    private Object cnRead;
    private CNCore cnCore;
    private CNReplication cnReplication;
    private HazelcastInstance hzClient;
    private IdentifierReservationQueryService reserveIdentifierService;
    private NodeRegistryService nodeRegistryService;
    // helpful for debugging
    private Integer number;

    // keeps track of whether a thread is actively using this comm node
    private NodeCommState state;
    // help to determine if thread is blocking, used as timeout
    private Date runningStartDate;
    
    public NodeComm(Object mnRead, NodeRegistryService nodeRegistryService, HazelcastInstance hzClient) {
        this.mnRead = mnRead;
        this.reserveIdentifierService = null;
        this.nodeRegistryService = nodeRegistryService;
        this.hzClient = hzClient;
        this.cnRead = null;
    }

    public NodeComm(Object mnRead, Object cnRead, CNCore cnCore, CNReplication cnReplication, IdentifierReservationQueryService reserveIdentifierService, HazelcastInstance hzClient) {
        this.mnRead = mnRead;
        this.cnRead = cnRead;
        this.cnCore = cnCore;
        this.cnReplication = cnReplication;
        this.reserveIdentifierService = reserveIdentifierService;
        this.hzClient = hzClient;
    }

    public Object getMnRead() {
        return mnRead;
    }

    public void setMnRead(Object mnRead) {
        this.mnRead = mnRead;
    }
    
    public Object getCnRead() {
        return cnRead;
    }

    public void setCnRead(Object cnRead) {
        this.cnRead = cnRead;
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

    public IdentifierReservationQueryService getReserveIdentifierService() {
        return reserveIdentifierService;
    }

    public void setReserveIdentifierService(IdentifierReservationQueryService reserveIdentifierService) {
        this.reserveIdentifierService = reserveIdentifierService;
    }

    public NodeRegistryService getNodeRegistryService() {
        return nodeRegistryService;
    }

    public void setNodeRegistryService(NodeRegistryService nodeRegistryService) {
        this.nodeRegistryService = nodeRegistryService;
    }

    
}
