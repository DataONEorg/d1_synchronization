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
