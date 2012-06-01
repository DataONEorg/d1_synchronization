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

package org.dataone.cn.batch.synchronization.tasks;

import java.util.concurrent.Callable;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.batch.synchronization.type.SyncObject;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.SynchronizationFailed;
import org.dataone.service.types.v1.Session;

/**
 * This is a callable class to report back to the MN when a failure has
 * occurred during synchronization.
 *
 * @author waltz
 */
public class SyncFailedTask implements Callable<String> {

    Logger logger = Logger.getLogger(TransferObjectTask.class.getName());
    private NodeComm nodeCommunications;
    private SyncObject task;
    private Session session = null;

    public SyncFailedTask(NodeComm nodeCommunications, SyncObject task) {
        this.nodeCommunications = nodeCommunications;
        this.task = task;
    }
    /**
     * Implement the Callable interface.  This class is executed as a separate thread
     *
     * @author waltz
     *
     */
    @Override
    public String call() {
        //
        // if this is being called it is because another connection timed out
        //
        ServiceFailure serviceFailure = new ServiceFailure("-1", "Connection failure. Connection timed out on service call.");
        submitSynchronizationFailed(task.getPid(), serviceFailure);
        return "done";
    }

    public void submitSynchronizationFailed(String pid, BaseException exception) {
        String nodeId = Settings.getConfiguration().getString("cn.nodeId");
        SynchronizationFailed syncFailed = new SynchronizationFailed("6001", "Synchronization task of [PID::]" + pid + "[::PID] failed. " + exception.getDescription());
        syncFailed.setPid(pid);
        syncFailed.setNodeId(nodeId);
        try {
            nodeCommunications.getMnRead().synchronizationFailed(session, syncFailed);
        } catch (InvalidToken ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + " " + ex.serialize(ex.FMT_XML));
        } catch (NotAuthorized ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + " " + ex.serialize(ex.FMT_XML));
        } catch (NotImplemented ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + " " + ex.serialize(ex.FMT_XML));
        } catch (ServiceFailure ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + " " + ex.serialize(ex.FMT_XML));
        } catch (Exception ex) {
            logger.error("Task-" + task.getNodeId() + "-" + task.getPid() + " " + ex.getMessage());
        }
    }
}
