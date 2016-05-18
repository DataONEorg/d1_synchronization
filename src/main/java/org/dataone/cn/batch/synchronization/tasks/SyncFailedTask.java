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
import org.dataone.cn.synchronization.types.SyncObject;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.SynchronizationFailed;
import org.dataone.service.mn.tier1.v2.MNRead;
import org.dataone.service.types.v1.Session;

/**
 * This is a callable class to report back to the MN when a failure has
 * occurred during synchronization.
 *
 * @author waltz
 */
public class SyncFailedTask implements Callable<String> {

    static final Logger logger = Logger.getLogger(SyncFailedTask.class);
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
        ServiceFailure serviceFailure = new ServiceFailure(
                "-1", 
                "Connection failure. Connection timed out on service call."
                );
        submitSynchronizationFailed(task.getPid(), null, serviceFailure);
        return "done";
    }

    /**
     * Creates a SynchronizationFailed exception with a standardized message
     * for submitting back to the MN.  If handed a non DataONE BaseException, it will
     * transform the exception in a ServiceFailure, preserving the message in the
     * description field.
     * @param pid
     * @param exception
     * @return
     */
    public static SynchronizationFailed createSynchronizationFailed(String pid, String additionalContext, Exception exception) {
        String nodeId = Settings.getConfiguration().getString("cn.nodeId");
        BaseException be = null;
        if (exception instanceof BaseException) {
                be = (BaseException)exception;
        } else {
            be = new ServiceFailure("-1", 
                    String.format("%s - %s. %s", 
                            exception.getClass().getCanonicalName(),
                            exception.getMessage(),
                            additionalContext));
            be.initCause(exception);
        }
        SynchronizationFailed syncFailed = new SynchronizationFailed(
                "6001", 
                "Synchronization task of [PID::]" + pid + "[::PID] failed. " + be.getDescription()
                );
        syncFailed.setPid(pid);
        syncFailed.setNodeId(nodeId);
        return syncFailed;
    }


    public void submitSynchronizationFailed(String pid, String additionalContext, BaseException exception) {
        SynchronizationFailed syncFailed = createSynchronizationFailed(pid, additionalContext, exception);
        submitSynchronizationFailed(syncFailed);
    }
        
    public void submitSynchronizationFailed(SynchronizationFailed syncFailed) {
        try {
            Object mnRead = nodeCommunications.getMnRead();
            if (mnRead instanceof MNRead) {
                ((MNRead) mnRead).synchronizationFailed(session, syncFailed);;
            }
            if (mnRead instanceof org.dataone.service.mn.tier1.v1.MNRead) {
               ((org.dataone.service.mn.tier1.v1.MNRead) mnRead).synchronizationFailed(session, syncFailed);
            }
        } catch (InvalidToken ex) {
            logger.error(task.taskLabel() + " " + ex.serialize(ex.FMT_XML));
        } catch (NotAuthorized ex) {
            logger.error(task.taskLabel() + " " + ex.serialize(ex.FMT_XML));
        } catch (NotImplemented ex) {
            logger.error(task.taskLabel() + " " + ex.serialize(ex.FMT_XML));
        } catch (ServiceFailure ex) {
            logger.error(task.taskLabel() + " " + ex.serialize(ex.FMT_XML));
        } catch (Exception ex) {
            logger.error(task.taskLabel() + " " + ex.getMessage());
        }
    }
}
