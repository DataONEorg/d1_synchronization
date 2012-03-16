/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (NotAuthorized ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (NotImplemented ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (ServiceFailure ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
    }
}
