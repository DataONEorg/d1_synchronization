/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.synchronization.tasks;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.type.NodeCommState;
import org.dataone.cn.batch.type.NodeComm;
import org.dataone.cn.batch.type.SyncObject;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.SynchronizationFailed;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;

/**
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
        SynchronizationFailed syncFailed = new SynchronizationFailed("6001", "Synchronization task of [PID::]" + pid + "[::PID] failed. " + exception.getDescription());
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
