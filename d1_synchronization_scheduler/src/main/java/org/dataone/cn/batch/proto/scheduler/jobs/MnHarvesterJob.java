/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.scheduler.jobs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.proto.harvest.*;
import org.dataone.cn.jjigae.BiBimBob;
import org.dataone.service.cn.CoordinatingNodeAuthentication;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.ObjectInfo;
import org.dataone.service.types.SystemMetadata;

/**
 *
 * @author rwaltz
 */
public class MnHarvesterJob {

    static Logger logger = Logger.getLogger(MnHarvesterJob.class.getName());
    BiBimBob bob;
    AuthToken cnToken;
    CoordinatingNodeAuthentication cnClient;
    List<String> bi;
    ObjectListQueueBuilder queueBuilder;
    ObjectListQueueProcessor queueProcessor;
    ObjectListQueueWriter queueWriter;

    public void init() {
        try {
            bi = bob.bapMeogeureoGaja();
        } catch (FileNotFoundException ex) {
            logger.error(ex.getMessage(), ex);
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        } catch (GeneralSecurityException ex) {
            logger.error(ex.getMessage(), ex);

        }
    }

    public void harvestMetadata() {
        try {
            ArrayList<ObjectInfo> objectInfoList = new ArrayList<ObjectInfo>();
            // Need the LinkedHashMap to preserver insertion order
            LinkedHashMap<Identifier, SystemMetadata> writeQueue = new LinkedHashMap<Identifier, SystemMetadata>();
            // TODO review how tomcat sessions deal with multiple logins from same user
            AuthToken authToken = cnClient.login(bi.get(0), bi.get(1));
            cnToken.setToken(authToken.getToken());
            queueBuilder.setWriteQueue(objectInfoList);
            queueProcessor.setWriteQueue(writeQueue);
            /* XXX Could make this a very generic and configurable pipeline
            See: http://userweb.cs.utexas.edu/users/vin/pub/pdf/plop96.pdf */
            queueBuilder.buildQueue();
            queueProcessor.setReadQueue(queueBuilder.getWriteQueue());
            queueProcessor.processQueue();
            do {
                queueWriter.setReadQueue(queueProcessor.getWriteQueue());
                queueWriter.writeQueue();
            } while (queueProcessor.processQueue());

        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }
    }

    public BiBimBob getBob() {
        return bob;
    }

    public void setBob(BiBimBob bob) {
        this.bob = bob;
    }

    public CoordinatingNodeAuthentication getCnClient() {
        return cnClient;
    }

    public void setCnClient(CoordinatingNodeAuthentication cnClient) {
        this.cnClient = cnClient;
    }

    public AuthToken getCnToken() {
        return cnToken;
    }

    public void setCnToken(AuthToken cnToken) {
        this.cnToken = cnToken;
    }

    public ObjectListQueueBuilder getQueueBuilder() {
        return queueBuilder;
    }

    public void setQueueBuilder(ObjectListQueueBuilder queueBuilder) {
        this.queueBuilder = queueBuilder;
    }

    public ObjectListQueueProcessor getQueueProcessor() {
        return queueProcessor;
    }

    public void setQueueProcessor(ObjectListQueueProcessor queueProcessor) {
        this.queueProcessor = queueProcessor;
    }

    public ObjectListQueueWriter getQueueWriter() {
        return queueWriter;
    }

    public void setQueueWriter(ObjectListQueueWriter queueWriter) {
        this.queueWriter = queueWriter;
    }
}
