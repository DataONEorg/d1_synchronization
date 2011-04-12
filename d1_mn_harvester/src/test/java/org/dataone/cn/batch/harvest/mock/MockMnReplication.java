/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.harvest.mock;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import org.dataone.cn.batch.utils.TypeMarshaller;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.mn.MemberNodeReplication;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectList;
import org.jibx.runtime.JiBXException;

/**
 *
 * @author waltz
 */
public class MockMnReplication implements MemberNodeReplication {

    File objectListFile;

    public File getObjectListFile() {
        return objectListFile;
    }

    public void setObjectListFile(File objectListFile) {
        this.objectListFile = objectListFile;
    }

    @Override
    public ObjectList listObjects(AuthToken token, Date startTime, Date endTime, ObjectFormat objectFormat, Boolean replicaStatus, Integer start, Integer count) throws NotAuthorized, InvalidRequest, NotImplemented, ServiceFailure, InvalidToken {
        try {
            return TypeMarshaller.unmarshalTypeFromFile(ObjectList.class, objectListFile);
        } catch (IOException ex) {
            throw new ServiceFailure("4801", ex.getClass().getName() + ": " + ex.getMessage());
        } catch (InstantiationException ex) {
            throw new ServiceFailure("4801", ex.getClass().getName() + ": " + ex.getMessage());
        } catch (IllegalAccessException ex) {
            throw new ServiceFailure("4801", ex.getClass().getName() + ": " + ex.getMessage());
        } catch (JiBXException ex) {
            throw new ServiceFailure("4801", ex.getClass().getName() + ": " + ex.getMessage());
        }

    }
}
