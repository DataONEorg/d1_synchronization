/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.proto.packager.mock;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.dataone.cn.batch.utils.TypeMarshaller;
import org.dataone.service.cn.CoordinatingNodeRegister;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.NodeList;
import org.dataone.service.types.Services;
import org.jibx.runtime.JiBXException;

/**
 *
 * @author waltz
 */
public class MockCnClient implements CoordinatingNodeRegister{

    File nodeListFile;
    @Override
    public NodeList listNodes(AuthToken token) throws NotImplemented, ServiceFailure {
        try {
            return TypeMarshaller.unmarshalTypeFromFile(NodeList.class, nodeListFile);
        } catch (IOException ex) {
            throw new ServiceFailure("4801", ex.getMessage());
        } catch (InstantiationException ex) {
           throw new ServiceFailure("4801", ex.getMessage());
        } catch (IllegalAccessException ex) {
           throw new ServiceFailure("4801", ex.getMessage());
        } catch (JiBXException ex) {
           throw new ServiceFailure("4801", ex.getMessage());
        }
    }

    @Override
    public boolean addNodeCapabilities(AuthToken token, Identifier pid, Services capabilities) throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier register(AuthToken token, Services capabilities) throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public File getNodeListFile() {
        return nodeListFile;
    }

    public void setNodeListFile(File nodeListFile) {
        this.nodeListFile = nodeListFile;
    }


}
