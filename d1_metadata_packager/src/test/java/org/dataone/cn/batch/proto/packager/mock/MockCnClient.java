/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.proto.packager.mock;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.dataone.cn.batch.utils.TypeMarshaller;
import org.dataone.service.cn.CNCore;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.impl.ObjectFormatServiceImpl;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Event;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.Log;
import org.dataone.service.types.NodeList;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectFormatIdentifier;
import org.dataone.service.types.ObjectFormatList;
import org.dataone.service.types.Services;
import org.dataone.service.types.Session;
import org.dataone.service.types.SystemMetadata;
import org.jibx.runtime.JiBXException;

/**
 *
 * @author waltz
 */
public class MockCnClient implements CNCore {

    File nodeListFile;
    @Override
    public NodeList listNodes() throws NotImplemented, ServiceFailure {
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


    public File getNodeListFile() {
        return nodeListFile;
    }

    public void setNodeListFile(File nodeListFile) {
        this.nodeListFile = nodeListFile;
    }

    @Override
    public ObjectFormatList listFormats() throws InvalidRequest, ServiceFailure, NotFound, InsufficientResources, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ObjectFormat getFormat(ObjectFormatIdentifier fmtid) throws InvalidRequest, ServiceFailure, NotFound, InsufficientResources, NotImplemented {
        ObjectFormatServiceImpl objectFormatService = ObjectFormatServiceImpl.getInstance();
        return objectFormatService.getFormat(fmtid);
    }

    @Override
    public Log getLogRecords(Session session, Date fromDate, Date toDate, Event event) throws InvalidToken, InvalidRequest, ServiceFailure, NotAuthorized, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier reserveIdentifier(Session session, Identifier pid, String scope, String format) throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier create(Session session, Identifier pid, InputStream object, SystemMetadata sysmeta) throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean registerSystemMetadata(Session session, Identifier pid, SystemMetadata sysmeta) throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, InvalidSystemMetadata {
        throw new UnsupportedOperationException("Not supported yet.");
    }


}
