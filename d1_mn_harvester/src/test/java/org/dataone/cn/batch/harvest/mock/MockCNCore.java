/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.harvest.mock;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import org.apache.commons.codec.net.URLCodec;
import org.apache.log4j.Logger;
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
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Event;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.Log;
import org.dataone.service.types.NodeList;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectFormatIdentifier;
import org.dataone.service.types.ObjectFormatList;
import org.dataone.service.types.Session;
import org.dataone.service.types.SystemMetadata;
import org.jibx.runtime.JiBXException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 *
 * @author waltz
 */
@Service("cnCoreServiceImpl")
@Qualifier("cnCoreService")
public class MockCNCore implements CNCore {

    @Autowired
    @Qualifier("testTmpCacheDirectory")
    private String dataoneCacheDirectory;
    Logger logger = Logger.getLogger(MockCNCore.class.getName());
    final static int SIZE = 16384;
    private URLCodec urlCodec = new URLCodec();

    @Override
        public Identifier create(Session session, Identifier pid, InputStream object,
            SystemMetadata sysmeta) throws InvalidToken, ServiceFailure,
            NotAuthorized, IdentifierNotUnique, UnsupportedType,
            InsufficientResources, InvalidSystemMetadata, NotImplemented,
            InvalidRequest{
        try {
            File systemMetadata = new File(dataoneCacheDirectory + File.separator + "cn" + File.separator + "meta" + File.separator + urlCodec.encode(pid.getValue(), "UTF-8"));
            TypeMarshaller.marshalTypeToFile(sysmeta, systemMetadata.getAbsolutePath());

            if (systemMetadata.exists()) {
                systemMetadata.setLastModified(sysmeta.getDateSysMetadataModified().getTime());
            } else {
                throw new ServiceFailure("1190", "SystemMetadata not found on FileSystem after create");
            }
            File objectFile = new File(dataoneCacheDirectory + File.separator + "cn" + File.separator + "object" + File.separator + pid.getValue());
            if (!objectFile.exists() && objectFile.createNewFile()) {
                objectFile.setReadable(true);
                objectFile.setWritable(true);
                objectFile.setExecutable(false);
            }

            if (object != null) {
                FileOutputStream objectFileOutputStream = new FileOutputStream(objectFile);
                BufferedInputStream inputStream = new BufferedInputStream(object);
                byte[] barray = new byte[SIZE];
                int nRead = 0;

                while ((nRead = inputStream.read(barray, 0, SIZE)) != -1) {
                    objectFileOutputStream.write(barray, 0, nRead);
                }
                objectFileOutputStream.flush();
                objectFileOutputStream.close();
                inputStream.close();
            }
        } catch (FileNotFoundException ex) {
            throw new ServiceFailure("1190", ex.getMessage());
        } catch (IOException ex) {
            throw new ServiceFailure("1190", ex.getMessage());
        } catch (JiBXException ex) {
            throw new InvalidSystemMetadata("1180", ex.getMessage());
        }
        return pid;
    }


    public String getDataoneCacheDirectory() {
        return dataoneCacheDirectory;
    }

    public void setDataoneCacheDirectory(String dataoneCacheDirectory) {
        this.dataoneCacheDirectory = dataoneCacheDirectory;
    }

    @Override
    public ObjectFormatList listFormats() throws InvalidRequest, ServiceFailure, NotFound, InsufficientResources, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ObjectFormat getFormat(ObjectFormatIdentifier fmtid) throws InvalidRequest, ServiceFailure, NotFound, InsufficientResources, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Log getLogRecords(Session session, Date fromDate, Date toDate, Event event) throws InvalidToken, InvalidRequest, ServiceFailure, NotAuthorized, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public NodeList listNodes() throws NotImplemented, ServiceFailure {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier reserveIdentifier(Session session, Identifier pid, String scope, String format) throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean registerSystemMetadata(Session session, Identifier pid, SystemMetadata sysmeta) throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, InvalidSystemMetadata {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}