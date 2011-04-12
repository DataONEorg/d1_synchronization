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
import org.apache.commons.codec.net.URLCodec;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.utils.TypeMarshaller;
import org.dataone.service.cn.CoordinatingNodeCrud;
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
import org.dataone.service.types.Identifier;
import org.dataone.service.types.IdentifierFormat;
import org.dataone.service.types.ObjectLocationList;
import org.dataone.service.types.SystemMetadata;
import org.jibx.runtime.JiBXException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 *
 * @author waltz
 */
@Service("coordinatingNodeCrudImpl")
@Qualifier("crudService")
public class MockCnCrud implements CoordinatingNodeCrud {

    @Autowired
    @Qualifier("testTmpCacheDirectory")
    private String dataoneCacheDirectory;
    Logger logger = Logger.getLogger(MockCnCrud.class.getName());
    final static int SIZE = 16384;
    private URLCodec urlCodec = new URLCodec();

    @Override
    public InputStream get(AuthToken token, Identifier guid) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SystemMetadata getSystemMetadata(AuthToken token, Identifier guid) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, InvalidRequest, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ObjectLocationList resolve(AuthToken token, Identifier guid) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, InvalidRequest, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier create(AuthToken token, Identifier guid, InputStream object, SystemMetadata sysmeta) throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented {
        try {
            File systemMetadata = new File(dataoneCacheDirectory + File.separator + "cn" + File.separator + "meta" + File.separator + urlCodec.encode(guid.getValue(), "UTF-8"));
            TypeMarshaller.marshalTypeToFile(sysmeta, systemMetadata.getAbsolutePath());

            if (systemMetadata.exists()) {
                systemMetadata.setLastModified(sysmeta.getDateSysMetadataModified().getTime());
            } else {
                throw new ServiceFailure("1190", "SystemMetadata not found on FileSystem after create");
            }
            File objectFile = new File(dataoneCacheDirectory + File.separator + "cn" + File.separator + "object" + File.separator + guid.getValue());
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
        return guid;
    }

    @Override
    public Identifier reserveIdentifier(AuthToken token, String scope, IdentifierFormat format) throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier reserveIdentifier(AuthToken token, String scope) throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier reserveIdentifier(AuthToken token, IdentifierFormat format) throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier reserveIdentifier(AuthToken token) throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean assertRelation(AuthToken token, Identifier subjectId, String relationship, Identifier objectId) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, InvalidRequest, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public String getDataoneCacheDirectory() {
        return dataoneCacheDirectory;
    }

    public void setDataoneCacheDirectory(String dataoneCacheDirectory) {
        this.dataoneCacheDirectory = dataoneCacheDirectory;
    }

    @Override
    public Identifier update(AuthToken token, Identifier guid, InputStream object, Identifier obsoletedGuid, SystemMetadata sysmeta) throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, UnsupportedType, InsufficientResources, NotFound, InvalidSystemMetadata, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
