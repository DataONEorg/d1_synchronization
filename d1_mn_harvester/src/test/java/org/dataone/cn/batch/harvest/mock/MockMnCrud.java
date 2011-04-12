/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.harvest.mock;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.utils.TypeMarshaller;
import org.dataone.service.EncodingUtilities;
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
import org.dataone.service.mn.MemberNodeCrud;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Checksum;
import org.dataone.service.types.DescribeResponse;
import org.dataone.service.types.Event;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.Log;
import org.dataone.service.types.SystemMetadata;
import org.jibx.runtime.JiBXException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 *
 * @author waltz
 */
@Service("memberNodeCrudImpl")
@Qualifier("crudService")
public class MockMnCrud implements MemberNodeCrud {

    @Autowired
    @Qualifier("testSamplesDirectory")
    private String dataoneCacheDirectory;
    Logger logger = Logger.getLogger(MockMnCrud.class.getName());
    final static int SIZE = 16384;

    @Override
    public InputStream get(AuthToken token, Identifier guid) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        InputStream inputStream = null;
        try {
            String filePath = dataoneCacheDirectory + File.separator + "mn" + File.separator + "object" + File.separator + EncodingUtilities.encodeUrlPathSegment(guid.getValue());
            logger.info("get filepath: " + filePath);
            inputStream = new FileInputStream(new File(filePath));
            logger.info("is it available? " + inputStream.available());
        } catch (FileNotFoundException ex) {
            logger.warn(ex);
            NotFound exception = new NotFound("000", ex.getMessage());
            throw exception;
        } catch (IOException ex) {
            logger.warn(ex);
            ServiceFailure exception = new ServiceFailure("001", ex.getMessage());
            throw exception;
        }
        return inputStream;
    }

    @Override
    public SystemMetadata getSystemMetadata(AuthToken token, Identifier guid) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, InvalidRequest, NotImplemented {
        SystemMetadata systemMetadata = new SystemMetadata();

        InputStream inputStream = null;
        try {
            String filePath = dataoneCacheDirectory + File.separator + "mn" + File.separator + "meta" + File.separator + EncodingUtilities.encodeUrlPathSegment(guid.getValue());
            logger.info(filePath);
            systemMetadata = TypeMarshaller.unmarshalTypeFromFile(SystemMetadata.class, filePath);
            logger.info("\n");
            logger.info(systemMetadata.getIdentifier());
            logger.info("\n");
        } catch (InstantiationException ex) {
            ServiceFailure exception = new ServiceFailure("002", ex.getMessage());

        } catch (IllegalAccessException ex) {
            ServiceFailure exception = new ServiceFailure("003", ex.getMessage());

        } catch (JiBXException ex) {
            ServiceFailure exception = new ServiceFailure("004", ex.getMessage());

        } catch (FileNotFoundException ex) {
            logger.warn(ex);
            NotFound exception = new NotFound("005", ex.getMessage());
            throw exception;
        } catch (IOException ex) {
            logger.warn(ex);
            ServiceFailure exception = new ServiceFailure("006", ex.getMessage());
            throw exception;
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException ex) {
                logger.warn(ex);
            }
        }

        return systemMetadata;
    }

    @Override
    public Identifier create(AuthToken token, Identifier guid, InputStream object, SystemMetadata sysmeta) throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public String getDataoneCacheDirectory() {
        return dataoneCacheDirectory;
    }

    public void setDataoneCacheDirectory(String dataoneCacheDirectory) {
        this.dataoneCacheDirectory = dataoneCacheDirectory;
    }

    @Override
    public Log getLogRecords(AuthToken token, Date fromDate, Date toDate, Event event) throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public DescribeResponse describe(AuthToken token, Identifier guid) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier update(AuthToken token, Identifier guid, InputStream object, Identifier obsoletedGuid, SystemMetadata sysmeta) throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, UnsupportedType, InsufficientResources, NotFound, InvalidSystemMetadata, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier delete(AuthToken token, Identifier guid) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Checksum getChecksum(AuthToken token, Identifier guid) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, InvalidRequest, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Checksum getChecksum(AuthToken token, Identifier guid, String checksumAlgorithm) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, InvalidRequest, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
