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
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.mn.tier1.MNRead;
import org.dataone.service.types.Checksum;
import org.dataone.service.types.DescribeResponse;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectList;
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
@Service("mnReadServiceImpl")
@Qualifier("mnReadService")
public class MockMNRead implements MNRead {

    @Autowired
    @Qualifier("testSamplesDirectory")
    private String dataoneCacheDirectory;
    Logger logger = Logger.getLogger(MockMNRead.class.getName());
    final static int SIZE = 16384;
    File objectListFile;

    @Override
    public InputStream get(Session cert, Identifier pid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
                   NotImplemented, InvalidRequest {
        InputStream inputStream = null;
        try {
            String filePath = dataoneCacheDirectory + File.separator + "mn" + File.separator + "object" + File.separator + EncodingUtilities.encodeUrlPathSegment(pid.getValue());
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
    public SystemMetadata getSystemMetadata(Session cert, Identifier pid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            InvalidRequest, NotImplemented {
        SystemMetadata systemMetadata = new SystemMetadata();

        InputStream inputStream = null;
        try {
            String filePath = dataoneCacheDirectory + File.separator + "mn" + File.separator + "meta" + File.separator + EncodingUtilities.encodeUrlPathSegment(pid.getValue());
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


    public String getDataoneCacheDirectory() {
        return dataoneCacheDirectory;
    }

    public void setDataoneCacheDirectory(String dataoneCacheDirectory) {
        this.dataoneCacheDirectory = dataoneCacheDirectory;
    }

    @Override
    public DescribeResponse describe(Session cert, Identifier pid) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Checksum getChecksum(Session cert, Identifier pid, String checksumAlgorithm) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, InvalidRequest, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ObjectList listObjects(Session cert, Date startTime, Date endTime, ObjectFormat objectFormat, Boolean replicaStatus, Integer start, Integer count) throws NotAuthorized, InvalidRequest, NotImplemented, ServiceFailure, InvalidToken {
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

    public File getObjectListFile() {
        return objectListFile;
    }

    public void setObjectListFile(File objectListFile) {
        this.objectListFile = objectListFile;
    }
}
