/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.batch.harvest.mock;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import org.dataone.exceptions.MarshallingException;
import org.apache.log4j.Logger;
import org.dataone.service.exceptions.*;
import org.dataone.service.mn.tier1.v1.MNRead;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.DescribeResponse;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.EncodingUtilities;
import org.dataone.service.util.TypeMarshaller;
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
    static final Logger logger = Logger.getLogger(MockMNRead.class);
    final static int SIZE = 16384;
    File objectListFile;

    @Override
    public InputStream get(Session cert, Identifier pid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
                   NotImplemented {
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
            NotImplemented {
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
            throw exception;
        } catch (IllegalAccessException ex) {
            ServiceFailure exception = new ServiceFailure("003", ex.getMessage());
            throw exception;
        } catch (MarshallingException ex) {
            ServiceFailure exception = new ServiceFailure("004", ex.getMessage());
            throw exception;
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
    public DescribeResponse describe(Session cert, Identifier pid) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Checksum getChecksum(Session cert, Identifier pid, String checksumAlgorithm) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, InvalidRequest, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    
    public File getObjectListFile() {
        return objectListFile;
    }

    public void setObjectListFile(File objectListFile) {
        this.objectListFile = objectListFile;
    }

    @Override
    public ObjectList listObjects(Session cert, Date startTime, Date endTime, ObjectFormatIdentifier objectFormatId, Boolean replicaStatus, Integer start, Integer count) throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure {
        try {
            return TypeMarshaller.unmarshalTypeFromFile(ObjectList.class, objectListFile);
        } catch (IOException ex) {
            throw new ServiceFailure("4801", ex.getClass().getName() + ": " + ex.getMessage());
        } catch (InstantiationException ex) {
            throw new ServiceFailure("4801", ex.getClass().getName() + ": " + ex.getMessage());
        } catch (IllegalAccessException ex) {
            throw new ServiceFailure("4801", ex.getClass().getName() + ": " + ex.getMessage());
        } catch (MarshallingException ex) {
            throw new ServiceFailure("4801", ex.getClass().getName() + ": " + ex.getMessage());
        }
    }

    @Override
    public boolean synchronizationFailed(Session session, SynchronizationFailed message) throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public InputStream get(Identifier pid) throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound, InsufficientResources {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SystemMetadata getSystemMetadata(Identifier pid) throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public DescribeResponse describe(Identifier pid) throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Checksum getChecksum(Identifier pid, String checksumAlgorithm) throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ObjectList listObjects(Date fromDate, Date toDate, ObjectFormatIdentifier formatid, Boolean replicaStatus, Integer start, Integer count) throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean synchronizationFailed(SynchronizationFailed message) throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public InputStream getReplica(Identifier pid) throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound, InsufficientResources {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public InputStream getReplica(Session session, Identifier pid) throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound, InsufficientResources {
        throw new UnsupportedOperationException("Not supported yet.");
    }


}
