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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import org.apache.commons.codec.net.URLCodec;
import org.apache.log4j.Logger;
import org.dataone.service.cn.v2.CNCore;
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
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.types.v1.ChecksumAlgorithmList;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v2.Log;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;
import org.dataone.service.types.v2.ObjectFormat;
import org.dataone.service.types.v2.ObjectFormatList;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.dataone.exceptions.MarshallingException;
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
    static final Logger logger = Logger.getLogger(MockCNCore.class);
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
        } catch (MarshallingException ex) {
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
    public NodeList listNodes() throws NotImplemented, ServiceFailure {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier registerSystemMetadata(Session session, Identifier pid, SystemMetadata sysmeta) throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, InvalidSystemMetadata {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Log getLogRecords(Session session, Date fromDate, Date toDate, String event, String pidFilter, Integer start, Integer count) throws InvalidToken, InvalidRequest, ServiceFailure, NotAuthorized, NotImplemented, InsufficientResources {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    @Override
    public Identifier generateIdentifier(Session session, String scheme, String fragment) throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier delete(Session session, Identifier pid)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier reserveIdentifier(Session session, Identifier pid) throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ObjectFormatList listFormats() throws ServiceFailure, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ObjectFormat getFormat(ObjectFormatIdentifier formatid) throws ServiceFailure, NotFound, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Date ping() throws NotImplemented, ServiceFailure, InsufficientResources {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ChecksumAlgorithmList listChecksumAlgorithms() throws ServiceFailure, NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean setObsoletedBy(Session session, Identifier pid, Identifier obsoletedByPid, long serialVersion) throws NotImplemented, NotFound, NotAuthorized, ServiceFailure, InvalidRequest, InvalidToken, VersionMismatch {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean hasReservation(Session session, Subject subject, Identifier pid) throws InvalidToken, ServiceFailure, NotFound, NotAuthorized, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

	@Override
	public Node getCapabilities() throws NotImplemented, ServiceFailure {
        throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public boolean updateSystemMetadata(Session session, Identifier pid,
			SystemMetadata sysmeta) throws NotImplemented, NotAuthorized,
			ServiceFailure, InvalidRequest, InvalidSystemMetadata, InvalidToken {
        throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public Identifier archive(Session session, Identifier id)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
			NotImplemented {
        throw new UnsupportedOperationException("Not supported yet.");

	}


	@Override
	public ObjectFormatIdentifier addFormat(Session session,
			ObjectFormatIdentifier formatid, ObjectFormat format)
			throws ServiceFailure, NotFound, NotImplemented, InvalidRequest,
			NotAuthorized, InvalidToken {
        throw new UnsupportedOperationException("Not supported yet.");

	}


	@Override
	public boolean synchronize(Session session, Identifier pid)
			throws NotImplemented, NotAuthorized, ServiceFailure,
			InvalidRequest, InvalidSystemMetadata, InvalidToken {
        throw new UnsupportedOperationException("Not supported yet.");

	}


}
