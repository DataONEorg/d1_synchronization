/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.harvester;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
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
import org.dataone.service.types.Identifier;
import org.dataone.service.types.ObjectInfo;
import org.dataone.service.types.SystemMetadata;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.JiBXException;

/**
 *
 * @author rwaltz
 */
public class MetadataQueueBuilder {

    Logger logger = Logger.getLogger(MetadataQueueBuilder.class.getName());
    private MemberNodeCrud mnReader;
    private MemberNodeCrud cnWriter;
    private List<ObjectInfo> readQueue;
    private Map<String, String> writeQueue;
    String writeObjectDirectory;
    String writeMetadataDirectory;
    private AuthToken token;

    public void buildQueue()  {
        
        int c;
        for (ObjectInfo objectInfo : readQueue) {
            FileOutputStream output = null;
            InputStream input = null;
            try {
                input = mnReader.get(null, objectInfo.getIdentifier());
                output = new FileOutputStream(this.getWriteObjectDirectory() + objectInfo.getIdentifier().getValue());
                while ((c = input.read()) != -1) {
                    output.write(c);
                }
                SystemMetadata systemMetadata = mnReader.getSystemMetadata(null, objectInfo.getIdentifier());
                File sysMetaFile = this.writeSystemMetadataToFile(systemMetadata, this.getWriteMetadataDirectory() + objectInfo.getIdentifier().getValue() );
                System.out.println("size: " + systemMetadata.getSize());
                FileInputStream fileInputStream = new FileInputStream(sysMetaFile);
                this.writeToMetacat(fileInputStream, systemMetadata);
                this.getWriteQueue().put(objectInfo.getIdentifier().getValue(), objectInfo.getIdentifier().getValue());
            } catch (InvalidToken ex) {
                ex.printStackTrace();
                logger.error(ex.serialize(ex.FMT_XML));
            } catch (ServiceFailure ex) {
                ex.printStackTrace();
                logger.error(ex.serialize(ex.FMT_XML));
            } catch (NotAuthorized ex) {
                ex.printStackTrace();
                logger.error(ex.serialize(ex.FMT_XML));
            } catch (NotFound ex) {
                ex.printStackTrace();
                logger.error(ex.serialize(ex.FMT_XML));
            } catch (NotImplemented ex) {
                ex.printStackTrace();
                logger.error(ex.serialize(ex.FMT_XML));
            } catch (InvalidRequest ex) {
                ex.printStackTrace();
                logger.error(ex.serialize(ex.FMT_XML));
            } catch (IOException ex) {
                ex.printStackTrace();
                logger.error(ex.getMessage());
            } catch (JiBXException ex) {
                ex.printStackTrace();
                logger.error(ex.getMessage());
            } finally {
                try {
                    if (input != null) {
                        input.close();
                    }
                    if (output != null) {
                        output.close();
                    }
                } catch (IOException ex) {
                    logger.error(ex.getMessage());
                }
            }
        }
    }

    public void writeToMetacat(InputStream objectInputStream, SystemMetadata sysmeta) {

        Identifier guid = new Identifier();
// get the system metadata from the system

        guid.setValue(sysmeta.getIdentifier().getValue());

       try {

            Identifier d1Identifier = cnWriter.create(token, guid, objectInputStream, sysmeta);

            System.out.println("create success, id returned is " + d1Identifier.getValue());
        } catch (InvalidToken e) {
           logger.error(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        } catch (ServiceFailure e) {
            logger.error(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        } catch (NotAuthorized e) {
            logger.error(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        } catch (IdentifierNotUnique e) {
            logger.error(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        } catch (UnsupportedType e) {
            logger.error(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        } catch (InsufficientResources e) {
            logger.error(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        } catch (InvalidSystemMetadata e) {
            logger.error(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        } catch (NotImplemented e) {
            logger.error(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        }
    }

    public File writeSystemMetadataToFile (SystemMetadata systemMetadata, String filenamePath) throws JiBXException, FileNotFoundException {
        IBindingFactory bfact = BindingDirectory.getFactory(org.dataone.service.types.SystemMetadata.class);

        IMarshallingContext mctx = bfact.createMarshallingContext();
        File outputFile = new File(filenamePath);
        FileOutputStream testSytemMetadataOutput = new FileOutputStream(outputFile);

        mctx.marshalDocument(systemMetadata, "UTF-8", null, testSytemMetadataOutput);
        try {
            testSytemMetadataOutput.close();
        } catch (IOException ex) {
            logger.error( ex);
        }
        return outputFile;
    }
    public MemberNodeCrud getMnReader() {
        return mnReader;
    }

    public void setMnReader(MemberNodeCrud mnReader) {
        this.mnReader = mnReader;
    }

    public Map<String, String> getWriteQueue() {
        return writeQueue;
    }

    public void setWriteQueue(Map<String, String> writeQueue) {
        this.writeQueue = writeQueue;
    }

    public List<ObjectInfo> getReadQueue() {
        return readQueue;
    }

    public void setReadQueue(List<ObjectInfo> readQueue) {
        this.readQueue = readQueue;
    }

    public MemberNodeCrud getCnWriter() {
        return cnWriter;
    }

    public void setCnWriter(MemberNodeCrud cnWriter) {
        this.cnWriter = cnWriter;
    }

    public String getWriteMetadataDirectory() {
        return writeMetadataDirectory;
    }

    public void setWriteMetadataDirectory(String writeMetadataDirectory) {
        this.writeMetadataDirectory = writeMetadataDirectory;
    }

    public String getWriteObjectDirectory() {
        return writeObjectDirectory;
    }

    public void setWriteObjectDirectory(String writeObjectDirectory) {
        this.writeObjectDirectory = writeObjectDirectory;
    }


    public AuthToken getToken() {
        return token;
    }

    public void setToken(AuthToken token) {
        this.token = token;
    }



}
