/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.harvest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.utils.NodeListAccess;
import org.dataone.cn.batch.utils.NodeReference;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
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
import org.dataone.service.types.Node;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.SystemMetadata;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.JiBXException;

/**
 *
 * @author rwaltz
 */
public class ObjectListQueueWriter {

    /**
     *
     *
    1) testCreateData() & testCreateScienceMetadata() do not use the setAccess method.  But, testGet() uses it. How does this work?

    they don't need setAccess because they use the authenticated sessionid to do the testing

    if you wanted to pull the created docs as user public, you'd have to use the setAccess() method
    like in testGet()

    2) if objects are created as user public, then you don't have to use setAccess to get?
    you can't create as user public
    you must be logged in


    3) Do all records have access set as public when they are created, regardless of the user?

    no, if you create something as uid=kepler, then by default only uid=kepler can read it
    so you have to setAccess() to public for the "public" user to be able to read it
    the public user isn't really a user at all.  just a sessionid of 0 or null


    Executive summary:
    1) only logged in users can create
    2) by default, only that logged in user can read the created doc
    (including with listObjects)
    3) you must setAccess to public if you want anonymous users to be able to read the doc

    so you see in testGet(),
    I create the doc as user "kepler"
    then I can read it with the kepler token
    but as I setAccess to public read
    I can use the publicToken to read it
     *
     *
     */
    Logger logger = Logger.getLogger(ObjectListQueueWriter.class.getName());
    private MemberNodeCrud mnReader;
    private MemberNodeCrud cnWriter;
    private Map<Identifier, SystemMetadata> readQueue;
    private List<ObjectFormat> validSciMetaObjectFormats;
    private NodeReference nodeReferenceUtility;
    private NodeListAccess nodeListAccess;
    private AuthToken token;

    public void writeQueue() throws Exception {
        boolean hasException;
        //    File sciMetaFile = null;
        InputStream sciMetaStream = null;
        File sysMetaFile = null;

        Node mnNode = nodeReferenceUtility.getMnNode();
        Date lastMofidiedDate = nodeReferenceUtility.getMnNode().getSynchronization().getLastHarvested();

        try {
            for (Iterator<Identifier> it = readQueue.keySet().iterator(); it.hasNext();) {
                Identifier identifier = it.next();
                hasException = true;

                // is the object format a data object or a scimetadata object
                // tag in system metadata object we are using the objectFormat,
                // need a mapping between objectformat and objectType
                //
                SystemMetadata systemMetadata = readQueue.get(identifier);
                ObjectFormat objectFormat = systemMetadata.getObjectFormat();
                if (validSciMetaObjectFormats.contains(objectFormat)) {
                    sciMetaStream = mnReader.get(null, identifier);
                } else {
                    sciMetaStream = null;
                }

                // Verify that the ordering is correct or blow up
                if (systemMetadata.getDateSysMetadataModified().before(lastMofidiedDate)) {
                    throw new Exception("Dates are not ordered correctly!");
                }

                this.writeToMetacat(sciMetaStream, readQueue.get(identifier));

                // XXX IN THE FUTURE TO VERIFY THAT EVERYTHING
                // is printed write to a log discrete log file?
                logger.info("sent metacat " + identifier.getValue() + " with sci meta? ");
                logger.info(sciMetaStream == null ? "no" : "yes");
                hasException = false;
                lastMofidiedDate = systemMetadata.getDateSysMetadataModified();
                     try {
                        if (sciMetaStream != null) {
                            sciMetaStream.close();
                            sciMetaStream = null;
                        }

                    } catch (IOException ex) {
                        logger.error(ex.getMessage(), ex);
                    }
            }

        } catch (IdentifierNotUnique ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (UnsupportedType ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (InsufficientResources ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (InvalidSystemMetadata ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (InvalidToken ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (ServiceFailure ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (NotAuthorized ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (NotFound ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (NotImplemented ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        } finally {
            try {
                if (sciMetaStream != null) {
                    sciMetaStream.close();
                }

            } catch (IOException ex) {
                logger.error(ex.getMessage(), ex);
            }
        }
        if (lastMofidiedDate.after(mnNode.getSynchronization().getLastHarvested())) {
            nodeReferenceUtility.getMnNode().getSynchronization().setLastHarvested(lastMofidiedDate);
            nodeListAccess.setNodeList(nodeReferenceUtility.getMnNodeList());
            nodeListAccess.persistNodeListToFileSystem();

        }
    }

    public void writeToMetacat(InputStream objectInputStream, SystemMetadata sysmeta) throws ServiceFailure  {
        Identifier guid = new Identifier();
// get the system metadata from the system
        Identifier d1Identifier = null;
        guid.setValue(sysmeta.getIdentifier().getValue());
        try {
            d1Identifier = cnWriter.create(token, guid, objectInputStream, sysmeta);
        } catch (InvalidToken ex) {
            logger.error("d1client.create:\n" + ex.serialize(ex.FMT_XML));
        } catch (ServiceFailure ex) {
             logger.error("d1client.create:\n" + ex.serialize(ex.FMT_XML));
        } catch (NotAuthorized ex) {
             logger.error("d1client.create:\n" + ex.serialize(ex.FMT_XML));
        } catch (IdentifierNotUnique ex) {
             logger.error("d1client.create:\n" + ex.serialize(ex.FMT_XML));
        } catch (UnsupportedType ex) {
            logger.error("d1client.create:\n" + ex.serialize(ex.FMT_XML));
        } catch (InsufficientResources ex) {
             logger.error("d1client.create:\n" + ex.serialize(ex.FMT_XML));
        } catch (InvalidSystemMetadata ex) {
             logger.error("d1client.create:\n" + ex.serialize(ex.FMT_XML));
        } catch (NotImplemented ex) {
             logger.error("d1client.create:\n" + ex.serialize(ex.FMT_XML));
        }
        if (d1Identifier != null) {
            cnWriter.setAccess(token, guid, "public", "read", "allow", "allowFirst");
            logger.info("create success, id returned is " + d1Identifier.getValue());
        }
    }

    private File writeSystemMetadataToFile(SystemMetadata systemMetadata, String filenamePath) throws JiBXException, FileNotFoundException {
        IBindingFactory bfact = BindingDirectory.getFactory(org.dataone.service.types.SystemMetadata.class);

        IMarshallingContext mctx = bfact.createMarshallingContext();
        File outputFile = new File(filenamePath);
        FileOutputStream testSytemMetadataOutput = new FileOutputStream(outputFile);

        mctx.marshalDocument(systemMetadata, "UTF-8", null, testSytemMetadataOutput);
        try {
            testSytemMetadataOutput.close();
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        }
        return outputFile;
    }

    public MemberNodeCrud getMnReader() {
        return mnReader;
    }

    public void setMnReader(MemberNodeCrud mnReader) {
        this.mnReader = mnReader;
    }

    public Map<Identifier, SystemMetadata> getReadQueue() {
        return readQueue;
    }

    public void setReadQueue(Map<Identifier, SystemMetadata> readQueue) {
        this.readQueue = readQueue;
    }

    public MemberNodeCrud getCnWriter() {
        return cnWriter;
    }

    public void setCnWriter(MemberNodeCrud cnWriter) {
        this.cnWriter = cnWriter;
    }

    public AuthToken getToken() {
        return token;
    }

    public void setToken(AuthToken token) {
        this.token = token;
    }

    public List<ObjectFormat> getValidSciMetaObjectFormats() {
        return validSciMetaObjectFormats;
    }

    public void setValidSciMetaObjectFormats(List<ObjectFormat> validSciMetaObjectFormats) {
        this.validSciMetaObjectFormats = validSciMetaObjectFormats;
    }

    public NodeReference getNodeReferenceUtility() {
        return nodeReferenceUtility;
    }

    public void setNodeReferenceUtility(NodeReference nodeReferenceUtility) {
        this.nodeReferenceUtility = nodeReferenceUtility;
    }

    public NodeListAccess getNodeListAccess() {
        return nodeListAccess;
    }

    public void setNodeListAccess(NodeListAccess nodeListAccess) {
        this.nodeListAccess = nodeListAccess;
    }
}
