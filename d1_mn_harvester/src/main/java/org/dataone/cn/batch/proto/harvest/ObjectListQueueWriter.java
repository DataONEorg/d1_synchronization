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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.utils.NodeListAccess;
import org.dataone.cn.batch.utils.NodeReference;
import org.dataone.service.cn.CoordinatingNodeCrud;
import org.dataone.service.cn.CoordinatingNodeAuthorization;
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
    private CoordinatingNodeCrud cnWriter;
    private CoordinatingNodeAuthorization cnAuthorization;
    private Map<Identifier, SystemMetadata> readQueue;
    private List<ObjectFormat> validSciMetaObjectFormats;
    private NodeReference nodeReferenceUtility;
    private NodeListAccess nodeListAccess;
    private AuthToken token;

    public void writeQueue() throws Exception {

        //    File sciMetaFile = null;
        InputStream sciMetaStream = null;
        File sysMetaFile = null;

        Node mnNode = nodeReferenceUtility.getMnNode();
        Date lastMofidiedDate = nodeReferenceUtility.getMnNode().getSynchronization().getLastHarvested();

        if (readQueue == null) {
            throw new Exception("readQueue is null!!!!!");
        }
        if (readQueue.keySet() == null) {
            throw new Exception("readQueue KeySet is null!!!!!");
        }
        if (readQueue.keySet().iterator() == null) {
            throw new Exception("readQueue KeySet iterator is null!!!!!");
        }
        try {
            for (Iterator<Identifier> it = readQueue.keySet().iterator(); it.hasNext();) {

                Identifier identifier = it.next();
                logger.debug("Write " + identifier.getValue() + " to metacat");


                // is the object format a data object or a scimetadata object
                //
                // we are using the objectFormat to determine if object is scimeta or scidata
                //
                // XXX we need a better mapping between objectformat and objectType
                // currenly it is static xml file, should be a service somewhere
                //
                SystemMetadata systemMetadata = readQueue.get(identifier);
                ObjectFormat objectFormat = systemMetadata.getObjectFormat();
                logger.debug("Writing systemMetadata to metacat: " + systemMetadata.getIdentifier().getValue() + " with Format of " + objectFormat.name());

                if (validSciMetaObjectFormats.contains(objectFormat)) {
                    sciMetaStream = mnReader.get(null, identifier);
                } else {
                    sciMetaStream = null;
                }

                // TODO figure out the problem below
                // Verify that the ordering is correct or blow up
                // oh no!!!
                // Dates can not be ordered correctly now because
                // we are only processing X # of items per batch
                // from a returned fetch
                if ((systemMetadata.getDateSysMetadataModified().getTime() > lastMofidiedDate.getTime())) {
                    lastMofidiedDate = systemMetadata.getDateSysMetadataModified();
//                  throw new Exception("Dates are not ordered correctly! " + convertDateToGMT(systemMetadata.getDateSysMetadataModified()) + " " + systemMetadata.getDateSysMetadataModified().getTime()+ "of record: " + identifier + " is before previous lastModifieddate of " + convertDateToGMT(lastMofidiedDate) + " " + lastMofidiedDate.getTime());
                }

                if (this.writeToMetacat(sciMetaStream, systemMetadata)) {

                    // XXX IN THE FUTURE TO VERIFY THAT EVERYTHING
                    // is printed write to a discrete log file?
                    logger.info("Sent metacat " + identifier.getValue() + " with DateSysMetadataModified of " + convertDateToGMT(systemMetadata.getDateSysMetadataModified()) + " with sci meta? ");
                    logger.info(sciMetaStream == null ? "no" : "yes");
                } else {
                    logger.warn("Metacat rejected object");
                }
                try {
                    if (sciMetaStream != null) {
                        sciMetaStream.close();
                        sciMetaStream = null;
                    }
                } catch (IOException ex) {
                    logger.error(ex.getMessage(), ex);
                }

            }

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
            lastMofidiedDate.setTime(lastMofidiedDate.getTime() + (1000 - (lastMofidiedDate.getTime() % 1000)));
            nodeReferenceUtility.getMnNode().getSynchronization().setLastHarvested(lastMofidiedDate);
            nodeListAccess.setNodeList(nodeReferenceUtility.getMnNodeList());
            nodeListAccess.persistNodeListToFileSystem();
        }
        readQueue.clear();
    }

    private boolean writeToMetacat(InputStream objectInputStream, SystemMetadata sysmeta) throws ServiceFailure, InvalidToken, NotFound, NotAuthorized, NotImplemented, InvalidRequest {
        Identifier pid = new Identifier();
        boolean status = false;
// get the system metadata from the system
        Identifier d1Identifier = null;
        pid.setValue(sysmeta.getIdentifier().getValue());
        try {
            d1Identifier = cnWriter.create(token, pid, objectInputStream, sysmeta);
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
            if (cnAuthorization.setAccess(token, pid, "public", "read", "allow", "allowFirst")) {
                status = true;
                logger.info("create success, id returned is " + d1Identifier.getValue());
            } else {
                logger.error("setAccess failed for pid: " + pid.getValue());
            }
        } else {
            logger.error("create failed for pid:" + pid.getValue());
        }
        return status;
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

    private String convertDateToGMT(Date d) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT-0"));
        String s = dateFormat.format(d);
        return s;
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

    public CoordinatingNodeCrud getCnWriter() {
        return cnWriter;
    }

    public void setCnWriter(CoordinatingNodeCrud cnWriter) {
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

    public CoordinatingNodeAuthorization getCnAuthorization() {
        return cnAuthorization;
    }

    public void setCnAuthorization(CoordinatingNodeAuthorization cnAuthorization) {
        this.cnAuthorization = cnAuthorization;
    }
}
