/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.packager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.proto.packager.types.MergeMap;
import org.dataone.cn.batch.proto.packager.types.DataPersistenceKeys;
import org.dataone.service.cn.CoordinatingNodeRegister;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.ObjectFormat;
import org.jibx.runtime.JiBXException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 * @author rwaltz
 */
public class MetadataPackageWriter {

    Logger logger = Logger.getLogger(MetadataPackageWriter.class.getName());
    private String readMetacatDirectory;
    private String writeDirectory;
    private MergeMap readMap;
    private Map<String, String> scienceMetadataFormatPathMap;
    private HashMap<String, File> mergedMetaDir = new HashMap<String, File>();
    CoordinatingNodeRegister coordinatingNodeRegister;
    AuthToken cnToken;
    org.dataone.service.types.NodeList nodeList;
    private DataPersistenceWriter dataPersistenceWriter;
    private List<ObjectFormat> validSciMetaObjectFormats;
    private String cnWebUrl;
    private Map<String, String> metacatObjectFormatSkin;
    //  number of hours to search for
    private int hoursToExpire = 12;

    public void writePackages() {
        logger.info("start write for " + readMap.keySet().size() + " number of packages");
        int writtenPackages = 0;
        // Copy into a new set or else receive nasty error
        Set<String> readSetQueue = new HashSet<String>(readMap.keySet());
        try {
            // get the most up-to-date dataone cn registry of nodes
            nodeList = coordinatingNodeRegister.listNodes(cnToken);
            for (String key : readSetQueue) {
                Map<String, String> mergeFiles = readMap.get(key);
                // Determine if the record contains both SciMeta and SysMeta. if so, write out.
                // if not, set or check expiration date of incomplete record

                if (mergeFiles.containsKey(DataPersistenceKeys.SCIMETA.toString()) && mergeFiles.containsKey(DataPersistenceKeys.SYSMETA.toString())) {
                    logger.debug("found: scimetadata: " + mergeFiles.get(DataPersistenceKeys.SCIMETA.toString()) + ": sysmetadata: " + mergeFiles.get(DataPersistenceKeys.SYSMETA.toString()));
                    if (this.writePackage(mergeFiles.get(DataPersistenceKeys.SCIMETA.toString()), mergeFiles.get(DataPersistenceKeys.SYSMETA.toString()))) {
                        ++writtenPackages;
                        logger.debug("wrote: scimetadata: " + mergeFiles.get(DataPersistenceKeys.SCIMETA.toString()) + ": sysmetadata: " + mergeFiles.get(DataPersistenceKeys.SYSMETA.toString()));
                    }

                    // record has been written, remove from processing queue
                    readMap.remove(key);
                } else {
                    // Check the Expiration Date of the incomplete record
                    if (mergeFiles.containsKey("EXPIRE_DATE_LONG")) {
                        Date now = new Date();
                        Long expireDateLong = Long.parseLong(mergeFiles.get("EXPIRE_DATE_LONG"));
                        Date expireDate = new Date(expireDateLong.longValue());
                        if (now.after(expireDate)) {
                            // Expiration Date has passed
                            DateFormat df = DateFormat.getDateInstance(DateFormat.LONG);
                            logger.fatal("GUID: " + key + " HAS FAILED: " + df.format(expireDate) + " is now past due !!! DELETING RECORD. THIS SHOULD BE REPORTED TO THE AUTHORITIES");
                            readMap.remove(key);
                        } else {
                            // Expiration Date has not passed
                            logger.debug("GUID: " + key + " is not yet complete!");
                        }

                    } else {
                        // Create the Expiration Date of the incomplete record
                        Date now = new Date();

                        Long expireDateLong = new Long(now.getTime() + (hoursToExpire * 60 * 60 * 1000));
                        logger.warn("GUID: " + key + " is not yet complete!");
                        mergeFiles.put("EXPIRE_DATE_LONG", expireDateLong.toString());
                    }
                }
            }
        } catch (FileNotFoundException ex) {
            logger.error(ex);
        } catch (IOException ex) {
            logger.error(ex);
        } catch (SAXException ex) {
            logger.error(ex);
        } catch (JiBXException ex) {
            logger.error(ex);
        } catch (ParserConfigurationException ex) {
            logger.error(ex);
        } catch (NotImplemented ex) {
            logger.error(ex);
        } catch (ServiceFailure ex) {
            logger.error(ex);
        } catch (WritePackageException ex) {
            logger.error(ex);
        }
        try {
            dataPersistenceWriter.writePersistentData();
        } catch (FileNotFoundException ex) {
            logger.error(ex);
        } catch (IOException ex) {
            logger.error(ex);
        } catch (Exception ex) {
            logger.error(ex);
        }
        logger.info("wrote " + writtenPackages + " number of packages");

    }

    private boolean writePackage(String scienceMetadataFile, String systemMetadataFile) throws JiBXException, IOException, ParserConfigurationException, SAXException, WritePackageException {

// TODO code application logic here

        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder parser = documentBuilderFactory.newDocumentBuilder();
        Document sciMeta = null;
        Document sysMeta = null;
        org.dataone.service.types.Node d1Node = null;
        try {
            sysMeta = parser.parse(new File(readMetacatDirectory + File.separator + systemMetadataFile));
        } catch (SAXException ex) {
            logger.error(ex.getMessage(), ex);
            throw ex;
        }
        Element mercury = null;
        String objectFormat = "";

        // Object Format is needed for producing subdirectories under the project directory
        Element sysMetaRoot = sysMeta.getDocumentElement();
        NodeList sysNodeList = sysMetaRoot.getElementsByTagName("objectFormat");
        for (int i = 0; i < sysNodeList.getLength(); ++i) {
            objectFormat = sysNodeList.item(i).getTextContent();
        }
        if (objectFormat.isEmpty()) {
            throw new WritePackageException("ObjectFormat:" + objectFormat + ": of file " + systemMetadataFile + " is not valid");
        }
        ObjectFormat objectFormatEnum = ObjectFormat.convert(objectFormat);
        if ((objectFormatEnum != null) && validSciMetaObjectFormats.contains(objectFormatEnum)) {
            try {
                sciMeta = parser.parse(new File(readMetacatDirectory + File.separator + scienceMetadataFile));
            } catch (IOException ex) {
                logger.error(ex.getMessage(), ex);
                throw ex;
            }
        } else {
            if (objectFormatEnum == null) {
                System.out.println("ObjectFormat:" + objectFormat + " is missing from ObjectFormat Enumeration!!!!");
                logger.error("ObjectFormat:" + objectFormat + " is missing from ObjectFormat Enumeration!!!!");
            } else {
                logger.warn("ObjectFormat:" + objectFormat + " can not be indexed");
            }
            return false;
        }
        // Mercury has this idea of Projects by which it groups metadata
        // currently, our translation of a project is the 
        // originating membernode of the data is the 'project' that 
        // metadata should be grouped into
        // XXX THE ABOVE ASSUMPTION SHOULD BE REVIEWED
        //
        String originMemberNode = "";  // this should be a node identifier
        sysNodeList = sysMetaRoot.getElementsByTagName("originMemberNode");
        for (int i = 0; i < sysNodeList.getLength(); ++i) {
            originMemberNode = sysNodeList.item(i).getTextContent();
        }
        if (objectFormat.isEmpty()) {
            throw new WritePackageException("ObjectFormat:" + objectFormat + ": of file " + systemMetadataFile + " is not valid");
        }
        // Harzards of Languange we decided to use, Node/NodeList objects
        // in two different packages

        List<org.dataone.service.types.Node> nodes = nodeList.getNodeList();
        for (org.dataone.service.types.Node node : nodes) {
            if (node.getIdentifier().getValue().contentEquals(originMemberNode)) {
                d1Node = node;
                break;
            }
        }
        if (d1Node == null) {
            throw new WritePackageException("Can not determine node with id: " + originMemberNode + " from NodeList");
        }

        Node adoptedMetadata = sciMeta.importNode(sysMeta.getDocumentElement(), true);
        adoptedMetadata = sciMeta.renameNode(adoptedMetadata, "", "systemMetadata");
        Element root = sciMeta.getDocumentElement();
        NodeList mercuryNodeList = root.getElementsByTagName("mercury");
        if (mercuryNodeList.getLength() > 0) {
            mercury = (Element) mercuryNodeList.item(mercuryNodeList.getLength() - 1);
            mercury.appendChild(adoptedMetadata);
        } else {
            mercury = sciMeta.createElement("mercury");
            mercury.appendChild(adoptedMetadata);
            sciMeta.getDocumentElement().appendChild(mercury);
        }

        // XXX
        // This is needed for mercury to present the full page of the sci meta data
        // correctly to a user of mercury, is set to the 'web_url' field in solr
        Element metacatWebUrl = sciMeta.createElement("metacatWebUrl");
        // XXX default is hardcoded
        // String webUrl = cnWebUrl + "/" + scienceMetadataFile + "/" + metacatObjectFormatSkin.get(objectFormat);
        String webUrl = cnWebUrl + "/" + scienceMetadataFile + "/default";
        metacatWebUrl.setTextContent(webUrl);
        sciMeta.getDocumentElement().appendChild(metacatWebUrl);

//           sciMeta.normalizeDocument();
        // the full path to place the merged metadata for mercury should be
        // mercury base directory + project name + metadata type name (objectFormat)
        //
        // cross reference with the node to determine the directory path
        String mergedMetadataDirPath = writeDirectory + File.separator
                + d1Node.getName().replaceAll("[\\W]+", "-") + File.separator
                + getScienceMetadataFormatPathMap().get(objectFormat);

        // this is all for optimization, if the directory path exists
        // in this map, then we don't have to go through all the logic
        // to determine if it has already been created, etc
        if (!mergedMetaDir.containsKey(mergedMetadataDirPath)) {
            File mergedMetadataDir = new File(mergedMetadataDirPath);
            if (mergedMetadataDir.exists() && mergedMetadataDir.isDirectory()) {
                mergedMetaDir.put(mergedMetadataDirPath, mergedMetadataDir);
            } else {

                if (mergedMetadataDir.getParentFile().getParentFile().exists()) {
                    if (!mergedMetadataDir.getParentFile().exists()) {
                        if (mergedMetadataDir.getParentFile().mkdir()) {
                            logger.info("created " + mergedMetadataDir.getParentFile().getAbsolutePath());
                        } else {
                            throw new WritePackageException("Unable to create parent directory " + mergedMetadataDir.getParentFile());
                        }
                    }
                    if (mergedMetadataDir.mkdir()) {
                        logger.info("created " + mergedMetadataDirPath);
                    } else {
                        throw new WritePackageException("Unable to create directory :" + mergedMetadataDirPath + ":");
                    }
                } else {
                    throw new WritePackageException("Top Level Metadata Merge directory :" + writeDirectory + ": does not exist");
                }
                mergedMetaDir.put(mergedMetadataDirPath, mergedMetadataDir);
            }
        }
        // finally create the file in the correct directory with the correct name
        String mergedMetadata = systemMetadataFile.concat("_MERGED.xml");
        writeXmlFile(sciMeta, mergedMetadataDirPath + File.separator + mergedMetadata);

        return true;
    }

    private void writeXmlFile(Document doc, String filename) {

        try {
// Prepare the DOM document for writing
            Source source = new DOMSource(doc);

// Prepare the output file
            File file = new File(filename);

            Result result = new StreamResult(file);
// Write the DOM document to the file
            Transformer xformer = TransformerFactory.newInstance().newTransformer();

            xformer.transform(source, result);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public String getReadMetacatDirectory() {
        return readMetacatDirectory;
    }

    public void setReadMetacatDirectory(String readMetacatDirectory) {
        this.readMetacatDirectory = readMetacatDirectory;
    }

    public String getWriteDirectory() {
        return writeDirectory;
    }

    public void setWriteDirectory(String writeDirectory) {
        this.writeDirectory = writeDirectory;
    }

    public MergeMap getReadQueue() {
        return readMap;
    }

    public void setReadQueue(MergeMap readMap) {
        this.readMap = readMap;
    }

    public Map<String, String> getScienceMetadataFormatPathMap() {
        return scienceMetadataFormatPathMap;
    }

    public void setScienceMetadataFormatPathMap(Map<String, String> scienceMetadataFormatPathMap) {
        this.scienceMetadataFormatPathMap = scienceMetadataFormatPathMap;
    }

    public CoordinatingNodeRegister getCoordinatingNodeRegister() {
        return coordinatingNodeRegister;
    }

    public void setCoordinatingNodeRegister(CoordinatingNodeRegister coordinatingNodeRegister) {
        this.coordinatingNodeRegister = coordinatingNodeRegister;
    }

    public DataPersistenceWriter getMetadataPackageAccess() {
        return dataPersistenceWriter;
    }

    public void setMetadataPackageAccess(DataPersistenceWriter dataPersistenceWriter) {
        this.dataPersistenceWriter = dataPersistenceWriter;
    }

    public List<ObjectFormat> getValidSciMetaObjectFormats() {
        return validSciMetaObjectFormats;
    }

    public void setValidSciMetaObjectFormats(List<ObjectFormat> validSciMetaObjectFormats) {
        this.validSciMetaObjectFormats = validSciMetaObjectFormats;
    }

    public String getCnWebUrl() {
        return cnWebUrl;
    }

    public void setCnWebUrl(String cnWebUrl) {
        this.cnWebUrl = cnWebUrl;
    }

    public Map<String, String> getMetacatObjectFormatSkin() {
        return metacatObjectFormatSkin;
    }

    public void setMetacatObjectFormatSkin(Map<String, String> metacatObjectFormatSkin) {
        this.metacatObjectFormatSkin = metacatObjectFormatSkin;
    }

    public int getHoursToExpire() {
        return hoursToExpire;
    }

    public void setHoursToExpire(int hoursToExpire) {
        this.hoursToExpire = hoursToExpire;
    }

    public AuthToken getCnToken() {
        return cnToken;
    }

    public void setCnToken(AuthToken cnToken) {
        this.cnToken = cnToken;
    }

    private class WritePackageException extends Exception {

        WritePackageException(String s) {
            super(s);
        }
    }
}
