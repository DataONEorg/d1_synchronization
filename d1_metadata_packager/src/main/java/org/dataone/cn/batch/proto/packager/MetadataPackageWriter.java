/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.packager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
import org.dataone.cn.batch.utils.NodeReference;
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
    NodeReference nodeReferenceUtility;
    private DataPersistenceWriter dataPersistenceWriter;
    private List<ObjectFormat> validSciMetaObjectFormats;
    public void writePackages() throws FileNotFoundException, JiBXException, IOException, ParserConfigurationException, SAXException, Exception {
        logger.info("start write for " + readMap.keySet().size() + " number of packages");
        int writtenPackages = 0;
        // Copy into a new set or else receive nasty error
        Set<String> readSetQueue = new HashSet<String>(readMap.keySet());
        for (String key : readSetQueue) {
            Map<String, String> mergeFiles = readMap.get(key);
            if (mergeFiles.containsKey(DataPersistenceKeys.SCIMETA.toString()) && mergeFiles.containsKey(DataPersistenceKeys.SYSMETA.toString())) {
                logger.debug("found: scimetadata: " + mergeFiles.get(DataPersistenceKeys.SCIMETA.toString()) + ": sysmetadata: " + mergeFiles.get(DataPersistenceKeys.SYSMETA.toString()));
                if (this.writePackage(mergeFiles.get(DataPersistenceKeys.SCIMETA.toString()), mergeFiles.get(DataPersistenceKeys.SYSMETA.toString()))) {
                    ++writtenPackages;
                }
                logger.debug("wrote: scimetadata: " + mergeFiles.get(DataPersistenceKeys.SCIMETA.toString()) + ": sysmetadata: " + mergeFiles.get(DataPersistenceKeys.SYSMETA.toString()));
                readMap.remove(key);
            } else {
                if (mergeFiles.containsKey("COUNT")) {
                    Integer countInt  = Integer.parseInt(mergeFiles.get("COUNT"));
                    if (countInt < 10) {
                        logger.warn("GUID: " + key + " is not yet complete!");
                    } else if ((countInt > 10) && (countInt <= 20)) {
                        logger.error("GUID: " + key + " HAS FAILED " + countInt.toString() + " TIMES!");
                    }
                    ++countInt;

                    mergeFiles.put("COUNT", countInt.toString());
                    if (countInt > 20) {
                        logger.fatal("GUID: " + key + " HAS FAILED " + countInt.toString() + " TIMES !!! DELETING RECORD. THIS SHOULD BE REPORTED TO THE AUTHORITIES");
                        readMap.remove(key);
                    }
                    
                } else {
                    logger.info("GUID: " + key + " is not yet complete!");
                    mergeFiles.put("COUNT", Integer.toString(1));
                }
            }
        }
        dataPersistenceWriter.writePersistentData();
        logger.info("wrote " + writtenPackages + " number of packages");
    }

    private boolean writePackage(String scienceMetadataFile, String systemMetadataFile) throws JiBXException, IOException, ParserConfigurationException, SAXException, Exception {

// TODO code application logic here

            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder parser = documentBuilderFactory.newDocumentBuilder();
            Document sciMeta = null;
            Document sysMeta = null;
            
            try {
                sysMeta = parser.parse(new File(readMetacatDirectory + File.separator + systemMetadataFile));
            } catch (Exception ex) {
                logger.error(ex.getMessage(), ex);
                throw ex;
            }
            Element mercury = null;
            String objectFormat = "";
            
            Element sysMetaRoot = sysMeta.getDocumentElement();
            NodeList sysNodeList = sysMetaRoot.getElementsByTagName("objectFormat");
            for (int i = 0; i < sysNodeList.getLength(); ++i) {
                objectFormat = sysNodeList.item(i).getTextContent();
            }
            if (objectFormat.isEmpty()) {
                throw new Exception("ObjectFormat:" + objectFormat + ": of file " + systemMetadataFile + " is not valid");
            }
            ObjectFormat objectFormatEnum = ObjectFormat.convert(objectFormat);
            if ((objectFormatEnum != null)  && validSciMetaObjectFormats.contains(objectFormatEnum)) {
              try {
                sciMeta = parser.parse(new File(readMetacatDirectory + File.separator + scienceMetadataFile));
               } catch (Exception ex) {
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

            Element root = sciMeta.getDocumentElement();
            NodeList mercuryNodeList = root.getElementsByTagName("mercury");
            if (mercuryNodeList.getLength() > 0) {
                mercury = (Element) mercuryNodeList.item(mercuryNodeList.getLength() - 1);
            } else {
                mercury = sciMeta.createElement("mercury");
            }

            Node adoptedMetadata = sciMeta.importNode(sysMeta.getDocumentElement(), true);
            adoptedMetadata = sciMeta.renameNode(adoptedMetadata, "", "systemMetadata");
            mercury.appendChild(adoptedMetadata);
            // TODO this should only append the mercury element to the document root if the mercury element does not exist
            sciMeta.getDocumentElement().appendChild(mercury);
//           sciMeta.normalizeDocument();
            // need to retrieve the systemmetadata to get GUID and ObjectType to
            // cross reference with the node to determine the directory path
            String mergedMetadataDirPath = writeDirectory + File.separator
                    + nodeReferenceUtility.getNodeListMergeDirPath() + File.separator
                    + getScienceMetadataFormatPathMap().get(objectFormat);
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
                                 throw new Exception("Unable to create parent directory " + mergedMetadataDir.getParentFile());
                            }
                        }
                        if (mergedMetadataDir.mkdir()) {
                            logger.info("created " + mergedMetadataDirPath);
                        } else {
                            throw new Exception("Unable to create directory :" + mergedMetadataDirPath + ":");
                        }
                    } else {
                        throw new Exception("Top Level Metadata Merge directory :" + writeDirectory + ": does not exist");
                    }
                    mergedMetaDir.put(mergedMetadataDirPath, mergedMetadataDir);
                }
            }

            String mergedMetadata = systemMetadataFile.concat("_MERGED.xml");
            writeXmlFile(sciMeta,  mergedMetadataDirPath + File.separator + mergedMetadata);

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
            logger.error(e.getMessage(),e);
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

    public NodeReference getNodeReferenceUtility() {
        return nodeReferenceUtility;
    }

    public void setNodeReferenceUtility(NodeReference nodeReferenceUtility) {
        this.nodeReferenceUtility = nodeReferenceUtility;
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
}
