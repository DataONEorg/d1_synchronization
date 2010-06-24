/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.proto.packager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.dataone.service.types.SystemMetadata;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
/**
 *
 * @author rwaltz
 */
public class MetadataPackageBuilder {

    private String readObjectDirectory;
    private String readMetadataDirectory;
    private String writeDirectory;
    private Map<String, String> readQueue;

    public void processQueue() throws FileNotFoundException, JiBXException, IOException {
        for (String key: this.readQueue.keySet()) {
           this.buildPackage(key, this.readQueue.get(key));
        }
    }

    public void buildPackage( String scienceMetadataFile, String systemMetadataFile) throws FileNotFoundException, JiBXException, IOException {

// TODO code application logic here
        try {

            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder parser = documentBuilderFactory.newDocumentBuilder();
            Document sciMeta;
            Document sysMeta;

            sciMeta = parser.parse(new File(readObjectDirectory + scienceMetadataFile));
            sysMeta = parser.parse(new File(readMetadataDirectory + systemMetadataFile));
            Element mercury = null;
            Element root = sciMeta.getDocumentElement();
            NodeList mercuryNodeList = root.getElementsByTagName("mercury");
            if (mercuryNodeList.getLength() > 0) {
                mercury = (Element)mercuryNodeList.item(mercuryNodeList.getLength() -1);
            } else {
                mercury = sciMeta.createElement("mercury");
            }

           Node adoptedMetadata = sciMeta.importNode(sysMeta.getDocumentElement(), true);
           adoptedMetadata = sciMeta.renameNode(adoptedMetadata, "", "systemMetadata");
            mercury.appendChild(adoptedMetadata);
            sciMeta.getDocumentElement().appendChild(mercury);
//        sciMeta.normalizeDocument();
            String mergedSciMetadata = scienceMetadataFile.concat("_MERGED.xml");
            writeXmlFile(sciMeta, writeDirectory + mergedSciMetadata );

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void writeXmlFile(Document doc, String filename) {

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
            e.printStackTrace();
        }
    }

    public SystemMetadata getSystemMetadata(String metadataFile) throws JiBXException, IOException {
        Reader reader = null;
        SystemMetadata  systemMetadata = null;
        try {
            IBindingFactory bfact =
                    BindingDirectory.getFactory(org.dataone.service.types.SystemMetadata.class);

            IMarshallingContext mctx = bfact.createMarshallingContext();
            IUnmarshallingContext uctx = bfact.createUnmarshallingContext();
             reader = new FileReader(readMetadataDirectory + metadataFile);

            systemMetadata = (SystemMetadata) uctx.unmarshalDocument(reader);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            reader.close();
        }
        return systemMetadata;
    }

    public String getReadMetadataDirectory() {
        return readMetadataDirectory;
    }

    public void setReadMetadataDirectory(String readMetadataDirectory) {
        this.readMetadataDirectory = readMetadataDirectory;
    }

    public String getReadObjectDirectory() {
        return readObjectDirectory;
    }

    public void setReadObjectDirectory(String readObjectDirectory) {
        this.readObjectDirectory = readObjectDirectory;
    }



    public String getWriteDirectory() {
        return writeDirectory;
    }

    public void setWriteDirectory(String writeDirectory) {
        this.writeDirectory = writeDirectory;
    }

    public Map getReadQueue() {
        return readQueue;
    }

    public void setReadQueue(Map readQueue) {
        this.readQueue = readQueue;
    }

}
