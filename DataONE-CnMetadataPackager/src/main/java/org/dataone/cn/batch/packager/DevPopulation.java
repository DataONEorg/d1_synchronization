/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.packager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.dataone.client.D1Client;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Identifier;
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
public class DevPopulation {


    private D1Client d1;

    private String readDirectory;
    private String writeDirectory;

    public void buildMetadata( String scienceMetadataFile, String systemMetadataFile) throws FileNotFoundException, JiBXException, IOException {

        AuthToken token = new AuthToken("public");
        Identifier guid = new Identifier();
// get the system metadata from the system

        SystemMetadata sysmeta = getSystemMetadata(systemMetadataFile);
        guid.setValue(sysmeta.getIdentifier().getValue());

        InputStream objectStream = new FileInputStream(readDirectory + scienceMetadataFile);

       try {
// create the object in metacat??

            Identifier d1Identifier = d1.create(token, guid, objectStream, sysmeta);

            System.out.println("create success, id returned is " + d1Identifier.getValue());
        } catch (InvalidToken e) {
            System.out.println(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        } catch (ServiceFailure e) {
            System.out.println(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        } catch (NotAuthorized e) {
            System.out.println(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        } catch (IdentifierNotUnique e) {
            System.out.println(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        } catch (UnsupportedType e) {
            System.out.println(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        } catch (InsufficientResources e) {
            System.out.println(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        } catch (InvalidSystemMetadata e) {
            System.out.println(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        } catch (NotImplemented e) {
            System.out.println(e.serialize(e.FMT_XML));
            e.printStackTrace();
            return;
        }

// TODO code application logic here
        try {

            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder parser = documentBuilderFactory.newDocumentBuilder();
            Document sciMeta;
            Document sysMeta;

            sciMeta = parser.parse(new File(readDirectory + scienceMetadataFile));
            sysMeta = parser.parse(new File(readDirectory + systemMetadataFile));
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
            String mergedSciMetadata = scienceMetadataFile.replaceAll("\\.xml", "_SYSMETAD1.xml");
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
             reader = new FileReader(readDirectory + metadataFile);

            systemMetadata = (SystemMetadata) uctx.unmarshalDocument(reader);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            reader.close();
        }
    return systemMetadata;
    }

    public D1Client getD1() {
        return d1;
    }

    public void setD1(D1Client d1) {
        this.d1 = d1;
    }

    public String getReadDirectory() {
        return readDirectory;
    }

    public void setReadDirectory(String readDirectory) {
        this.readDirectory = readDirectory;
    }

    public String getWriteDirectory() {
        return writeDirectory;
    }

    public void setWriteDirectory(String writeDirectory) {
        this.writeDirectory = writeDirectory;
    }


}
