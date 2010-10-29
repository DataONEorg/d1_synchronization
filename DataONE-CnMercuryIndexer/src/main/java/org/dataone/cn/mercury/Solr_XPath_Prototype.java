package org.dataone.cn.mercury;

import index_utils.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

import org.w3c.dom.*;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;


/**
 * @author gj1 Proof of concept class for using XPath arrays for parsing
 *         metadata.
 */
public class Solr_XPath_Prototype {
    Logger logger = Logger.getLogger(this.getClass().getName());
    private LinkedHashMap<String, LinkedHashMap<String, XPathExpression>> xprMap;
    // reuse these
    Object result;
    NodeList nodes;
    static final String patternStr = "\\s+";
    static final String replaceStr = " ";
    static Pattern pattern = Pattern.compile(patternStr);
    static Matcher matcher = null;

    /**
     * @param doc2
     *            The metadata source
     * @param cxp2
     *            a map of named XPathExpressions
     *
     * @return parsed metadata as a map of named lists
     */
    public LinkedHashMap<String, ArrayList<String>> getRawMetadata(Document doc2,
            LinkedHashMap<String, XPathExpression> cxp2) {

        LinkedHashMap<String, ArrayList<String>> metaTerms = new LinkedHashMap<String, ArrayList<String>>();
        ArrayList<String> s2 = new ArrayList<String>();

        try {
            Set<String> terms2 = cxp2.keySet();
            for (String var : terms2) {
                try {
                    s2 = new ArrayList<String>();
                    result = cxp2.get(var).evaluate(doc2, XPathConstants.NODESET);
                    nodes = (NodeList) result;
                    for (int i = 0; i < nodes.getLength(); i++) {
                        s2.add(nodes.item(i).getNodeValue());
                    }
                    /*
                     * Since we do not know in advance whether a function might be used,
                     * try for the 99% case and if it fails,
                     * try a STRING type which will get the function responses.
                     */
                } catch (XPathExpressionException exc) {
                    s2 = new ArrayList<String>();
//					logger.warn("XPathExpressionException caught!! " + exc.getCause().getMessage());
//					logger.warn("Trying String type");
                    result = cxp2.get(var).evaluate(doc2, XPathConstants.STRING);
                    s2.add((String) result);
                }
                metaTerms.put(var, s2);

            }


        } catch (XPathExpressionException exc) {
            exc.printStackTrace();
            logger.warn("Another XPathExpressionException caught!! " + exc.getMessage());
        }
        return metaTerms;
    }

    // private HashMap<String, HashMap<String, String>> schema_map = new
    // HashMap<String, HashMap<String, String>>();
    // private HashMap<String, HashMap<String, XPathExpression>> xprMaps = new
    // HashMap<String, HashMap<String, XPathExpression>>();
    public Solr_XPath_Prototype(Map<String, Map<String, String>> schema_map, MercuryNamespaceContext namespaces) throws XPathExpressionException {

        LinkedHashMap<String, LinkedHashMap<String, XPathExpression>> xMap = new LinkedHashMap<String, LinkedHashMap<String, XPathExpression>>();

        for (String schemaKey : schema_map.keySet()) {

            XPathFactory factory = XPathFactory.newInstance();
            XPath xpath = factory.newXPath();
            xpath.setNamespaceContext(namespaces);
            // should build map of compiled expressions for each element
            LinkedHashMap<String, XPathExpression> xpr = new LinkedHashMap<String, XPathExpression>();
            for (String term : schema_map.get(schemaKey).keySet()) {
                String tempexpr = schema_map.get(schemaKey).get(term);
                XPathExpression temp_expr = xpath.compile(tempexpr);
                xpr.put(term, temp_expr);
            }

            xMap.put(schemaKey, xpr);

        }

        this.xprMap = xMap;
    }

    public String getFullText2(String filename) throws IOException {

        String full = "";
        String line = "";
        StringBuilder sb = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        FileReader fr = new FileReader(filename);
        BufferedReader in2 = new BufferedReader(fr);
        try {
            while ((line = in2.readLine()) != null) {
                line = line.replaceAll("<!\\[CDATA\\[", "").replaceAll("]]>", "").replaceAll("&nbsp\\;", "").replaceAll("\\|", " ");
                sb2.append(line);
            }
        } catch (Exception exc) {
            exc.printStackTrace();
        }
        in2.close();
        Reader r1 = new StringReader(sb2.toString());
        BufferedReader in;
        try {
            in = new BufferedReader(new RemoveHTMLReader(r1));
            while ((line = in.readLine()) != null) {
                matcher = pattern.matcher(line.trim());
                full = matcher.replaceAll(replaceStr);
                full.replaceAll("\n", " ");
                if (full.trim().length() > 0) {
                    sb.append(" " + full.trim());
                }
            }
            in.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return sb.toString();
    }

    public String getFullText(Document doc) {

        String full = "";

        try {
            String patternStr = "\\s+";
            String replaceStr = " ";
            Pattern pattern = Pattern.compile(patternStr);
            Transformer xformer = TransformerFactory.newInstance().newTransformer();
            xformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            xformer.setOutputProperty(OutputKeys.INDENT, "yes");
            xformer.setOutputProperty(OutputKeys.MEDIA_TYPE, "text/xml");
            xformer.setOutputProperty(OutputKeys.METHOD, "text");
            Writer outWriter = new StringWriter();
            StreamResult result = new StreamResult(outWriter);
            Source source = new DOMSource(doc);
            xformer.transform(source, result);
            String output = outWriter.toString().trim();
            Matcher matcher = pattern.matcher(output);
            full = matcher.replaceAll(replaceStr);

        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
            logger.warn(" TransformerConfigurationException \n ");
        } catch (TransformerException e) {
            e.printStackTrace();
            logger.warn(" TransformerException \n ");
        }






        /*
        // This method is FAST, but it concatenates the extracted values,
        // preventing searches for discrete strings	not good
        try { String patternStr = "\\s+";
        String replaceStr = " ";
        Pattern pattern = Pattern.compile(patternStr);
        Transformer xformer =TransformerFactory.newInstance() .newTransformer();
        xformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        xformer.setOutputProperty(OutputKeys.INDENT, "yes");
        xformer.setOutputProperty(OutputKeys.MEDIA_TYPE, "text/xml");
        xformer.setOutputProperty(OutputKeys.METHOD, "text");
        Writer outWriter = new StringWriter();
        StreamResult result = new StreamResult(outWriter);
        Source source = new DOMSource(doc);
        xformer.transform(source, result);
        String output =outWriter.toString().trim();
        Matcher matcher = pattern.matcher(output);
        full = matcher.replaceAll(replaceStr);

        } catch (TransformerConfigurationException e) { e.printStackTrace();
        logger.warn(" TransformerConfigurationException \n ");
        } catch(TransformerException e) {
        e.printStackTrace();
        logger.warn(" TransformerException \n ");
        }
         */

        /*
        // works great, but for unknown reasons, is VERY SLOW, UNACCEPTABLE
        try {
        full = iterator.followNode2(doc);

        // logger.warn(nodeText);
        } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        }
         */
//		System.out.println(full);

        return full;
    }

    public void fail(String msg) {
        logger.warn("\n" + msg + "\n\n");
    }

    public LinkedHashMap<String, LinkedHashMap<String, XPathExpression>> getXprMap() {
        return xprMap;
    }

    public void setXprMap(LinkedHashMap<String, LinkedHashMap<String, XPathExpression>> xprMap) {
        this.xprMap = xprMap;
    }
    
}
