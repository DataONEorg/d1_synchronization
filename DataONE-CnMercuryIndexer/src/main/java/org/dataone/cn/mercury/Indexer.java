/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.mercury;

import com.epublishing.schema.solr.Add;
import com.epublishing.schema.solr.Add.Doc;
import com.epublishing.schema.solr.AddDocument;
import com.epublishing.schema.solr.Field;
import index_utils.DocumentDatabase;
import index_utils.FileInfo;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import javax.xml.xpath.XPathExpression;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.dataone.cn.mercury.Solr_XPath_Prototype;
/**
 *
 * @author rwaltz
 */
public class Indexer {

    Logger logger = Logger.getLogger(this.getClass().getName());
    private String solrdb;
    private String solrPostURL;
    private String instance;
    private String source;
    private DocumentDatabase documentDatabase;
    private Solr_XPath_Prototype sxp;
    XPath_Handler xpathHandler;
    private List traceList;
    private LinkedHashMap<String, LinkedHashMap<String, XPathExpression>> xpr_maps = new LinkedHashMap<String, LinkedHashMap<String, XPathExpression>>();
    /** The Solr_ is o8601 format. */
    private SimpleDateFormat Solr_ISO8601FORMAT = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ssZ");
    /** The Solr_ rf c822 dateformat. */
    private SimpleDateFormat Solr_RFC822DATEFORMAT = new SimpleDateFormat(
            "EEE', 'dd' 'MMM' 'yyyy' 'HH:mm:ss' 'Z", Locale.US);
    /** The yada_ solr_ solr_ is o8601 format. */
    private SimpleDateFormat yada_Solr_Solr_ISO8601FORMAT = new SimpleDateFormat(
            "yyyy'-'MM'-'dd'T'HH:mm:ss'Z'");

    /**
     * Convert to rf c822 string.
     *
     * @param date the date in iso format
     *
     * @return the string representation of the date in RFC
     */
    private String convertToRFC822String(String date) {
        try {
            Solr_ISO8601FORMAT.parse(date, new ParsePosition(0));
        } catch (Exception pex) {
            pex.printStackTrace();
        }
        return Solr_RFC822DATEFORMAT.format(Solr_ISO8601FORMAT.getCalendar().getTime());
    }

    /**
     * Convert to iso 8601 format.
     *
     * @param date the date in RFC822 format
     *
     * @return the string representation of the date in iso format.
     */
    private String convertToISO8601FORMAT(String date) {
        try {
            Solr_ISO8601FORMAT.parse(date, new ParsePosition(0));
        } catch (Exception pex) {
            pex.printStackTrace();
        }
        return yada_Solr_Solr_ISO8601FORMAT.format(Solr_ISO8601FORMAT.getCalendar().getTime());
    }

    /**
     * Stream contents.
     *
     * @param solrdb the solrdb
     * @param instance the instance
     * @param source the source
     *
     * @return the int
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public int indexRecords()
            throws IOException, Exception {

        //////////////////////////////////////////////////////////////////////

        xpr_maps = sxp.getXprMap();

        Calendar now = Calendar.getInstance();
        SimpleDateFormat formatter = new SimpleDateFormat("MM / dd   hh:mm");
        List content = new ArrayList();
         if (logger.getLevel().toInt() < Level.DEBUG_INT) {
              content = getTraceList();
              if (content == null) {
                  throw new Exception("content Array is null");
              }
         } else { 
            content = this.documentDatabase.getCurrentContentBytesbySource2(
                solrdb, instance, source);
        }
        int rs_ctr = 0;
        int totalRecords = content.size();
        logger.info("total records from DB " + solrdb + "  " + totalRecords);
        for (Iterator it = content.iterator(); it.hasNext();) {
            FileInfo fi = (FileInfo) it.next();

            try {
                if (!(solrPostURL.length() == 0)) {
                    //byte[] contents2 = fi.getContents();
                    String filename = fi.getFileURL();
                    Date update_date = fi.getUpdate_date();
                    String fileName = fi.getFileName();
                    rs_ctr++;

                    try {
                        //	XPath_Handler xpathHandler = new XPath_Handler(solrdb, instance, source,xpr_maps);

                    	// TODO	 need to determine the Copyright and license status of com.epublishing.schema
                        AddDocument addDoc = AddDocument.Factory.newInstance();
                        Add add = addDoc.addNewAdd();
                        Doc doc = add.addNewDoc();

                        if (doc != null) {

                            Field field = doc.addNewField();
                            if ((!filename.equalsIgnoreCase("<"))
                                    && (filename.trim().length() > 0)) {
                                field.setName("id");

                                field.setStringValue(filename);


                            } else {
                                logger.debug("***** NO ID so assigning-> "
                                        + rs_ctr);
                                field.setName("id");
                                field.setStringValue(rs_ctr + "|" + source
                                        + "");
                                logger.debug(rs_ctr + "|" + source);
                            }

                            Field field_source = doc.addNewField();
                            field_source.setName("datasource");
                            field_source.setStringValue(source);

                            Field field_date = doc.addNewField();
                            field_date.setName("update_date");
                            field_date.setStringValue(convertToISO8601FORMAT(update_date.toString()));

                            String fullText = sxp.getFullText2(fileName);

//							logger.debug("Text Blob -> "+fullText.length());

                            Field fullTextfield = doc.addNewField();
                            fullTextfield.setName("fullText");
                            fullTextfield.setStringValue(fullText);

                        } else {
                            System.err.println("File type not supported "
                                    + "; skipping");
                        }

                        // original method for getting content to index
                        // get the contents from mysql

                        //	ByteArrayInputStream bs = new ByteArrayInputStream(contents2);
                        //	xmlHandler.getDocument(bs, doc);

                        // below is to try replacing with direct file access by the name
                        //xmlHandler.getDocument(new FileInputStream(fileName),doc);

                        //		xpathHandler.getDocument2(new FileInputStream(fileName), doc);

                        xpathHandler.getDocument2(new FileInputStream(fileName), doc);


                        HttpClient client = new HttpClient();
                        RequestEntity re = new StringRequestEntity(addDoc.toString(), "text/xml", "UTF-8");
                        if (logger.getLevel().toInt() < Level.DEBUG_INT) {
                            re.writeRequest(System.out);
                        } else {

                            PostMethod post = new PostMethod(solrPostURL);

                            post.setRequestEntity(re);
                            int statusCode = client.executeMethod(post);
                            post.releaseConnection();
                            if (rs_ctr % 100 == 0 || rs_ctr == totalRecords) {
                                PostMethod postCommit = new PostMethod(solrPostURL);

                                RequestEntity reCommit = new StringRequestEntity(
                                        "<commit/>", "text/xml", "UTF-8");
                                postCommit.setRequestEntity(reCommit);
                                statusCode = client.executeMethod(postCommit);

                                if (rs_ctr == totalRecords) {
                                    logger.info(formatter.format(now.getTime())
                                            + " datasource: "
                                            + source + "  "
                                            + rs_ctr + " of " + totalRecords + " indexed  \n "
                                            + " solrPostURL " + solrPostURL
                                            + "  solrdb  " + solrdb + " \n ");
                                }

                            }
                        }
                    } catch (IOException e) {
                        logger.error("Cannot index " + fileName
                                + "; skipping (" + e.getMessage() + ")", e);
                    } catch (Exception e) {
                        logger.error("ERROR in parsing the file: "
                                + filename, e);
                    }
                }
            } catch (Exception e) {
                logger.error("Exception inside IndexAction: " + e.getMessage(), e);
            }
        }
        return rs_ctr;

    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Logger getLogger() {
        return logger;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public String getSolrPostURL() {
        return solrPostURL;
    }

    public void setSolrPostURL(String solrPostURL) {
        this.solrPostURL = solrPostURL;
    }

    public String getSolrdb() {
        return solrdb;
    }

    public void setSolrdb(String solrdb) {
        this.solrdb = solrdb;
    }

    public Solr_XPath_Prototype getSxp() {
        return sxp;
    }

    public void setSxp(Solr_XPath_Prototype sxp) {
        this.sxp = sxp;
    }

    public XPath_Handler getXpathHandler() {
        return xpathHandler;
    }

    public void setXpathHandler(XPath_Handler xpathHandler) {
        this.xpathHandler = xpathHandler;
    }

    public DocumentDatabase getDocumentDatabase() {
        return documentDatabase;
    }

    public void setDocumentDatabase(DocumentDatabase documentDatabase) {
        this.documentDatabase = documentDatabase;
    }

    public List getTraceList() {
        return traceList;
    }

    public void setTraceList(List traceList) {
        this.traceList = traceList;
    }

}
