/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.mercury;

import java.io.*;

import gov.ornl.mercury.harvest.beans.*;
import gov.ornl.mercury.harvest.util.*;
import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.log4j.Logger;

import java.util.Date;

/**
 *
 * @author rwaltz
 */
public class DBAccessThread implements Runnable {

    public static final String fsep = File.separator;
    private DBAccessor access;
    private IndexInfoDB iidb;
    private final int m_filetype;
    private final String dir;
    private final String instance;
    private final String index;
    private final boolean log;
    private final String m_htmlurl;
    private final int min_num;
    private final int max_num;
    private final int threadnum;
    private int garbage_count = 0;
    private char fs = File.separatorChar;
    Logger logger = Logger.getLogger(this.getClass().getName());

    /**
     * Instantiates a new update harvest2a.
     *
     * @param m_htmlurl		used as switch for selecting XPATH for file	location
     * 						 saved in the harvestInfo and transformedFiles tables
     * @param m_filetype
     *            the m_filetype
     * @param dir
     *            the directory
     * @param instance
     *            instance	name of the directory for locating the data files
     * @param index
     *            the datasource name
     * @param log
     *            set to true to generate info
     * @param harvest_in_source
     *            specifies data source directory for walking
     * @param max_num
     *            upper index into the file list....stop here
     * @param min_num
     *            lower index into the file list... starting reference
     * @param threadnum
     *            number of threads to attempt to split processing between
     */
    public DBAccessThread(DBAccessor access,  IndexInfoDB iidb,
            String m_htmlurl, int m_filetype, String dir,
            String instance, String index, boolean log,
            int max_num, int min_num, int threadnum) {
        this.access = access;
        this.iidb = iidb;
        this.m_filetype = m_filetype;
        this.dir = dir;
        this.instance = instance;
        this.index = index;
        this.log = log;
        this.m_htmlurl = m_htmlurl;
        this.min_num = min_num;
        this.max_num = max_num;
        this.threadnum = threadnum;

    }

    public void mlog(String m_fileURL, String filePath) {
        if ((m_fileURL == null) || (m_fileURL.length() < 1)) {
            logger.debug("Metadata problem : " + index + "  " + filePath);
        }
    }

    public void run() {
        System.out.println("Now Starting: " + instance + " : " + index);

        java.util.Date date = new java.util.Date();
        long t = date.getTime();
        java.sql.Date sqlDate = new java.sql.Date(t);
        String m_fileURL = "";
        int fileID = 0;
        int fileCount = 0;
        int fileURL_ID = 0;
        String tableName = "";

        Connection conn = access.getConnection();  //create connection object
        

        try {
            conn.setAutoCommit(false);

            String updateSQL = "insert into " + access.getDbName() + ".file_reference values(?,?)";
            PreparedStatement insPstmt1 = conn.prepareStatement(updateSQL);

            String updateSQL2 = "insert into " + access.getDbName() + ".harvest_info values (?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement insPstmt2 = conn.prepareStatement(updateSQL2);



            if (m_filetype == FileType.FETCHED) {
                tableName = "fetched_files";

            } else if (m_filetype == FileType.HARVESTED) {
                tableName = "transformed_files";
            }

            String updateSQL4 = "insert into " + access.getDbName() + "." + tableName + " values (?,?,?,?,?)";
            PreparedStatement insPstmt4 = conn.prepareStatement(updateSQL4);


            File fileHarvestDirectory = new File(dir);

            Dom4jXMLRetriever dom4j = new Dom4jXMLRetriever();
            if (fileHarvestDirectory.isDirectory()) {
                File[] harvestedFiles = fileHarvestDirectory.listFiles();

                HarvestInfo harvestInfo = new HarvestInfo();
                FileInfo harFileInfo = new FileInfo();
                IndexInfo indexInfo = new IndexInfo();
                indexInfo.setIndexName(index);
                indexInfo.setInstance(instance);

                indexInfo = iidb.updateIndexInfo(indexInfo);

                int indexID = indexInfo.getIndexInfoID();

                for (int i = min_num; i < max_num; i++) {
                    harvestInfo.setBase_fileURL_ID(i);  // new item for h_0 method
                    File file = harvestedFiles[i];
                    harvestInfo.setChecksumCode("");
                    harFileInfo.setFileName("");
                    harFileInfo.setFileContent("");
                    harFileInfo.setFileType(m_filetype);
                    harFileInfo.setCksum("");
                    harvestInfo.setHarvestedFileInfo(null);
                    fileURL_ID = i;
                    harvestInfo.setFileURL_ID(fileURL_ID);
                    harvestInfo.setIndexInfo(null);
                    boolean metadatafile = file.getName().endsWith("xml");
                    if (!metadatafile) {
                        garbage_count++;
                    }
                    if (file.isFile() && file.getName().endsWith("xml")) {
                        fileCount += 1;
                        String fileName = file.getName();
                        String filePath = dir + fsep + fileName;
                        char fs = File.separatorChar;
                        String fetchedFileContent = "";
                        BufferedReader in = null;
                        BufferedReader in2 = null;

                        StringBuffer sb1 = new StringBuffer();
                        in = new BufferedReader(new FileReader(file));//
                        String str;
                        while ((str = in.readLine()) != null) {
                            sb1.append(str);
                            sb1.append(System.getProperty("line.separator"));
                        }
                        fetchedFileContent = sb1.toString();
                        String csNum = CheckSumGenerator.getCheckSumCode(filePath);

                        try {
                            if (m_htmlurl.equalsIgnoreCase("nbii")) {
                                try {
                                    m_fileURL = XMLRetriever.getXPathValue(filePath,
                                            "//metadata/mercury/htmlurl/text()");

                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                    System.out.println("The Offending file was ==>>  " + filePath + "\n");
                                }

                            } else if (m_htmlurl.equalsIgnoreCase("lbaesip")) {
                                try {
                                    m_fileURL = XMLRetriever.getXPathValue(filePath,
                                            "//metadata/Local-Control-Number/text()");
                                    mlog(m_fileURL, filePath);

                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                    System.out.println("The Offending file was ==>>  " + filePath + "\n");
                                }

                            } else if (m_htmlurl.equalsIgnoreCase("iai")) {

                                m_fileURL = filePath;

                                /*
                                try{
                                m_fileURL = XMLRetriever
                                .getXPathValue(filePath,
                                "//metadata/mercury/Metadata_URL/text()");

                                }catch(Exception ex){
                                ex.printStackTrace();
                                System.out.println("The Offending file was ==>>  "+filePath+"\n");
                                }
                                 */
                            } else if ((m_htmlurl.equalsIgnoreCase("ocean")) || (m_htmlurl.equalsIgnoreCase("i3n"))) {
                                m_fileURL = filePath;

                            } // m_fileURL = filePath;
                            else {
                                try {
                                    m_fileURL = dom4j.getXPathValue(filePath,
                                            "//metadata/idinfo/citation/citeinfo/onlink/text()");

                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                    System.out.println("The Offending file was ==>>  " + filePath + "\n");
                                }

                            }
                        } catch (Exception ex) {
                            logger.debug("Exception caught : " + index + "  " + filePath);
                            ex.printStackTrace();

                        } finally {
                            mlog(m_fileURL, filePath);
                        }

                        // code changes to allow for existence of a link for this field...don't insist on a regular file name

                        //	File fi = new File(m_fileURL);
                        //	String name = fi.getName();
                        //	int dotIndex = name.indexOf(".");

                        // if ((!(m_fileURL == null))&& (m_fileURL.length()>0)&&(dotIndex > 0)&& (dotIndex < name.length())) {

                        if (m_fileURL == "") {
                            m_fileURL = filePath;
                            System.out.println("m_fileURL is empty so using the filepath (" + filePath + ")  as the  url");
                        }



                        if ((!(m_fileURL == null)) && (m_fileURL.length() > 0)) {

                            //	String ext = name.substring(dotIndex + 1,name.length());
                            harvestInfo.setChecksumCode(csNum);
                            harFileInfo.setCksum(csNum);
                            harFileInfo.setFileName(filePath);
                            //harFileInfo.setFileContent(fetchedFileContent);
                            harFileInfo.setFileURL(m_fileURL);

                            harFileInfo.setFileType(m_filetype);
                            harvestInfo.setHarvestedFileInfo(harFileInfo);
                            harvestInfo.setIndexInfo(indexInfo);


                            harvestInfo.setFileURL(m_fileURL);
                            in.close();

                            FileInfo harvestedFileInfo = harvestInfo.getHarvestedFileInfo();


                            m_fileURL = harvestedFileInfo.getFileURL();
                            fileID = fileURL_ID;


                            // file_reference
                            insPstmt1.setString(1, null);
                            insPstmt1.setString(2, m_fileURL);
                            insPstmt1.addBatch();


                            String fileCheckSum = harvestInfo.getChecksumCode();

                            int maxPrevHarInfoID = 0;

                            // harvest_info
                            insPstmt2.setString(1, null);
                            insPstmt2.setInt(2, indexID);
                            insPstmt2.setInt(3, fileID);
                            insPstmt2.setString(4, fileCheckSum);
                            insPstmt2.setDate(5, sqlDate);
                            insPstmt2.setDate(6, sqlDate);
                            insPstmt2.setInt(7, maxPrevHarInfoID);
                            insPstmt2.setString(8, m_fileURL);
                            insPstmt2.setDate(9, null);

                            insPstmt2.setInt(10, harvestInfo.getBase_fileURL_ID());  // set to file index for first harvest
                            insPstmt2.addBatch();  // new h_0

                            // transformed_files
                            insPstmt4.setString(1, null);
                            insPstmt4.setString(2, harFileInfo.getFileName());
                            //insPstmt4.setString(3, harFileInfo.getFileContent());
                            insPstmt4.setString(3, "Go get fullText from the file system");
                            insPstmt4.setString(4, harFileInfo.getFileURL());
                            insPstmt4.setString(5, harFileInfo.getCksum());
                            insPstmt4.addBatch();

                        } else {
                            System.out.println("Problem with parsing a proper harvested filename due to malformed url extracted. "
                                    + "from fileurl = : "
                                    + m_fileURL + "\n"
                                    + "fileName => " + fileName + "\n");
                        }

                    }
                }

                Date dt = new Date();


                int[] updateCounts = insPstmt1.executeBatch();
                int[] updateCounts2 = insPstmt2.executeBatch();
                int[] updateCounts4 = insPstmt4.executeBatch();
                System.out.println("indexID " + indexID + ". Totals committed by thread " + threadnum + " : \n"
                        + " file reference :  " + updateCounts.length + "\n"
                        + " harvest info :   " + updateCounts2.length + "\n"
                        + " transformed files :  " + updateCounts4.length + "\n"
                        + " garbage counter ran up: " + garbage_count + "\n");
                System.out.println("Totals committed : " + index + "  " + updateCounts2.length + " of " + fileCount + "   " + dt);
                logger.debug("Totals committed : " + index + "  " + updateCounts2.length + " of " + fileCount + "   " + dt);
                conn.commit();

            }
        } catch (Exception ignored) {
            ignored.printStackTrace();
        }
        System.out.println("Done with: " + instance + " : " + index);
    }
}
