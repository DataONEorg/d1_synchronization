/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.mercury;

import java.io.*;
import java.util.concurrent.*;

import gov.ornl.mercury.harvest.beans.*;

import org.apache.log4j.Logger;

/**
 * Uncertain what this class will really do because I do understand the difference between FETCHED files
 * and HARVESTED files. So, this is a copy of the Harvester. But this code may be modified to implement
 * a different workflow, or configuration before calling the DBAccessThread
 * 
 * @author rwaltz
 */
public class Fetcher {

    public static final String fsep = File.separator;
    private DBAccessor access;
    private IndexInfoDB iidb;
    private String source;
    private String instance;
    private int threads = 1;
    private String mercury;
    private String thesaurus;
    private String htmlurl;
    private boolean log = false;

    Logger logger = Logger.getLogger(this.getClass().getName());

    public void harvest() {
        ExecutorService executor = Executors.newFixedThreadPool(90);

        String basedir = mercury;
        String harvest_dir = basedir + instance + fsep + source + fsep + thesaurus;

        int max = 0;
        int min = 0;

        try {

            File fileHarvestDirectory = new File(harvest_dir);

            File[] harvestedFiles = fileHarvestDirectory.listFiles();

            if (null != harvestedFiles) {

                for (int i2 = 0; i2 < threads; i2++) {

                    min = max;
                    max = min + harvestedFiles.length / threads;
                    if (i2 == (threads - 1)) {
                        max = harvestedFiles.length;
                    }

                    Runnable runner = new DBAccessThread(access, iidb, htmlurl, FileType.FETCHED, harvest_dir, instance, source, log, max, min, i2);

                    executor.execute(runner);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {

            executor.shutdown();
            executor.awaitTermination(20000, TimeUnit.SECONDS);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public String getHtmlurl() {
        return htmlurl;
    }

    public void setHtmlurl(String htmlurl) {
        this.htmlurl = htmlurl;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public boolean isLog() {
        return log;
    }

    public void setLog(boolean log) {
        this.log = log;
    }

    public String getMercury() {
        return mercury;
    }

    public void setMercury(String mercury) {
        this.mercury = mercury;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getThesaurus() {
        return thesaurus;
    }

    public void setThesaurus(String thesaurus) {
        this.thesaurus = thesaurus;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public DBAccessor getAccess() {
        return access;
    }

    public void setAccess(DBAccessor access) {
        this.access = access;
    }

    public IndexInfoDB getIidb() {
        return iidb;
    }

    public void setIidb(IndexInfoDB iidb) {
        this.iidb = iidb;
    }

}
