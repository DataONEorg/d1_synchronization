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
 *
 * @author rwaltz
 */
public class Harvester {

    public static final String fsep = File.separator;
    private DBAccessor access;
    private IndexInfoDB iidb;
    private String source;
    private String instance;
    private int threads = 1;
    private String basedir;
    private String schema;
    private String htmlurl;
    private boolean log = false;
    Logger logger = Logger.getLogger(this.getClass().getName());

    public void harvest() {
        ExecutorService executor = Executors.newFixedThreadPool(90);

        String harvest_dir = basedir + fsep + instance + fsep + source + fsep + schema;
        logger.info("harvesting " + harvest_dir);
        int max = 0;
        int min = 0;

        try {

            File fileHarvestDirectory = new File(harvest_dir);

            File[] harvestedFiles = fileHarvestDirectory.listFiles();
            logger.info("harvesting " + harvest_dir + " with " + harvestedFiles.length + " files");
            if (null != harvestedFiles) {

                for (int i2 = 0; i2 < threads; i2++) {

                    min = max;
                    max = min + harvestedFiles.length / threads;
                    if (i2 == (threads - 1)) {
                        max = harvestedFiles.length;
                    }

                    Runnable runner = new DBAccessThread(access, iidb, htmlurl, FileType.HARVESTED, harvest_dir, instance, source, log, max, min, i2);

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

    public String getBasedir() {
        return basedir;
    }

    public void setBasedir(String basedir) {
        this.basedir = basedir;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
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
