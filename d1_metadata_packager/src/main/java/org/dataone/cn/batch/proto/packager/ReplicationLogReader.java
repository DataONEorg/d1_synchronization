/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.packager;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.utils.MetadataPackageAccess;

;

/**
 *
 * @author rwaltz
 */
public class ReplicationLogReader extends EventLogReader {

    Logger logger = Logger.getLogger(ReplicationLogReader.class.getName());

    private String logFileName;
    private String logFilePath;

    public static String newline = System.getProperty("line.separator");

    //
    // this will be map of a map, the first key is GUID
    // the map that the GUID points to will have two entires
    // one is a key of 'SCIMETA' (needs to be a type maybe, but for now string)
    // with the GUID's science metadata file name from the log
    // while the second has a key of 'SYSMETA' (once again type needed)
    // with a value of the system metadata file name from the log
    //
    private Map<String, Map<String, String>> mergeQueue;
    private LogDirFilesComparator logDirFilesComparator = new LogDirFilesComparator();

    public void readLogfile() throws FileNotFoundException, Exception {

        this.mergeQueue = super.metadataPackageAccess.getPendingDataQueue();
        // Persistent file that will tell you how many bytes to skip before reading.
        // It will also maintain the lastAccessedDate of the file being processed
        super.readLogfile();
        super.metadataPackageAccess.setPendingDataQueue(mergeQueue);

    }

    protected void buildMergeQueue(BufferedReader bufferedReader) throws IOException {
        int readlines = 0;
        String logEntry;
        // logfile entry should look something like:
        //knb 20100718-16:22:00: [INFO]: create D1GUID:knb:testid:201019913220178:D1SCIMETADATA:autogen.20101991322022.1:D1SYSMETA:autogen.20101991322039.1:
        while ((logEntry = bufferedReader.readLine()) != null) {
            logger.trace(logEntry);
            if (logEntry.contains("replicate") && logEntry.contains(EventLogReader.GUIDTOKEN)) {
                
                String entryType = null;
                Map<String, String> sciSysMetaHashMap = null;
                String[] findIdFields = logEntry.split(EventLogReader.GUIDTOKEN, 2);
                if (findIdFields[1].contains(EventLogReader.SCIDATATOKEN) ) {

                    entryType = EventLogReader.SCIDATATOKEN;
                } else if (findIdFields[1].contains(EventLogReader.SYSDATATOKEN) ) {
                    entryType = EventLogReader.SYSDATATOKEN;
                } else {
                    logger.error("Non Exception Error: Unable to determine intent of the following log entry" + newline + logEntry);
                    continue;
                }
                
                int guidMarker = findIdFields[1].lastIndexOf(entryType);

                String guid = findIdFields[1].substring(0, guidMarker);
                if (this.mergeQueue.containsKey(guid)) {
                    sciSysMetaHashMap = this.mergeQueue.get(guid);
                } else {
                    sciSysMetaHashMap = new HashMap<String, String>();
                    ++readlines;
                }
                sciSysMetaHashMap.put(entryType, findIdFields[1].substring(guidMarker + SCIDATATOKEN.length(), findIdFields[1].lastIndexOf(":")));
                this.mergeQueue.put(guid, sciSysMetaHashMap);
            }
        }
        logger.info("number of new and unique entries = " + readlines);
    }

    public String getReplicationLogFileName() {
        return super.logFileName;
    }

    public void setReplicationLogFileName(String logFileName) {
        super.logFileName = logFileName;

    }

    public String getReplicationLogFilePath() {
        return super.logFilePath;
    }

    public void setReplicationLogFilePath(String logFilePath) {
        super.logFilePath = logFilePath;
    }


    @Override
    public Map<String, Map<String, String>> getMergeQueue() {
        return this.mergeQueue;
    }

    @Override
    public void setMergeQueue(Map<String, Map<String, String>> mergeQueue) {
        this.mergeQueue = mergeQueue;
    }



}
