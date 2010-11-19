/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.packager;

import org.dataone.cn.batch.proto.packager.types.DataPersistenceKeys;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.apache.log4j.Logger;

;

/**
 *
 * @author rwaltz
 */
public class ReplicationLogReader extends EventLogReader {

    Logger logger = Logger.getLogger(ReplicationLogReader.class.getName());

    //
    // this will be map of a map, the first key is GUID
    // the map that the GUID points to will have two entires
    // one is a key of 'SCIMETA' (needs to be a type maybe, but for now string)
    // with the GUID's science metadata file name from the log
    // while the second has a key of 'SYSMETA' (once again type needed)
    // with a value of the system metadata file name from the log
    //

    protected ReplicationPersistence replicationPersistence;
    @Override
    public void readLogfile() throws FileNotFoundException, Exception {

        this.setMergeMap(replicationPersistence.getPersistMergeMap());
        // Persistent file that will tell you how many bytes to skip before reading.
        // It will also maintain the lastAccessedDate of the file being processed
        File logFileDir = new File(logFilePath);
        // this is the date the file that is being processed is last modified
        long processingFileLastModified = 0;
        long skipInLogFile = 0;
        long totalBytesRead = 0;

        LinkedList<File> processLogFiles;

         Map<String, Long> persistMappings = replicationPersistence.getPersistEventMap().getMap();
        // Persistent file that will tell you how many bytes to skip before reading.
        // It will also maintain the lastAccessedDate of the file being processed

        processLogFiles = getLogFileQueue(logFileDir, persistMappings.get(DataPersistenceKeys.DATE_TIME_LAST_ACCESSED_FIELD.toString()));

        // process the files in order
        skipInLogFile = persistMappings.get(DataPersistenceKeys.SKIP_IN_LOG_FIELD.toString()).longValue();
        logger.info("Skipping " + skipInLogFile + "bytes");
        while (!processLogFiles.isEmpty()) {
            File processFile = processLogFiles.removeLast();
            // record the lastModfiedDateTime of the file
            // TODO There maybe more data written to this file before it is opened and read, need to read that date
            // after the buffer has been completely read

            processingFileLastModified = processFile.lastModified();
            // pass in the skip bytes argument
            // return the total bytes read
            totalBytesRead = this.processLogFile(processFile, skipInLogFile);
            // 0 out the skip bytes argument
            skipInLogFile = 0;
        }

        persistMappings.put(DataPersistenceKeys.SKIP_IN_LOG_FIELD.toString(), new Long(totalBytesRead));
        persistMappings.put(DataPersistenceKeys.DATE_TIME_LAST_ACCESSED_FIELD.toString(), new Long(processingFileLastModified));

    }

    @Override
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
                if (this.getMergeMap().containsKey(guid)) {
                    sciSysMetaHashMap = (Map<String, String>)this.getMergeMap().get(guid);
                } else {
                    sciSysMetaHashMap = new HashMap<String, String>();
                    ++readlines;
                }
                sciSysMetaHashMap.put(entryType, findIdFields[1].substring(guidMarker + SCIDATATOKEN.length(), findIdFields[1].lastIndexOf(":")));
                this.getMergeMap().put(guid, sciSysMetaHashMap);
            }
        }
        logger.info("number of new and unique entries = " + readlines);
    }

    public ReplicationPersistence getReplicationPersistence() {
        return replicationPersistence;
    }

    public void setReplicationPersistence(ReplicationPersistence replicationPersistence) {
        this.replicationPersistence = replicationPersistence;
    }

}
