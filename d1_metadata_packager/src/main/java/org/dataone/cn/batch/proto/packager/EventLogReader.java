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
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.proto.packager.types.LogAccessMap;
import org.dataone.cn.batch.proto.packager.types.MergeMap;

/**
 *
 * @author rwaltz
 */
public class EventLogReader extends LogReader {

    Logger logger = Logger.getLogger(EventLogReader.class.getName());
    protected DataPersistence eventPersistence;
    //
    // this will be map of a map, the first key is GUID
    // the map that the GUID points to will have two entires
    // one is a key of 'SCIMETA' (needs to be a type maybe, but for now string)
    // with the GUID's science metadata file name from the log
    // while the second has a key of 'SYSMETA' (once again type needed)
    // with a value of the system metadata file name from the log
    //
    private LogDirFilesComparator logDirFilesComparator = new LogDirFilesComparator();

    @Override
    public void readLogfile() throws FileNotFoundException, Exception {
        this.setMergeMap(eventPersistence.getPersistMergeMap());
        // entries are from the event log
        File logFileDir = new File(logFilePath);
        // this is the date the file that is being processed is last modified
        long processingFileLastModified = 0;
        long skipInLogFile = 0;
        long totalBytesRead = 0;

        LinkedList<File> processLogFiles;

        LogAccessMap persistentMapping = eventPersistence.getPersistLogAcessMap();
        // Persistent file that will tell you how many bytes to skip before reading.
        // It will also maintain the lastAccessedDate of the file being processed

        processLogFiles = getLogFileQueue(logFileDir, persistentMapping.get(DataPersistenceKeys.DATE_TIME_LAST_ACCESSED_FIELD.toString()));

        // process the files in order
        skipInLogFile = persistentMapping.get(DataPersistenceKeys.SKIP_IN_LOG_FIELD.toString()).longValue();
        logger.debug("number of logfiles on queue = " + processLogFiles.size() + " number of bytes to skip = " + skipInLogFile
                + "Date Log last Accessed: " + DateFormat.getDateInstance().format(new Date(persistentMapping.get(DataPersistenceKeys.DATE_TIME_LAST_ACCESSED_FIELD.toString()))));
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

        persistentMapping.put(DataPersistenceKeys.SKIP_IN_LOG_FIELD.toString(), new Long(totalBytesRead));
        persistentMapping.put(DataPersistenceKeys.DATE_TIME_LAST_ACCESSED_FIELD.toString(), new Long(processingFileLastModified));

    }

    @Override
    protected void buildMergeQueue(BufferedReader bufferedReader) throws IOException {
        int readlines = 0;
        String logEntry;
        // logfile entry should look something like:
        //knb 20100718-16:22:00: [INFO]: create D1GUID:knb:testid:201019913220178:D1SCIMETADATA:autogen.20101991322022.1:D1SYSMETA:autogen.20101991322039.1:
        while ((logEntry = bufferedReader.readLine()) != null) {
            logger.trace(logEntry);
            if (logEntry.contains("create") && logEntry.contains(GUIDTOKEN)) {
                ++readlines;
                HashMap<String, String> sciSysMetaHashMap = new HashMap<String, String>();
                String[] findIdFields = logEntry.split(GUIDTOKEN, 2);
                int guidMarker = findIdFields[1].lastIndexOf(SCIDATATOKEN);
                String guid = findIdFields[1].substring(0, guidMarker);
                String[] sysSciMetaIdFields = findIdFields[1].substring(guidMarker + SCIDATATOKEN.length()).split(SYSDATATOKEN, 2);
                sciSysMetaHashMap.put(DataPersistenceKeys.SCIMETA.toString(), sysSciMetaIdFields[0]);
                int finalIndex = sysSciMetaIdFields[1].lastIndexOf(":");
                if (finalIndex == -1) {
                    finalIndex = sysSciMetaIdFields[1].length() - 1;
                }
                sciSysMetaHashMap.put(DataPersistenceKeys.SYSMETA.toString(), sysSciMetaIdFields[1].substring(0, finalIndex));
                this.getMergeMap().put(guid, sciSysMetaHashMap);
            }
        }
        logger.info("number of entries = " + readlines);
    }

    public DataPersistence getEventPersistence() {
        return eventPersistence;
    }

    public void setEventPersistence(DataPersistence eventPersistence) {
        this.eventPersistence = eventPersistence;
    }
}
