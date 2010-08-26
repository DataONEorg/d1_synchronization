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
public class LogReader {

    Logger logger = Logger.getLogger(LogReader.class.getName());

    private String eventLogFileName;
    private String eventLogFilePath;

    public static String newline = System.getProperty("line.separator");
    private MetadataPackageAccess metadataPackageAccess;
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

    public void readEventLogfile() throws FileNotFoundException, Exception {
        this.mergeQueue = new HashMap<String, Map<String, String>>();
        // entries are from the event log
        File eventLogFileDir = new File(eventLogFilePath);
        // this is the date the file that is being processed is last modified
        long processingFileLastModified = 0;
        long skipInLogFile = 0;
        long totalBytesRead = 0;

        LinkedList<File> processLogFiles;

         HashMap<String, Long> persistMappings = metadataPackageAccess.getPersistMappings();
        // Persistent file that will tell you how many bytes to skip before reading.
        // It will also maintain the lastAccessedDate of the file being processed

        processLogFiles = this.getLogFileQueue(eventLogFileDir, persistMappings.get(metadataPackageAccess.DATE_TIME_LAST_ACCESSED_FIELD));

        // process the files in order
        skipInLogFile = persistMappings.get(metadataPackageAccess.SKIP_IN_LOG_FIELD).longValue();

        while (!processLogFiles.isEmpty()) {
            File processFile = processLogFiles.removeLast();
            // record the lastModfiedDateTime of the file
            // TODO There maybe more data written to this file before it is opened and read, need to read that date
            // after the buffer has been completely read
            
            processingFileLastModified = processFile.lastModified();
            // pass in the skip bytes argument
            // return the total bytes read
            totalBytesRead = this.processEventLogFile(processFile, skipInLogFile);
            // 0 out the skip bytes argument
            skipInLogFile = 0;
        }

        persistMappings.put(metadataPackageAccess.SKIP_IN_LOG_FIELD, new Long(totalBytesRead));
        persistMappings.put(metadataPackageAccess.DATE_TIME_LAST_ACCESSED_FIELD, new Long(processingFileLastModified));

        metadataPackageAccess.setPersistMappings(persistMappings);


    }

    private long processEventLogFile(File processFile, long skipInLogFile) throws FileNotFoundException, IOException, Exception {
        Long totalBytesRead;
        int offset = 0;
        int numRead = 0;
        int lastByteOffset = 0;
        if (skipInLogFile < processFile.length()) {
            Long bufferSize = new Long(processFile.length() - skipInLogFile);
            if (bufferSize.intValue() > Integer.MAX_VALUE) {
                throw new Exception("File " + processFile.getAbsolutePath() + " too long with " + bufferSize.longValue() + " bytes");
            }
            FileInputStream eventFileInputStream = new FileInputStream(processFile);
            BufferedInputStream bufferedEventStream = new BufferedInputStream(eventFileInputStream);
            bufferedEventStream.skip(skipInLogFile);

            byte[] buffer = new byte[bufferSize.intValue()];

            // Read in the bytes

            while (offset < buffer.length
                    && (numRead = bufferedEventStream.read(buffer, offset, buffer.length - offset)) >= 0) {
                offset += numRead;
            }
            bufferedEventStream.close();
            // Ensure all the bytes have been read in
            if (offset < buffer.length) {
                throw new IOException("Could not completely read file " + processFile.getName());
            }

            // make certain the last char is ('\n') or a carriage return ('\r') or ('\r\n')
            // if not then find the last position in the buffer that has a
            // newline and trunc the buffer to that size
            // the remaining bytes not processed will be subtracted from totalbytes read, and processed next iteration
            // newline should only be a double character on dos
            int lastBufferIndex = bufferSize.intValue() - 1;
            ;

            byte[] newlineArray = newline.getBytes();
            while (((lastBufferIndex - lastByteOffset) > 0)
                    && !(ArrayUtils.isEquals(ArrayUtils.subarray(buffer, lastBufferIndex - newlineArray.length - lastByteOffset, lastBufferIndex - lastByteOffset), newlineArray))) {
                lastByteOffset += newlineArray.length;
            }

            // trunc the buffer to the last position that has a newline character
            if (lastByteOffset > 0) {
                buffer = ArrayUtils.subarray(buffer, 0, lastBufferIndex - lastByteOffset);
            }
            logger.info("size of buffer = " + buffer.length + " lastByteOffset = " + lastByteOffset);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer)));
            this.buildEventMergeQueue(bufferedReader);

        }
        totalBytesRead = new Long(offset);
        // this return value is the total bytes processed in the logfile
        return totalBytesRead.longValue() + skipInLogFile - lastByteOffset;
    }

    private void buildEventMergeQueue(BufferedReader bufferedReader) throws IOException {
        int readlines = 0;
        String logEntry;
        // logfile entry should look something like:
        //knb 20100718-16:22:00: [INFO]: create D1GUID:knb:testid:201019913220178:D1SCIMETADATA:autogen.20101991322022.1:D1SYSMETA:autogen.20101991322039.1:
        while ((logEntry = bufferedReader.readLine()) != null) {
            logger.trace(logEntry);
            if (logEntry.contains("create") && logEntry.contains("D1GUID:")) {
                ++readlines;
                HashMap<String, String> sciSysMetaHashMap = new HashMap<String, String>();
                String[] findIdFields = logEntry.split("D1GUID:", 2);
                int guidMarker = findIdFields[1].lastIndexOf(":D1SCIMETADATA:");
                String guid = findIdFields[1].substring(0, guidMarker);
                String[] sysSciMetaIdFields = findIdFields[1].substring(guidMarker + ":D1SCIMETADATA:".length()).split(":D1SYSMETA:", 2);
                sciSysMetaHashMap.put("SCIMETA", sysSciMetaIdFields[0]);
                int finalIndex = sysSciMetaIdFields[1].lastIndexOf(":");
                if (finalIndex == -1) {
                    finalIndex = sysSciMetaIdFields[1].length() - 1;
                }
                sciSysMetaHashMap.put("SYSMETA", sysSciMetaIdFields[1].substring(0, finalIndex));
                this.mergeQueue.put(guid, sciSysMetaHashMap);
            }
        }
        logger.info("number of entries = " + readlines);
    }



    private LinkedList<File> getLogFileQueue(File eventLogFileDir, long dateTimeLastAccessed) throws Exception {

        LinkedList<File> processLogFiles = new LinkedList<File>();

        File eventLogFile = new File(eventLogFilePath + File.separator + eventLogFileName);
        if (eventLogFile == null || !(eventLogFile.exists())) {
            throw new Exception("Log file " + eventLogFilePath + File.separator + eventLogFileName + "does not exist");
        }
        // TODO This reading through multiple files 
        if (eventLogFileDir.exists() && eventLogFileDir.isDirectory()) {
            // create a directory listing of all rolled over log files
            // to determine if we need to catch up in synchronization
            LogDirFilter eventLogFileDirFilter = new LogDirFilter(eventLogFileName + "\\.\\d+", dateTimeLastAccessed);

            File[] eventLogFileDirList = eventLogFileDir.listFiles(eventLogFileDirFilter);

            if (eventLogFileDirList != null && eventLogFileDirList.length > 0) {

                Arrays.sort(eventLogFileDirList, logDirFilesComparator);
                // create ordered queue

                for (File logFile : eventLogFileDirList) {
                    processLogFiles.addFirst(logFile);
                }
            }

            processLogFiles.addFirst(eventLogFile);


        } else {
            throw new Exception("LogReader: Logging directory " + eventLogFilePath + " either does not exist or cannot be read!");
        }

        return processLogFiles;
    }
    // TODO Need a better comparator, or at least test this one with 0-100 entries...
    class LogDirFilesComparator implements Comparator<File> {

        // Comparator interface requires defining compare method.
        public int compare(File file1, File file2) {
            return file1.getName().compareToIgnoreCase(file2.getName());
        }
    }

    class LogDirFilter implements FileFilter {

        private Pattern pattern;
        private long lastModifiedDateTime;

        public LogDirFilter(String regex, long lastModifiedDateTime) {
            this.pattern = Pattern.compile(regex);
            this.lastModifiedDateTime = lastModifiedDateTime;
        }

        public boolean accept(File logFile) {
            if (pattern.matcher(logFile.getName()).matches()) {
                return (this.lastModifiedDateTime < logFile.lastModified());
            } else {
                return false;
            }
        }
    }

    public String getEventLogFileName() {
        return eventLogFileName;
    }

    public void setEventLogFileName(String eventLogFileName) {
        this.eventLogFileName = eventLogFileName;

    }

    public String getEventLogFilePath() {
        return eventLogFilePath;
    }

    public void setEventLogFilePath(String eventLogFilePath) {
        this.eventLogFilePath = eventLogFilePath;
    }

    public Map<String, Map<String, String>> getMergeQueue() {
        return this.mergeQueue;
    }

    public void setMergeQueue(Map<String, Map<String, String>> mergeQueue) {
        this.mergeQueue = mergeQueue;
    }

    public MetadataPackageAccess getMetadataPackageAccess() {
        return metadataPackageAccess;
    }

    public void setMetadataPackageAccess(MetadataPackageAccess metadataPackageAccess) {
        this.metadataPackageAccess = metadataPackageAccess;
    }

}
