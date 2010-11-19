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
import org.dataone.cn.batch.proto.packager.types.MergeMap;

;

/**
 *
 * @author rwaltz
 */
public abstract class LogReader {

    Logger logger = Logger.getLogger(LogReader.class.getName());

    public static final String GUIDTOKEN = "D1GUID:";
    public static final String SCIDATATOKEN = ":D1SCIMETADATA:";
    public static final String SYSDATATOKEN = ":D1SYSMETADATA:";

    protected String logFileName;
    protected String logFilePath;

    public static String newline = System.getProperty("line.separator");
    private MergeMap mergeMap;
    //
    // this will be map of a map, the first key is GUID
    // the map that the GUID points to will have two entires
    // one is a key of 'SCIMETA' (needs to be a type maybe, but for now string)
    // with the GUID's science metadata file name from the log
    // while the second has a key of 'SYSMETA' (once again type needed)
    // with a value of the system metadata file name from the log
    //
    private LogDirFilesComparator logDirFilesComparator = new LogDirFilesComparator();

    abstract public void readLogfile() throws FileNotFoundException, Exception;

    protected long processLogFile(File processFile, long skipInLogFile) throws FileNotFoundException, IOException, Exception {
        Long totalBytesRead;
        int offset = 0;
        int numRead = 0;
        int lastByteOffset = 0;
        if (skipInLogFile < processFile.length()) {
            Long bufferSize = new Long(processFile.length() - skipInLogFile);
            if (bufferSize.intValue() > Integer.MAX_VALUE) {
                throw new Exception("File " + processFile.getAbsolutePath() + " too long with " + bufferSize.longValue() + " bytes");
            }
            FileInputStream fileInputStream = new FileInputStream(processFile);
            BufferedInputStream bufferedStream = new BufferedInputStream(fileInputStream);
            bufferedStream.skip(skipInLogFile);

            byte[] buffer = new byte[bufferSize.intValue()];

            // Read in the bytes

            while (offset < buffer.length
                    && (numRead = bufferedStream.read(buffer, offset, buffer.length - offset)) >= 0) {
                offset += numRead;
            }
            bufferedStream.close();
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
            this.buildMergeQueue(bufferedReader);

        }
        totalBytesRead = new Long(offset);
        // this return value is the total bytes processed in the logfile
        return totalBytesRead.longValue() + skipInLogFile - lastByteOffset;
    }

    abstract protected void buildMergeQueue(BufferedReader bufferedReader) throws IOException;

    protected LinkedList<File> getLogFileQueue(File logFileDir, long dateTimeLastAccessed) throws Exception {

        LinkedList<File> processLogFiles = new LinkedList<File>();

        File logFile = new File(logFilePath + File.separator + logFileName);
        if (logFile == null || !(logFile.exists())) {
            throw new Exception("Log file " + logFilePath + File.separator + logFileName + "does not exist");
        }
        // TODO This reading through multiple files 
        if (logFileDir.exists() && logFileDir.isDirectory()) {
            // create a directory listing of all rolled over log files
            // to determine if we need to catch up in synchronization
            LogDirFilter logFileDirFilter = new LogDirFilter(logFileName + "\\.\\d+", dateTimeLastAccessed);

            File[] logFileDirList = logFileDir.listFiles(logFileDirFilter);

            if (logFileDirList != null && logFileDirList.length > 0) {

                Arrays.sort(logFileDirList, logDirFilesComparator);
                // create ordered queue

                for (File log : logFileDirList) {
                    processLogFiles.addFirst(log);
                }
            }

            processLogFiles.addFirst(logFile);


        } else {
            throw new Exception("EventLogReader: Logging directory " + logFilePath + " either does not exist or cannot be read!");
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

    public String getLogFileName() {
        return logFileName;
    }

    public void setLogFileName(String logFileName) {
        this.logFileName = logFileName;

    }

    public String getLogFilePath() {
        return logFilePath;
    }

    public void setLogFilePath(String logFilePath) {
        this.logFilePath = logFilePath;
    }

    public MergeMap getMergeMap() {
        return mergeMap;
    }

    public void setMergeMap(MergeMap mergeMap) {
        this.mergeMap = mergeMap;
    }


}
