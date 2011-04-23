/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.packager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.proto.packager.types.DataPersistenceKeys;
import org.dataone.cn.batch.proto.packager.types.LogAccessMap;
import org.dataone.cn.batch.proto.packager.types.MergeMap;

/**
 *
 * @author waltz
 */
public class DataPersistence implements DataPersistenceWriter {

    private String persistentDataFileName;
    private String persistentDataFileNamePath;
    private File persistentDataFile = null;
    Logger logger = Logger.getLogger(DataPersistence.class.getName());
    private MergeMap persistMergeMap;
    private LogAccessMap persistLogAcessMap;

    public void init() throws FileNotFoundException, IOException, ClassNotFoundException, Exception {

        if (this.getPersistentDataFileNamePath() != null && this.getPersistentDataFileName() != null) {
            this.setPersistentDataFile(new File(this.getPersistentDataFileNamePath() + File.separator + this.getPersistentDataFileName()));
            if (this.getPersistentDataFile().exists() && this.getPersistentDataFile().length() > 0) {
                if (this.getPersistentDataFile().canRead()) {
                    logger.info(this.getPersistentDataFile().getAbsolutePath());
                    FileInputStream fis = null;
                    fis = new FileInputStream(this.getPersistentDataFile());
                    ObjectInputStream ois = new ObjectInputStream(fis);
                    Object obj = null;
                    // ois.readObject() does not return a null, it throws and EOF exception instead
                    // therefore, keep track of  number of objects processed
                    int numberOfObjects = 0;
                    while ((numberOfObjects < 2) && (obj = ois.readObject()) != null) {
                        if (obj instanceof LogAccessMap) {
                            this.persistLogAcessMap = (LogAccessMap) obj;
                        } else if (obj instanceof MergeMap) {
                            this.persistMergeMap = (MergeMap) obj;
                        } else {
                            throw new Exception(this.getPersistentDataFileName() + " does not contain deserializable data");
                        }
                        ++numberOfObjects;
                    }
                    fis.close();
                } else {
                    throw new Exception("LogFilePendingData file " + this.getPersistentDataFileName() + " either does not exist or cannot be read!");
                }
            } else {
                this.persistMergeMap = new MergeMap();
                this.persistLogAcessMap = new LogAccessMap();
                this.persistLogAcessMap.put(DataPersistenceKeys.SKIP_IN_LOG_FIELD.toString(), new Long(0));
                this.persistLogAcessMap.put(DataPersistenceKeys.DATE_TIME_LAST_ACCESSED_FIELD.toString(), new Long(0));
                this.getPersistentDataFile().createNewFile();
            }
        }
    }
    @Override
    public void writePersistentData() throws FileNotFoundException, IOException, Exception {
        if (this.getPersistentDataFile() != null && this.getPersistentDataFile().exists() && (this.getPersistentDataFile().canWrite())) {
            FileOutputStream fos = new FileOutputStream(this.getPersistentDataFile());
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(this.persistLogAcessMap);
            oos.writeObject(this.persistMergeMap);
            oos.flush();
            fos.close();
        }
    }
    public LogAccessMap getPersistLogAcessMap() {
        return persistLogAcessMap;
    }

    public void setPersistLogAcessMap(LogAccessMap persistLogAcessMap) {
        this.persistLogAcessMap = persistLogAcessMap;
    }

    public MergeMap getPersistMergeMap() {
        return persistMergeMap;
    }

    public void setPersistMergeMap(MergeMap persistMergeMap) {
        this.persistMergeMap = persistMergeMap;
    }
    @Override
    public String getPersistentDataFileName() {
        return persistentDataFileName;
    }

    @Override
    public void setPersistentDataFileName(String persistentDataFileName) {
        this.persistentDataFileName = persistentDataFileName;
    }

    @Override
    public String getPersistentDataFileNamePath() {
        return persistentDataFileNamePath;
    }

    @Override
    public void setPersistentDataFileNamePath(String persistentDataFileNamePath) {
        this.persistentDataFileNamePath = persistentDataFileNamePath;
    }

    public File getPersistentDataFile() {
        return persistentDataFile;
    }

    public void setPersistentDataFile(File persistentDataFile) {
        this.persistentDataFile = persistentDataFile;
    }
}
