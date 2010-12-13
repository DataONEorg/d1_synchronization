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
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.proto.packager.types.DataPersistenceKeys;
import org.dataone.cn.batch.proto.packager.types.EventMap;
import org.dataone.cn.batch.proto.packager.types.MergeMap;

/**
 *
 * @author waltz
 */
public class ReplicationPersistence extends DataPersistence implements DataPersistenceWriter {

    Logger logger = Logger.getLogger(ReplicationPersistence.class.getName());
    private MergeMap persistMergeMap;
    private EventMap persistEventMap;

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
                    while ( (numberOfObjects < 2) && (obj = ois.readObject()) != null) {
                        if (obj instanceof EventMap) {
                            persistEventMap = (EventMap) obj;
                        } else if (obj instanceof MergeMap) {
                            persistMergeMap = (MergeMap) obj;
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
                persistMergeMap = new MergeMap();
                persistEventMap = new EventMap();
                this.persistEventMap.put(DataPersistenceKeys.SKIP_IN_LOG_FIELD.toString(), new Long(0));
                this.persistEventMap.put(DataPersistenceKeys.DATE_TIME_LAST_ACCESSED_FIELD.toString(), new Long(0));
                this.getPersistentDataFile().createNewFile();
            }
        }
    }

    @Override
    public void writePersistentData() throws FileNotFoundException, IOException, Exception {
        if (this.getPersistentDataFile() != null && this.getPersistentDataFile().exists() && (this.getPersistentDataFile().canWrite())) {
            FileOutputStream fos = new FileOutputStream(this.getPersistentDataFile());
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(this.persistEventMap);
            oos.writeObject(this.persistMergeMap);
            oos.flush();
            fos.close();
        }
    }

    public EventMap getPersistEventMap() {
        return persistEventMap;
    }

    public void setPersistEventMap(EventMap persistEventMap) {
        this.persistEventMap = persistEventMap;
    }

    public MergeMap getPersistMergeMap() {
        return persistMergeMap;
    }

    public void setPersistMergeMap(MergeMap persistMergeMap) {
        this.persistMergeMap = persistMergeMap;
    }
}
