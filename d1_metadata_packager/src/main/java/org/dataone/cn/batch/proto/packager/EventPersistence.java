/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.proto.packager;

import org.dataone.cn.batch.proto.packager.types.DataPersistenceKeys;
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
import org.dataone.cn.batch.proto.packager.types.EventMap;


/**
 *
 * @author waltz
 */
public class EventPersistence extends DataPersistence implements DataPersistenceWriter{


    private EventMap persistData;
     Logger logger = Logger.getLogger(EventPersistence.class.getName());

    public void init() throws FileNotFoundException, IOException, ClassNotFoundException, Exception {
        
        this.setPersistentDataFile(new File(this.getPersistentDataFileNamePath() + File.separator + this.getPersistentDataFileName()));
        FileInputStream fis = null;
        logger.info(this.getPersistentDataFile().getAbsolutePath());
        if (this.getPersistentDataFile().exists() && this.getPersistentDataFile().length() > 0) {
            if (this.getPersistentDataFile().canRead() ) {

                fis = new FileInputStream(this.getPersistentDataFile());
                ObjectInputStream ois = new ObjectInputStream(fis);
                this.persistData = (EventMap) ois.readObject();
                fis.close();

            } else {
                throw new Exception("LogFileSkipData file " + this.getPersistentDataFileName() + " either does not exist or cannot be read!");
            }
        } else {
            this.persistData = new EventMap();
            this.getPersistentDataFile().createNewFile();
            this.persistData.put(DataPersistenceKeys.SKIP_IN_LOG_FIELD.toString(), new Long(0));
            this.persistData.put(DataPersistenceKeys.DATE_TIME_LAST_ACCESSED_FIELD.toString(), new Long(0));
        }
    }
    @Override
    public void writePersistentData() throws FileNotFoundException, IOException, Exception {
        if (this.getPersistentDataFile() != null && this.getPersistentDataFile().exists() && (this.getPersistentDataFile().canWrite())) {
            FileOutputStream fos = new FileOutputStream(this.getPersistentDataFile());
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(this.persistData);
            oos.flush();
            fos.close();
        }
    }

    public EventMap getPersistMapping() {
        return persistData;
    }

    public void setPersistMapping(EventMap persistData) {
        this.persistData = persistData;
    }

}
