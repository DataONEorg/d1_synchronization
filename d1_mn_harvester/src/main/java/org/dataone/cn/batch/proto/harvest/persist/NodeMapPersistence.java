/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.harvest.persist;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.proto.harvest.types.NodeMap;

/**
 *
 * @author waltz
 */
public class NodeMapPersistence {

    private String persistentDataFileName;
    private String persistentDataFileNamePath;
    private File persistentDataFile = null;
    private NodeMap persistData = null;
    Logger logger = Logger.getLogger(NodeMapPersistence.class.getName());

    public void init() throws FileNotFoundException, IOException, ClassNotFoundException, Exception {

        this.setPersistentDataFile(new File(this.getPersistentDataFileNamePath() + File.separator + this.getPersistentDataFileName()));
        FileInputStream fis = null;
        logger.info(this.getPersistentDataFile().getAbsolutePath());
        if (this.getPersistentDataFile().exists() && this.getPersistentDataFile().length() > 0) {
            if (this.getPersistentDataFile().canRead()) {

                fis = new FileInputStream(this.getPersistentDataFile());
                ObjectInputStream ois = new ObjectInputStream(fis);
                this.persistData = (NodeMap) ois.readObject();
                fis.close();

            } else {
                throw new Exception("LogFileSkipData file " + this.getPersistentDataFileName() + " either does not exist or cannot be read!");
            }
        } else {
            this.persistData = new NodeMap();
            this.getPersistentDataFile().createNewFile();
            this.getPersistentDataFile().setReadable(true);
            this.getPersistentDataFile().setWritable(true);
        }
    }

    public void writePersistentData() throws FileNotFoundException, IOException, Exception {
        if (this.getPersistentDataFile() == null) {
            throw new NullPointerException("NodeMapPersistence has not been properly initialized");
        }
        if (!this.getPersistentDataFile().exists()) {
            throw new FileNotFoundException("Where is " + this.getPersistentDataFileNamePath() + File.separator + this.getPersistentDataFileName() + " ?");
        }
        if ((this.getPersistentDataFile().canWrite())) {
            FileOutputStream fos = new FileOutputStream(this.getPersistentDataFile());
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(this.persistData);
            oos.flush();
            fos.close();
        } else {
            throw new Exception(this.getPersistentDataFileNamePath() + File.separator + this.getPersistentDataFileName() + " is not writable");
        }
    }

    public NodeMap getPersistMapping() {
        return persistData;
    }

    public void setPersistMapping(NodeMap persistData) {
        this.persistData = persistData;
    }

    public String getPersistentDataFileName() {
        return persistentDataFileName;
    }

    public void setPersistentDataFileName(String persistentDataFileName) {
        this.persistentDataFileName = persistentDataFileName;
    }

    public String getPersistentDataFileNamePath() {
        return persistentDataFileNamePath;
    }

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
