/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.packager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author waltz
 */
public abstract class DataPersistence implements DataPersistenceWriter {

    private String persistentDataFileName;
    private String persistentDataFileNamePath;
    private File persistentDataFile = null;

    @Override
    abstract public void writePersistentData() throws FileNotFoundException, IOException, Exception;

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
