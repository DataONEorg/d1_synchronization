/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto.packager;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author waltz
 */
public interface DataPersistenceWriter {

    public void writePersistentData() throws FileNotFoundException, IOException, Exception;

    public String getPersistentDataFileName();

    public void setPersistentDataFileName(String persistentDataFileName);

    public String getPersistentDataFileNamePath();

    public void setPersistentDataFileNamePath(String persistentDataFileNamePath);
}
