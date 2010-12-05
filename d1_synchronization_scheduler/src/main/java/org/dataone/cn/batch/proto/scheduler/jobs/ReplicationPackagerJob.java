/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.proto.scheduler.jobs;

import java.io.FileNotFoundException;
import org.apache.log4j.Logger;

import org.dataone.cn.batch.proto.packager.ReplicationLogReader;
import org.dataone.cn.batch.proto.packager.MetadataPackageWriter;

/**
 *
 * @author rwaltz
 */
public class ReplicationPackagerJob {
    ReplicationLogReader logReader;
    MetadataPackageWriter packageWriter;
    Logger logger = Logger.getLogger(this.getClass().getName());
    public void packageMetadata()  {
        try {
            logReader.readLogfile();
            packageWriter.setReadQueue(logReader.getMergeMap());
            packageWriter.writePackages();
        } catch (FileNotFoundException ex) {
            logger.error(ex.getMessage(),ex);
        } catch (Exception ex) {
            logger.error(ex.getMessage(),ex);
        }

    }
    public ReplicationLogReader getLogReader() {
        return logReader;
    }

    public void setLogReader(ReplicationLogReader logReader) {
        this.logReader = logReader;
    }

    public MetadataPackageWriter getPackageWriter() {
        return packageWriter;
    }

    public void setPackageWriter(MetadataPackageWriter packageWriter) {
        this.packageWriter = packageWriter;
    }


}
