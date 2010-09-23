/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.proto.scheduler.jobs;

import java.io.FileNotFoundException;
import java.util.logging.Level;
import org.apache.log4j.Logger;

import org.dataone.cn.batch.proto.packager.EventLogReader;
import org.dataone.cn.batch.proto.packager.MetadataPackageWriter;

/**
 *
 * @author rwaltz
 */
public class PackagerJob {
    EventLogReader eventLogReader;
    MetadataPackageWriter packageWriter;
    Logger logger = Logger.getLogger(this.getClass().getName());
    public void packageMetadata()  {
        try {
            eventLogReader.readLogfile();
            packageWriter.setReadQueue(eventLogReader.getMergeQueue());
            packageWriter.writePackages();
        } catch (FileNotFoundException ex) {
            logger.error(ex.getMessage(),ex);
        } catch (Exception ex) {
            logger.error(ex.getMessage(),ex);
        }

    }
    public EventLogReader getLogReader() {
        return eventLogReader;
    }

    public void setLogReader(EventLogReader eventLogReader) {
        this.eventLogReader = eventLogReader;
    }

    public MetadataPackageWriter getPackageWriter() {
        return packageWriter;
    }

    public void setPackageWriter(MetadataPackageWriter packageWriter) {
        this.packageWriter = packageWriter;
    }


}
