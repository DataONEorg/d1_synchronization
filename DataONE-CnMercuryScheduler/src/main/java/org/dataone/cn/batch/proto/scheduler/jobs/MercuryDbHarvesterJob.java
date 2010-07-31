/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.proto.scheduler.jobs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.dataone.cn.mercury.Harvester;


/**
 *
 * @author rwaltz
 */
public class MercuryDbHarvesterJob {
    static Logger logger = Logger.getLogger(MercuryDbHarvesterJob.class.getName());
    Harvester mercuryDbHarvest;
    public void harvestDb()  {
        try {
        File mercuryBaseDir = new File(mercuryDbHarvest.getBasedir() + File.separator + mercuryDbHarvest.getInstance());
        if (mercuryBaseDir.exists() && mercuryBaseDir.canRead()) {
            File[] sources = mercuryBaseDir.listFiles();
            logger.info("Found " + sources.length + " sources");
            for (File source: sources) {

            // make certain the top level directory is a valid source
                String sourceName = source.getName();
                logger.info("Found " + sourceName );
                File[] objectSchema = source.listFiles();
                logger.info("Found " + objectSchema.length + " schema dirs");
                // only use those schema/objectTypes that have a Mapping
                for (File schema: objectSchema) {
                    String schemaName = schema.getName();
                    logger.info("Process " + schemaName);
                    mercuryDbHarvest.setSchema(schemaName);
                    mercuryDbHarvest.setSource(sourceName);
                    mercuryDbHarvest.setThreads(1);
                    mercuryDbHarvest.harvest();
                }
            }
        } else {
            logger.warn("Either directory does not exists or Unable to read" + mercuryBaseDir.getPath());
        }
        } catch (Exception ex) {
            logger.error(ex.getMessage(),ex);
        }
    }

    public Harvester getMercuryDbHarvest() {
        return mercuryDbHarvest;
    }

    public void setMercuryDbHarvest(Harvester mercuryDbHarvest) {
        this.mercuryDbHarvest = mercuryDbHarvest;
    }


}
