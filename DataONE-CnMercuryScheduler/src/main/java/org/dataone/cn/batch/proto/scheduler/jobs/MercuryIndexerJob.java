/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.proto.scheduler.jobs;

import java.io.FileNotFoundException;
import java.util.logging.Level;
import org.apache.log4j.Logger;

import org.dataone.cn.mercury.Indexer;

/**
 *
 * @author rwaltz
 */
public class MercuryIndexerJob {

    Indexer indexer;
    Logger logger = Logger.getLogger(this.getClass().getName());
    public void indexMercury()  {
        try {
            logger.info("starting to index " + indexer.getInstance() + " " + indexer.getSource());
            indexer.indexRecords();
            logger.info("completing index " + indexer.getInstance() + " " + indexer.getSource());
        } catch (FileNotFoundException ex) {
            logger.error(ex.getMessage(),ex);
        } catch (Exception ex) {
            logger.error(ex.getMessage(),ex);
        }

    }

    public Indexer getIndexer() {
        return indexer;
    }

    public void setIndexer(Indexer indexer) {
        this.indexer = indexer;
    }



}
