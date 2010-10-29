/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.proto.mercury;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.dataone.cn.mercury.Indexer;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 *
 * @author rwaltz
 */
public class Main {

    public static void main(String[] args) throws Exception, FileNotFoundException, IOException, GeneralSecurityException {
        ApplicationContext context =
   //             new ClassPathXmlApplicationContext(new String[]{"/org/dataone/cn/batch/proto/mercury/applicationContext.xml"});
new FileSystemXmlApplicationContext(new String[]{"file:/etc/dataone/mercury/indexer/applicationContext.xml"});
        Indexer indexer = (Indexer) context.getBean("DAACIndexer", Indexer.class);
        int totalRecords = indexer.indexRecords();
    }

}
