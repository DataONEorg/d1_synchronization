/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.proto;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.dataone.client.D1Client;
import org.dataone.cn.batch.proto.harvester.MetadataQueueBuilder;
import org.dataone.cn.batch.proto.harvester.ObjectListQueueBuilder;
import org.dataone.cn.batch.proto.packager.MetadataPackageBuilder;
import org.dataone.cn.jjigae.BiBimBob;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.ObjectInfo;
import org.jibx.runtime.JiBXException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 *
 * @author rwaltz
 */
public class Main {

    public static void main(String[] args) throws FileNotFoundException, JiBXException, IOException, ServiceFailure, GeneralSecurityException {
        ApplicationContext context =
                new ClassPathXmlApplicationContext(new String[]{"/org/dataone/cn/batch/packager/services.xml"});

        BiBimBob bob = (BiBimBob) context.getBean("biBimBob", BiBimBob.class);
        List<String> bi = bob.bapMeogeureoGaja();
        AuthToken cnToken = (AuthToken) context.getBean("cnToken", AuthToken.class);

        D1Client cnClient = (D1Client) context.getBean("cnClient", D1Client.class);

        AuthToken authToken = cnClient.login(bi.get(0), bi.get(1));
//        AuthToken authToken = cnClient.login("uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg", "kepler");
        cnToken.setToken(authToken.getToken());
        ObjectListQueueBuilder devHarvestObject= (ObjectListQueueBuilder) context.getBean("devObjectListQueue");
        
        devHarvestObject.buildQueue();
        List service = (List) context.getBean("harvestQueue", ArrayList.class);
        System.out.println("build Queue results\n");
        for (Object key : service) {
            ObjectInfo value = (ObjectInfo)key;
            System.out.println("queued = " + value.getIdentifier().getValue());

        }
        MetadataQueueBuilder devHarvester = (MetadataQueueBuilder) context.getBean("devHarvester");

        devHarvester.buildQueue();
        Map packagerQueue = (Map) context.getBean("packagerQueue", HashMap.class);
        System.out.println("packaging Queue results\n");
        for (Object key: packagerQueue.keySet()) {
            System.out.println("process = " + key + ":" + packagerQueue.get(key));
        }
        System.out.println("process Queue results\n");
        MetadataPackageBuilder packageBuilder = (MetadataPackageBuilder) context.getBean("devPackager");
        packageBuilder.processQueue();
    }
}
