package org.dataone.cn.batch.harvest;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */



import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import javax.annotation.Resource;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.ldap.impl.HazelcastLdapStore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import java.util.Set;
import org.dataone.cn.batch.type.SimpleNode;


/**
 *
 * @author waltz
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:org/dataone/configuration/applicationContext.xml"})
public class JobTest implements ApplicationContextAware {
static Logger logger = Logger.getLogger(JobTest.class);
    ApplicationContext applicationContext;
    HazelcastLdapStore hazelcastLdapStore;
    private HazelcastInstance hazelcast;
    /**
     * pull in the CnCore implementation to test against
     * @author rwaltz
     */
  /**
     * pull in the CnCore implementation to test against
     * @author rwaltz
     */
    @Resource
    public void setCNCore(HazelcastLdapStore hazelcastLdapStore) {
        this.hazelcastLdapStore = hazelcastLdapStore;
    }
    @Resource
    public void setHazelcastInstance(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
    }
    @Test
    public void loadAllSimpleNodeTest() {
        logger.info("begin");
         Set<String> keys = hazelcastLdapStore.loadAllKeys();
         for (String key : keys) {
             logger.info("KEY: "+ key);
         }
         IMap<String, SimpleNode> d1NodesMap = hazelcast.getMap("d1NodesMap");
         logger.info("Node map has " + d1NodesMap.size() + " entries");
    }
/*
    @Test
    public void harvesterJobTest()  {
        logger.info("begin");
        MnHarvesterJob job = applicationContext.getBean(MnHarvesterJob.class);
        job.init();
        job.harvestMetadata();
    }
*/
    @Override
    public void setApplicationContext(ApplicationContext ac) throws BeansException {
        applicationContext = ac;
    }
}
