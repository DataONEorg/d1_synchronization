/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.synchronization;

import com.hazelcast.core.HazelcastInstance;
import java.io.File;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.client.MNode;
import org.dataone.client.auth.CertificateManager;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.hazelcast.HazelcastClientInstance;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.v1.CNCore;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.util.D1Url;

/**
 *
 * @author waltz
 */
public class NodeCommD1ClientFactory implements NodeCommFactory {

    public final static Log logger = LogFactory.getLog(NodeCommD1ClientFactory.class);
    private static HazelcastInstance hzclient;
    private String clientCertificateLocation =
            Settings.getConfiguration().getString("D1Client.certificate.directory")
            + File.separator + Settings.getConfiguration().getString("D1Client.certificate.filename");

    @Override
    public NodeComm getNodeComm(String mnUrl) throws ServiceFailure {
        return this.getNodeComm(mnUrl, null);
    }

    @Override
    public NodeComm getNodeComm(String mnUrl, String hzConfigLocation) throws ServiceFailure {
        if (hzclient == null) {
            hzclient = HazelcastClientInstance.getHazelcastClient();
            CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
        }
/*        MNode mNode = new MNode(mnUrl);

        LocalHostNode metacatNode = new LocalHostNode(Settings.getConfiguration().getString("Synchronization.cn_base_url")); */
        D1Client d1client = new D1Client();
        CNode cNode = d1client.getCN();
        MNode mNode = d1client.getMN(mnUrl);
        NodeComm nodeComm = new NodeComm(mNode, cNode, cNode, hzclient);
        return nodeComm;
    }

    // CNode adds version to the url, but, we can not allow that to happen
    // because the cn metacat interface does not have versions
    // it is always of the latest version of implementation
/*    private class LocalHostNode extends CNode implements CNCore, CNRead {

        private String nodeBaseServiceUrl;

        public LocalHostNode(String nodeBaseServiceUrl) {
            super(nodeBaseServiceUrl);
            this.nodeBaseServiceUrl = nodeBaseServiceUrl;
        }

        @Override
        public String getNodeBaseServiceUrl() {
            D1Url url = new D1Url(this.nodeBaseServiceUrl);
            return url.getUrl();
        }
    } */
}
