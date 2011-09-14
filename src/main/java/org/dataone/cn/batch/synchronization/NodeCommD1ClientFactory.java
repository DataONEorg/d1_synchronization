/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.synchronization;

import org.dataone.client.CNode;
import org.dataone.client.MNode;
import org.dataone.cn.batch.type.NodeComm;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.v1.CNCore;
import org.dataone.service.cn.v1.CNRead;
import org.dataone.service.util.D1Url;

/**
 *
 * @author waltz
 */
public class NodeCommD1ClientFactory implements NodeCommFactory {

    @Override
    public NodeComm getNodeComm(String mnUrl) {
        MNode mNode = new MNode(mnUrl + "/v1");
        // XXX this could be configurable parameter
        LocalHostNode metacatNode = new LocalHostNode(Settings.getConfiguration().getString("Synchronization.cn_base_url"));
        NodeComm nodeComm = new NodeComm(mNode, metacatNode, metacatNode);
        return nodeComm;
    }

    // CNode adds version to the url, but, we can not allow that to happen
    // because the cn metacat interface does not have versions
    // it is always of the latest version of implementation
    private class LocalHostNode extends CNode implements CNCore, CNRead {

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

    }
}
