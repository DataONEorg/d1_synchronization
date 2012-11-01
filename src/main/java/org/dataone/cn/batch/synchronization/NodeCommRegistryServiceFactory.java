/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.batch.synchronization;

import com.hazelcast.core.HazelcastInstance;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.client.MNode;
import org.dataone.client.auth.CertificateManager;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.cn.hazelcast.HazelcastClientInstance;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.impl.v1.NodeRegistryService;
import org.dataone.service.cn.impl.v1.ReserveIdentifierService;
import org.dataone.service.cn.v1.CNCore;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.util.D1Url;

/**
 * Creates a CommNode (communication node) for use by the TransferObjectTask
 * A CommNode is memberNode specific
 * 
 * Sets up instances that should be reused by the TransferObjectTask
 * Assume that most of the instances are not thread-safe, in other words
 * the instances created by this factory will be re-used by threads, but no two concurrent threads
 * should access the same instances (with the exception of hazelcast client instance)
 * 
 * @author waltz
 */
public class NodeCommRegistryServiceFactory implements NodeCommFactory {

    public final static Log logger = LogFactory.getLog(NodeCommRegistryServiceFactory.class);
    private static HazelcastInstance hzclient;
    private static String clientCertificateLocation =
            Settings.getConfiguration().getString("D1Client.certificate.directory")
            + File.separator + Settings.getConfiguration().getString("D1Client.certificate.filename");
    private static ConcurrentMap<String, NodeComm> initializedMemberNodes = new ConcurrentHashMap<String, NodeComm>();
    private static NodeCommFactory nodeCommFactory = null;
    private NodeCommRegistryServiceFactory() {
        
    }
    public static NodeCommFactory getInstance () {
        if (nodeCommFactory == null) {
            nodeCommFactory = new NodeCommRegistryServiceFactory() ;
        }
        return nodeCommFactory;
    }
    @Override
    public NodeComm getNodeComm(String mnUrl) throws ServiceFailure {
        return this.getNodeComm(mnUrl, null);
    }

    @Override
    public NodeComm getNodeComm(String mnUrl, String hzConfigLocation) throws ServiceFailure {
        if (initializedMemberNodes.containsKey(mnUrl)) {
            return initializedMemberNodes.get(mnUrl);
        } else  {
        if (hzclient == null) {
            hzclient = HazelcastClientInstance.getHazelcastClient();
            CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
        }

        NodeRegistryService nodeRegistryService = new NodeRegistryService();
        NodeComm nodeComm = new NodeComm(nodeRegistryService, hzclient);
        initializedMemberNodes.putIfAbsent(mnUrl, nodeComm);
        return nodeComm;
        }
    }
}
