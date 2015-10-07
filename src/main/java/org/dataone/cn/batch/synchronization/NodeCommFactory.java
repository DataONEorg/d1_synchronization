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

import org.dataone.cn.batch.exceptions.NodeCommUnavailable;
import org.dataone.cn.batch.synchronization.type.NodeComm;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;

/**
 * Provides an interface for access to NodeComm objects that should be pooled for re-use.
 * Unfortunately, the string provided to the implementations maybe very different depending
 * upon how the NodeComm objects are constructed.  It would probably be best to
 * divide this into two separate interfaces and create two different type of returned
 * objects based on the calling task's needs, rather than attempt to mash two somewhat
 * different uses into one object and interface definition.
 * 
 * @author waltz
 */
public interface NodeCommFactory {

    public NodeComm getNodeComm(NodeReference mnNode) throws ServiceFailure, NodeCommUnavailable;
//    public NodeComm getNodeComm(NodeReference mnNode, String hzConfigLocation) throws ServiceFailure, NodeCommUnavailable;
}
