/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.agent.http.command;

import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

/**
 * Created by Enrico Risa on 19/02/16.
 */
public abstract class OServerCommandDistributedAuthenticated extends OServerCommandAuthenticatedServerAbstract {

  protected OServerCommandDistributedAuthenticated(String iRequiredResource) {
    super(iRequiredResource);
  }

  protected boolean isLocalNode(OHttpRequest iRequest) {

    ODistributedServerManager distributedManager = server.getDistributedManager();
    String node = iRequest.getParameter("node");

    if (node == null || distributedManager == null) {
      return true;
    }
    return distributedManager.getLocalNodeName().equalsIgnoreCase(node);
  }

}
