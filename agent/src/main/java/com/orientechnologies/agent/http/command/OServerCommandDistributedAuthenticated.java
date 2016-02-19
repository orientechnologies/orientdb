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

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.OL;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

import java.io.IOException;

/**
 * Created by Enrico Risa on 19/02/16.
 */
public abstract class OServerCommandDistributedAuthenticated extends OServerCommandAuthenticatedServerAbstract {

  protected OServerCommandDistributedAuthenticated(String iRequiredResource) {
    super(iRequiredResource);
  }

  @Override
  protected boolean authenticate(OHttpRequest iRequest, OHttpResponse iResponse, boolean iAskForAuthentication) throws IOException {
    if (isAgentAuthenticated(iRequest)) {
      return true;
    } else {
      return super.authenticate(iRequest, iResponse, iAskForAuthentication);
    }
  }

  protected boolean isLocalNode(OHttpRequest iRequest) {

    ODistributedServerManager distributedManager = server.getDistributedManager();
    String node = iRequest.getParameter("node");

    if (node == null || distributedManager == null) {
      return true;
    }
    return distributedManager.getLocalNodeName().equalsIgnoreCase(node);
  }

  private boolean isAgentAuthenticated(OHttpRequest iRequest) {

    String header = iRequest.getHeader("X-REQUEST-AGENT");
    return (isLocalNode(iRequest) && header != null && authenticateAgent(header));
  }

  private boolean authenticateAgent(String token) {

    try {
      String decrypted = OL.decrypt(token);
      String localDecripted = OL.decrypt(OEnterpriseAgent.TOKEN);
      return decrypted.equals(localDecripted);
    } catch (Exception e) {
      return false;
    }
  }
}
