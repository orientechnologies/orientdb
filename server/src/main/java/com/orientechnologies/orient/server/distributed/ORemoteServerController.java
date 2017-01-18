/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import java.io.IOException;

/**
 * Remote server controller. It handles the communication with remote servers in HA configuration.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ORemoteServerController {
  private final ORemoteServerChannel[] requestChannels;
  private volatile int                 requestChannelIndex  = 0;

  private final ORemoteServerChannel[] responseChannels;
  private volatile int                 responseChannelIndex = 0;

  public ORemoteServerController(final ODistributedServerManager manager, final String iServer, final String iURL,
      final String user, final String passwd) throws IOException {
    if( user == null )
      throw new IllegalArgumentException("User is null");
    if( passwd == null )
      throw new IllegalArgumentException("Password is null");

    ODistributedServerLog.debug(this, manager.getLocalNodeName(), iServer, ODistributedServerLog.DIRECTION.OUT,
        "Creating remote channel(s) to distributed server...");
    OContextConfiguration config = manager.getServerInstance().getContextConfiguration();

    requestChannels = new ORemoteServerChannel[ config.getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_REQUEST_CHANNELS)];
    for (int i = 0; i < requestChannels.length; ++i)
      requestChannels[i] = new ORemoteServerChannel(manager, iServer, iURL, user, passwd);

    responseChannels = new ORemoteServerChannel[config.getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_RESPONSE_CHANNELS)];
    for (int i = 0; i < responseChannels.length; ++i)
      responseChannels[i] = new ORemoteServerChannel(manager, iServer, iURL, user, passwd);
  }

  public void sendRequest(final ODistributedRequest req) {
    int idx = requestChannelIndex++;
    if( idx < 0)
      idx = 0;
    requestChannels[idx % responseChannels.length].sendRequest(req);
  }

  public void sendResponse(final ODistributedResponse response) {
    int idx = responseChannelIndex++;
    if( idx < 0)
      idx = 0;
    responseChannels[idx % responseChannels.length].sendResponse(response);
  }

  public void close() {
    for (int i = 0; i < requestChannels.length; ++i)
      requestChannels[i].close();

    for (int i = 0; i < responseChannels.length; ++i)
      responseChannels[i].close();
  }
}
