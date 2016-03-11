/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import java.io.IOException;

/**
 * Remote server controller.
 * 
 * @author Luca Garulli
 */
public class ORemoteServerController {
  private final ORemoteServerChannel requestChannel;
  private final ORemoteServerChannel responseChannel;

  public ORemoteServerController(final ODistributedServerManager manager, final String iServer, final String iURL,
      final String user, final String passwd) throws IOException {
    requestChannel = new ORemoteServerChannel(manager, iServer, iURL, user, passwd);
    responseChannel = new ORemoteServerChannel(manager, iServer, iURL, user, passwd);
  }

  public void sendRequest(final ODistributedRequest req, final String node) {
    requestChannel.sendRequest(req, node);
  }

  public void sendResponse(final ODistributedResponse response, final String node) {
    responseChannel.sendResponse(response, node);
  }

  public void close() {
    requestChannel.close();
    responseChannel.close();
  }
}
