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

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.client.remote.OStorageRemoteOperation;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Remote server controller.
 * 
 * @author Luca Garulli
 */
public class ORemoteServerController extends OServerAdmin {
  private final ODistributedServerManager manager;

  public ORemoteServerController(final ODistributedServerManager manager, final String iURL, final String user, final String passwd)
      throws IOException {
    super(iURL);

    clientType = "RemoteServerController";
    collectStats = false;

    // FORCE ALL THE MESSAGES TO BE QUEUED IN THE SAME CHANNEL
    storage.getClientConfiguration().setValue(OGlobalConfiguration.CLIENT_CHANNEL_MAX_POOL, 1);

    this.manager = manager;
    connect(user, passwd);
  }

  public void sendRequest(final ODistributedRequest req, final String node) {
    networkAdminOperation(new OStorageRemoteOperation<Object>() {
      @Override
      public Object execute(final OChannelBinaryAsynchClient network) throws IOException {
        storage.beginRequest(network, OChannelBinaryProtocol.DISTRIBUTED_REQUEST);

        try {
          final byte[] serializedRequest;
          final ByteArrayOutputStream out = new ByteArrayOutputStream();
          try {
            final ObjectOutputStream outStream = new ObjectOutputStream(out);
            try {
              req.writeExternal(outStream);
              serializedRequest = out.toByteArray();

              ODistributedServerLog.debug(this, manager.getLocalNodeName(), node, ODistributedServerLog.DIRECTION.OUT,
                  "Sending request %s (%d bytes)", req, serializedRequest.length);

              network.writeBytes(serializedRequest);

            } finally {
              outStream.close();
            }
          } finally {
            out.close();
          }

        } finally {
          storage.endRequest(network);
          // NO RESPONSE: FREE THE CHANNEL
          storage.getEngine().getConnectionManager().release(network);
        }
        return null;
      }
    }, "Cannot send distributed request");
  }

  public void sendResponse(final ODistributedResponse response, final String node) {
    networkAdminOperation(new OStorageRemoteOperation<Object>() {
      @Override
      public Object execute(final OChannelBinaryAsynchClient network) throws IOException {
        storage.beginRequest(network, OChannelBinaryProtocol.DISTRIBUTED_RESPONSE);
        try {

          final ByteArrayOutputStream out = new ByteArrayOutputStream();
          try {
            final ObjectOutputStream outStream = new ObjectOutputStream(out);
            try {
              response.writeExternal(outStream);
              final byte[] serializedResponse = out.toByteArray();

              ODistributedServerLog.debug(this, manager.getLocalNodeName(), node, ODistributedServerLog.DIRECTION.OUT,
                  "Sending response %s (%d bytes)", response, serializedResponse.length);

              network.writeBytes(serializedResponse);

            } finally {
              outStream.close();
            }
          } finally {
            out.close();
          }

        } finally {
          storage.endRequest(network);
          // NO RESPONSE: FREE THE CHANNEL
          storage.getEngine().getConnectionManager().release(network);
        }
        return null;
      }
    }, "Cannot send response back to the sender node '" + response.getSenderNodeName() + "'");
  }

}
