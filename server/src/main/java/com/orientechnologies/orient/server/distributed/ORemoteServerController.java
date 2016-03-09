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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.remote.OStorageRemoteThreadLocal;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinarySynchClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Remote server controller.
 * 
 * @author Luca Garulli
 */
public class ORemoteServerController {
  private final ODistributedServerManager manager;
  private final String                    url;
  private final String                    remoteHost;
  private final int                       remotePort;
  private OChannelBinarySynchClient       channel;

  private static final String             CLIENT_TYPE     = "RemoteServerController";
  private static final boolean            COLLECT_STATS   = false;
  private final OStorageRemoteThreadLocal storageRemoteTL = OStorageRemoteThreadLocal.INSTANCE;
  private int                             sessionId       = -1;
  private byte[]                          sessionToken;
  private OContextConfiguration           contextConfig   = new OContextConfiguration();

  private interface OStorageRemoteOperation<T> {
    T execute(final OChannelBinarySynchClient network) throws IOException;
  }

  public ORemoteServerController(final ODistributedServerManager manager, final String iURL, final String user, final String passwd)
      throws IOException {
    this.manager = manager;
    this.url = iURL;

    final int sepPos = iURL.indexOf(":");
    remoteHost = iURL.substring(0, sepPos);
    remotePort = Integer.parseInt(iURL.substring(sepPos + 1));

    connect();
  }

  public void sendRequest(final ODistributedRequest req, final String node) {
    networkAdminOperation(new OStorageRemoteOperation<Object>() {
      @Override
      public Object execute(final OChannelBinarySynchClient network) throws IOException {
        network.beginRequest(OChannelBinaryProtocol.DISTRIBUTED_REQUEST, storageRemoteTL.get(), sessionToken);

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
          endRequest();
        }
        return null;
      }
    }, "Cannot send distributed request");
  }

  public void sendResponse(final ODistributedResponse response, final String node) {
    networkAdminOperation(new OStorageRemoteOperation<Object>() {
      @Override
      public Object execute(final OChannelBinarySynchClient network) throws IOException {
        channel.beginRequest(OChannelBinaryProtocol.DISTRIBUTED_RESPONSE, storageRemoteTL.get(), sessionToken);
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
          endRequest();
        }
        return null;
      }
    }, "Cannot send response back to the sender node '" + response.getSenderNodeName() + "'");
  }

  protected <T> T networkAdminOperation(final OStorageRemoteOperation<T> operation, final String errorMessage) {
    try {
      if (channel == null || !channel.isConnected())
        connect();

      channel.acquireWriteLock();

      return operation.execute(channel);

    } catch (Exception e) {
      // DIRTY CONNECTION, CLOSE IT AND RE-ACQUIRE A NEW ONE
      close();
      throw OException.wrapException(new OStorageException(errorMessage), e);
    }
  }

  public void close() {
    if (channel != null)
      channel.close();
  }

  public void connect(final String iUserName, final String iUserPassword) throws IOException {
    networkAdminOperation(new OStorageRemoteOperation<Void>() {
      @Override
      public Void execute(final OChannelBinarySynchClient network) throws IOException {
        try {
          network.beginRequest(OChannelBinaryProtocol.REQUEST_CONNECT, storageRemoteTL.get(), null);

          network.writeString(CLIENT_TYPE).writeString(OConstants.ORIENT_VERSION)
              .writeShort((short) OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION).writeString("0");
          network.writeString(ODatabaseDocumentTx.getDefaultSerializer().toString());
          network.writeBoolean(true);
          network.writeBoolean(false); // SUPPORT PUSH
          network.writeBoolean(COLLECT_STATS); // COLLECT STATS

          network.writeString(iUserName);
          network.writeString(iUserPassword);

          network.flush();

          network.beginResponse(-1, false);
          sessionId = network.readInt();
          sessionToken = network.readBytes();
          if (sessionToken.length == 0) {
            sessionToken = null;
          }
        } finally {
          channel.endResponse();
        }

        return null;
      }
    }, "Cannot connect to the remote server '" + url + "'");
  }

  protected void connect() throws IOException {
    channel = new OChannelBinarySynchClient(remoteHost, remotePort, null, contextConfig,
        OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION);
  }

  private void endRequest() throws IOException {
    channel.flush();
    channel.releaseWriteLock();
  }
}
