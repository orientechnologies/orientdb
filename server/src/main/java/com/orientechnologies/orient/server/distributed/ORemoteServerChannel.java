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
import com.orientechnologies.orient.client.binary.OChannelBinarySynchClient;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Remote server channel.
 *
 * @author Luca Garulli
 */
public class ORemoteServerChannel {
  private final ODistributedServerManager manager;
  private final String                    url;
  private final String                    remoteHost;
  private final int                       remotePort;
  private final String                    userName;
  private final String                    userPassword;
  private final String                    server;
  private OChannelBinarySynchClient       channel;

  private static final int                MAX_RETRY     = 3;
  private static final String             CLIENT_TYPE   = "OrientDB Server";
  private static final boolean            COLLECT_STATS = false;
  private int                             sessionId     = -1;
  private byte[]                          sessionToken;
  private OContextConfiguration           contextConfig = new OContextConfiguration();

  public ORemoteServerChannel(final ODistributedServerManager manager, final String iServer, final String iURL, final String user,
      final String passwd) throws IOException {
    this.manager = manager;
    this.server = iServer;
    this.url = iURL;
    this.userName = user;
    this.userPassword = passwd;

    final int sepPos = iURL.lastIndexOf(":");
    remoteHost = iURL.substring(0, sepPos);
    remotePort = Integer.parseInt(iURL.substring(sepPos + 1));

    connect();
  }

  public interface OStorageRemoteOperation<T> {
    T execute() throws IOException;
  }

  public void sendRequest(final ODistributedRequest req, final String node) {
    networkOperation(OChannelBinaryProtocol.DISTRIBUTED_REQUEST, new OStorageRemoteOperation<Object>() {
      @Override
      public Object execute() throws IOException {
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

              channel.writeBytes(serializedRequest);

            } finally {
              outStream.close();
            }
          } finally {
            out.close();
          }
        } finally {
          channel.flush();
        }

        return null;
      }
    }, "Cannot send distributed request", MAX_RETRY, true);

  }

  public void sendResponse(final ODistributedResponse response, final String node) {
    networkOperation(OChannelBinaryProtocol.DISTRIBUTED_RESPONSE, new OStorageRemoteOperation<Object>() {
      @Override
      public Object execute() throws IOException {
        try {
          final ByteArrayOutputStream out = new ByteArrayOutputStream();
          try {
            final ObjectOutputStream outStream = new ObjectOutputStream(out);
            try {
              response.writeExternal(outStream);
              final byte[] serializedResponse = out.toByteArray();

              ODistributedServerLog.debug(this, manager.getLocalNodeName(), node, ODistributedServerLog.DIRECTION.OUT,
                  "Sending response %s (%d bytes)", response, serializedResponse.length);

              channel.writeBytes(serializedResponse);

            } finally {
              outStream.close();
            }
          } finally {
            out.close();
          }
        } finally {
          channel.flush();
        }

        return null;
      }
    }, "Cannot send response back to the sender node '" + response.getSenderNodeName() + "'", MAX_RETRY, true);

  }

  public void connect() throws IOException {
    channel = new OChannelBinarySynchClient(remoteHost, remotePort, null, contextConfig,
        OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION);

    networkOperation(OChannelBinaryProtocol.REQUEST_CONNECT, new OStorageRemoteOperation<Void>() {
      @Override
      public Void execute() throws IOException {
        channel.writeString(CLIENT_TYPE).writeString(OConstants.ORIENT_VERSION)
            .writeShort((short) OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION).writeString("0");
        channel.writeString(ODatabaseDocumentTx.getDefaultSerializer().toString());
        channel.writeBoolean(false);
        channel.writeBoolean(false); // SUPPORT PUSH
        channel.writeBoolean(COLLECT_STATS); // COLLECT STATS

        channel.writeString(userName);
        channel.writeString(userPassword);

        channel.flush();

        channel.beginResponse(false);
        sessionId = channel.readInt();
        sessionToken = channel.readBytes();
        if (sessionToken.length == 0) {
          sessionToken = null;
        }

        return null;
      }
    }, "Cannot connect to the remote server '" + url + "'", MAX_RETRY, false);
  }

  public void close() {
    if (channel != null)
      channel.close();
    sessionId = -1;
    sessionToken = null;
  }

  protected synchronized <T> T networkOperation(final byte operationId, final OStorageRemoteOperation<T> operation,
      final String errorMessage, final int maxRetry, final boolean autoReconnect) {
    Exception lastException = null;
    for (int retry = 1; retry <= maxRetry; ++retry) {
      try {
        channel.setWaitResponseTimeout();
        channel.beginRequest(operationId, sessionId, sessionToken);

        return operation.execute();

      } catch (Exception e) {
        // DIRTY CONNECTION, CLOSE IT AND RE-ACQUIRE A NEW ONE
        lastException = e;

        close();

        if (!autoReconnect)
          break;

        ODistributedServerLog.warn(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
            "Error on sending message to distributed node (%s) retrying (%d/%d)", lastException.toString(), retry, maxRetry);

        try {
          Thread.sleep(100 * (retry * 2));
        } catch (InterruptedException e1) {
          break;
        }

        if (!manager.isNodeAvailable(server))
          break;

        try {
          connect();
        } catch (IOException e1) {
          lastException = e1;

          ODistributedServerLog.warn(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
              "Error on reconnecting to distributed node (%s)", lastException.toString());
        }
      }
    }

    throw OException.wrapException(new OStorageException(errorMessage), lastException);
  }
}