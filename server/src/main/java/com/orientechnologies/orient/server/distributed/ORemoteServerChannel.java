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
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinarySynchClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Remote server channel.
 *
 * @author Luca Garulli
 */
public class ORemoteServerChannel {
  final ODistributedServerManager manager;
  final String                    server;
  final String                    url;
  final String                    remoteHost;
  final int                       remotePort;
  final String                    userName;
  final String                    userPassword;
  OChannelBinarySynchClient       channel;

  static final String             CLIENT_TYPE   = "OrientDB Server";
  static final boolean            COLLECT_STATS = false;
  private static final int        MAX_RETRY     = 3;
  int                             sessionId     = -1;
  byte[]                          sessionToken;
  OContextConfiguration           contextConfig = new OContextConfiguration();

  public ORemoteServerChannel(final ODistributedServerManager manager, final String iServer, final String iURL, final String user,
      final String passwd) throws IOException {
    this.manager = manager;
    this.server = iServer;
    this.url = iURL;
    this.userName = user;
    this.userPassword = passwd;

    final int sepPos = iURL.indexOf(":");
    remoteHost = iURL.substring(0, sepPos);
    remotePort = Integer.parseInt(iURL.substring(sepPos + 1));

    connect();
  }

  protected synchronized <T> T networkOperation(final byte operationId, final OStorageRemoteOperation<T> operation,
      final String errorMessage) {
    Exception lastException = null;
    for (int retry = 1; retry <= MAX_RETRY; ++retry) {
      try {
        channel.setWaitResponseTimeout();
        channel.beginRequest(operationId, sessionId, sessionToken);

        final T result = operation.execute();

        channel.flush();

        return result;

      } catch (IOException e) {
        // IO EXCEPTION: RETRY THE CONNECTION AND COMMAND
        lastException = e;

        ODistributedServerLog.warn(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
            "IO Exception during %s (%s). Retrying (%d/%d)...", operation.toString(), lastException.getMessage(), retry, MAX_RETRY);

        // DIRTY CONNECTION, CLOSE IT AND RE-ACQUIRE A NEW ONE
        for (; retry <= MAX_RETRY; ++retry) {
          try {
            close();

            channel = new OChannelBinarySynchClient(remoteHost, remotePort, null, contextConfig,
                OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION);

            authenticate();

            // OK
            break;

          } catch (IOException e1) {
            // IO EXCEPTION: RETRY THE CONNECTION AND COMMAND
            lastException = e;

            ODistributedServerLog.warn(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
                "IO Exception during %s (%s). Retrying (%d/%d)...", operation.toString(), lastException.getMessage(), retry,
                MAX_RETRY);
          }
        }
      }
    }

    ODistributedServerLog.warn(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
        "IO Exception during %s (%s)", operation.toString(), lastException.getMessage());

    throw OException.wrapException(new ODistributedException(errorMessage),
        new IOException("Cannot connect to remote node " + url, lastException));
  }

  public interface OStorageRemoteOperation<T> {
    T execute() throws IOException;
  }

  public void sendRequest(final ODistributedRequest req, final String node) {
    networkOperation(OChannelBinaryProtocol.DISTRIBUTED_REQUEST, new OStorageRemoteOperation<Object>() {
      @Override
      public Object execute() throws IOException {
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

        return null;
      }

      @Override
      public String toString() {
        return "SEND REQUEST";
      }
    }, "Cannot send distributed request");

  }

  public void sendResponse(final ODistributedResponse response, final String node) {
    networkOperation(OChannelBinaryProtocol.DISTRIBUTED_RESPONSE, new OStorageRemoteOperation<Object>() {
      @Override
      public Object execute() throws IOException {
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

        return null;
      }

      @Override
      public String toString() {
        return "SEND RESPONSE";
      }
    }, "Cannot send response back to the sender node '" + response.getSenderNodeName() + "'");

  }

  public void connect() throws IOException {
    channel = new OChannelBinarySynchClient(remoteHost, remotePort, null, contextConfig,
        OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION);

    networkOperation(OChannelBinaryProtocol.REQUEST_CONNECT, new OStorageRemoteOperation<Void>() {
      @Override
      public Void execute() throws IOException {
        authenticate();
        return null;
      }

      @Override
      public String toString() {
        return "CONNECT";
      }
    }, "Cannot connect to the remote server '" + url + "'");
  }

  public void close() {
    if (channel != null)
      channel.close();
  }

  private void authenticate() throws IOException {
    channel.writeString(CLIENT_TYPE).writeString(OConstants.ORIENT_VERSION)
        .writeShort((short) OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION).writeString("0");
    channel.writeString(ODatabaseDocumentTx.getDefaultSerializer().toString());
    channel.writeBoolean(false); // NO TOKEN
    channel.writeBoolean(false); // SUPPORT PUSH
    channel.writeBoolean(COLLECT_STATS); // COLLECT STATS

    channel.writeString(userName);
    channel.writeString(userPassword);

    channel.flush();

    sessionToken = channel.beginResponse(false);
    if (sessionToken != null && sessionToken.length == 0)
      sessionToken = null;
  }
}
