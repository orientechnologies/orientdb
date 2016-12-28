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
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.orient.client.binary.OChannelBinarySynchClient;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.Executors;

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
  private       OChannelBinarySynchClient channel;

  private static final int     MAX_RETRY     = 3;
  private static final String  CLIENT_TYPE   = "OrientDB Server";
  private static final boolean COLLECT_STATS = false;
  private              int     sessionId     = -1;
  private byte[] sessionToken;
  private OContextConfiguration contextConfig = new OContextConfiguration();
  private Date                  createdOn     = new Date();

  private volatile     int totalConsecutiveErrors = 0;
  private final static int MAX_CONSECUTIVE_ERRORS = 10;

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

  public void sendRequest(final ODistributedRequest request) {
    networkOperation(OChannelBinaryProtocol.DISTRIBUTED_REQUEST, new OStorageRemoteOperation<Object>() {
      @Override
      public Object execute() throws IOException {
        request.toStream(channel.getDataOutput());
        channel.flush();
        return null;
      }
    }, "Cannot send distributed request", MAX_RETRY, true);

  }

  public void sendResponse(final ODistributedResponse response) {
    networkOperation(OChannelBinaryProtocol.DISTRIBUTED_RESPONSE, new OStorageRemoteOperation<Object>() {
      @Override
      public Object execute() throws IOException {
        response.toStream(channel.getDataOutput());
        channel.flush();
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
    for (int retry = 1; retry <= maxRetry && totalConsecutiveErrors < MAX_CONSECUTIVE_ERRORS; ++retry) {
      try {
        channel.setWaitResponseTimeout();
        channel.beginRequest(operationId, sessionId, sessionToken);

        T result = operation.execute();

        // RESET ERRORS
        totalConsecutiveErrors = 0;

        return result;

      } catch (Exception e) {
        // DIRTY CONNECTION, CLOSE IT AND RE-ACQUIRE A NEW ONE
        lastException = e;
        handleNewError();

        close();

        if (!autoReconnect)
          break;

        if (!manager.isNodeAvailable(server))
          break;

        ODistributedServerLog.warn(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
            "Error on sending message to distributed node (%s) retrying (%d/%d)", lastException.toString(), retry, maxRetry);

        if (retry > 1)
          try {
            Thread.sleep(100 * (retry * 2));
          } catch (InterruptedException e1) {
            break;
          }

        try {
          connect();

          // RESET ERRORS
          totalConsecutiveErrors = 0;

        } catch (IOException e1) {
          lastException = e1;
          handleNewError();

          ODistributedServerLog.warn(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
              "Error on reconnecting to distributed node (%s)", lastException.toString());
        }
      }
    }

    if (lastException == null)
      handleNewError();

    throw OException.wrapException(new ODistributedException(errorMessage), lastException);
  }

  public ODistributedServerManager getManager() {
    return manager;
  }

  public String getServer() {
    return server;
  }

  public Date getCreatedOn() {
    return createdOn;
  }

  private void handleNewError() {
    totalConsecutiveErrors++;

    if (totalConsecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
      ODistributedServerLog.warn(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
          "Reached %d consecutive errors on connection, remove the server '%s' from the cluster", totalConsecutiveErrors, server);

      // REMOVE THE SERVER ASYNCHRONOUSLY
      Executors.newSingleThreadExecutor().execute(new Runnable() {
        @Override
        public void run() {
          try {
            manager.removeServer(server, true);
          } catch (Throwable e) {
            ODistributedServerLog.warn(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
                "Error on removing server '%s' from the cluster", server);
          }
        }
      });

      throw new OIOException("Reached " + totalConsecutiveErrors + " consecutive errors on connection, remove the server '" + server
          + "' from the cluster");
    }
  }
}
