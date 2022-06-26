/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.thread.OThreadPoolExecutors;
import com.orientechnologies.orient.client.binary.OChannelBinarySynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.message.ODistributedConnectRequest;
import com.orientechnologies.orient.client.remote.message.ODistributedConnectResponse;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.binary.OBinaryTokenSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.TimeUnit;

/**
 * Remote server channel.
 *
 * @author Luca Garulli
 */
public class ORemoteServerChannel {
  private final ORemoteServerAvailabilityCheck check;
  private final String url;
  private final String remoteHost;
  private final int remotePort;
  private final String userName;
  private final String userPassword;
  private final String server;
  private OChannelBinarySynchClient channel;
  private int protocolVersion;
  private ODistributedRequest prevRequest;
  private ODistributedResponse prevResponse;
  private final String localNodeName;

  private static final int MAX_RETRY = 3;
  private static final String CLIENT_TYPE = "OrientDB Server";
  private static final boolean COLLECT_STATS = false;
  private int sessionId = -1;
  private byte[] sessionToken;
  private OToken tokenInstance = null;
  private final OBinaryTokenSerializer tokenDeserializer = new OBinaryTokenSerializer();
  private final OContextConfiguration contextConfig = new OContextConfiguration();
  private final Date createdOn = new Date();

  private volatile int totalConsecutiveErrors = 0;
  private static final int MAX_CONSECUTIVE_ERRORS = 10;
  private final ExecutorService executor;

  public ORemoteServerChannel(
      final ORemoteServerAvailabilityCheck check,
      String localNodeName,
      final String iServer,
      final String iURL,
      final String user,
      final String passwd,
      final int currentProtocolVersion)
      throws IOException {
    this.check = check;
    this.localNodeName = localNodeName;
    this.server = iServer;
    this.url = iURL;
    this.userName = user;
    this.userPassword = passwd;

    final int sepPos = iURL.lastIndexOf(":");
    remoteHost = iURL.substring(0, sepPos);
    remotePort = Integer.parseInt(iURL.substring(sepPos + 1));
    long timeout =
        contextConfig.getValueAsLong(OGlobalConfiguration.DISTRIBUTED_TX_EXPIRE_TIMEOUT) / 2;
    protocolVersion = currentProtocolVersion;
    RejectedExecutionHandler reject =
        (task, executor) -> {
          try {
            if (!executor.getQueue().offer(task, timeout, TimeUnit.MILLISECONDS)) {
              check.nodeDisconnected(server);
              throw new RejectedExecutionException("Unable to enqueue task");
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RejectedExecutionException("Unable to enqueue task");
          }
        };

    executor = OThreadPoolExecutors.newSingleThreadPool("ORemoteServerChannel", 10, reject);

    connect();
  }

  public int getDistributedProtocolVersion() {
    return protocolVersion;
  }

  public void sendBinaryRequest(OBinaryRequest request) {
    executor.execute(
        () -> {
          networkOperation(
              request.getCommand(),
              () -> {
                request.write(channel, null);
                channel.flush();
                return null;
              },
              "Cannot send distributed request " + request.getClass(),
              MAX_RETRY,
              true);
        });
  }

  public interface OStorageRemoteOperation<T> {
    T execute() throws IOException;
  }

  public void checkReconnect() {
    if (tokenInstance == null || tokenInstance.isCloseToExpire()) {
      for (int retry = 1;
          retry <= MAX_RETRY && totalConsecutiveErrors < MAX_CONSECUTIVE_ERRORS;
          ++retry) {
        try {
          connect();
          totalConsecutiveErrors = 0;
          break;
        } catch (Exception e1) {
          handleNewError();
          if (retry > 1) {
            try {
              Thread.sleep(100 * (retry * 2));
            } catch (InterruptedException e2) {
              break;
            }
          }
        }
      }
    }
  }

  private <T> void executeNetworkOperation(
      final byte operationId,
      final OStorageRemoteOperation<T> operation,
      final String errorMessage,
      final int maxRetry,
      final boolean autoReconnect) {
    executor.execute(
        () -> {
          if (autoReconnect) {
            checkReconnect();
          }
          networkOperation(operationId, operation, errorMessage, maxRetry, autoReconnect);
        });
  }

  public void sendRequest(final ODistributedRequest request) {
    executeNetworkOperation(
        OChannelBinaryProtocol.DISTRIBUTED_REQUEST,
        () -> {
          request.toStream(channel.getDataOutput());
          channel.flush();
          return null;
        },
        "Cannot send distributed request " + request.getClass(),
        MAX_RETRY,
        true);
    this.prevRequest = request;
  }

  public void sendResponse(final ODistributedResponse response) {
    OStorageRemoteOperation<Object> remoteOperation =
        () -> {
          response.toStream(channel.getDataOutput());
          channel.flush();
          return null;
        };
    executeNetworkOperation(
        OChannelBinaryProtocol.DISTRIBUTED_RESPONSE,
        remoteOperation,
        "Cannot send response back to the sender node '"
            + response.getSenderNodeName()
            + "' "
            + response.getClass(),
        MAX_RETRY,
        true);
    this.prevResponse = response;
  }

  public void connect() throws IOException {
    networkClose();
    channel =
        new OChannelBinarySynchClient(
            remoteHost,
            remotePort,
            null,
            contextConfig,
            OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION);

    networkOperation(
        OChannelBinaryProtocol.DISTRIBUTED_CONNECT,
        () -> {
          ODistributedConnectRequest request =
              new ODistributedConnectRequest(protocolVersion, userName, userPassword);
          request.write(channel, null);
          channel.flush();

          channel.beginResponse(true);
          ODistributedConnectResponse response = request.createResponse();
          response.read(channel, null);
          sessionId = response.getSessionId();
          if (response.getToken() != null) {
            sessionToken = response.getToken();
            tokenInstance = tokenDeserializer.deserialize(new ByteArrayInputStream(sessionToken));
          }

          // SET THE PROTOCOL TO THE MINIMUM NUMBER TO SUPPORT BACKWARD COMPATIBILITY
          protocolVersion = response.getDistributedProtocolVersion();

          return null;
        },
        "Cannot connect to the remote server '" + url + "'",
        MAX_RETRY,
        false);
  }

  public void close() {
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    networkClose();
  }

  private void networkClose() {
    if (channel != null) channel.close();

    sessionId = -1;
    sessionToken = null;
  }

  protected synchronized <T> T networkOperation(
      final byte operationId,
      final OStorageRemoteOperation<T> operation,
      final String errorMessage,
      final int maxRetry,
      final boolean autoReconnect) {
    Exception lastException = null;
    for (int retry = 1;
        retry <= maxRetry && totalConsecutiveErrors < MAX_CONSECUTIVE_ERRORS;
        ++retry) {
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

        networkClose();

        if (!autoReconnect) break;

        if (!check.isNodeAvailable(server)) break;

        if (retry > 1) {
          try {
            Thread.sleep(100 * (retry * 2));
          } catch (InterruptedException e1) {
            break;
          }
        }

        try {
          connect();

          // RESET ERRORS
          totalConsecutiveErrors = 0;

        } catch (IOException e1) {
          lastException = e1;
          handleNewError();
        }
      }
    }

    if (lastException == null) handleNewError();

    return null;
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
      ODistributedServerLog.warn(
          this,
          localNodeName,
          server,
          ODistributedServerLog.DIRECTION.OUT,
          "Reached %d consecutive errors on connection, remove the server '%s' from the cluster",
          totalConsecutiveErrors,
          server);

      try {
        check.nodeDisconnected(server);
      } catch (Exception e) {
        ODistributedServerLog.warn(
            this,
            localNodeName,
            server,
            ODistributedServerLog.DIRECTION.OUT,
            "Error on removing server '%s' from the cluster",
            server);
      }
    }
  }
}
