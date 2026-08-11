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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.client.binary.OChannelBinarySynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.message.ODistributedConnectRequest;
import com.orientechnologies.orient.client.remote.message.ODistributedConnectResponse;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.ONetworkMessage;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.binary.OBinaryTokenSerializer;
import com.orientechnologies.orient.enterprise.channel.OSocketFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import com.orientechnologies.orient.enterprise.channel.binary.OResponseProcessingException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * Remote server channel.
 *
 * @author Luca Garulli
 */
public class ORemoteServerChannel {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(ORemoteServerChannel.class);

  private final ORemoteServerAvailabilityCheck check;
  private final String url;
  private final String remoteHost;
  private final int remotePort;
  private final String userName;
  private final String userPassword;
  private final ONodeId server;
  private OChannelBinarySynchClient channel;
  private int protocolVersion;
  private final ONodeId localNode;

  private static final int MAX_RETRY = 3;
  private static final String CLIENT_TYPE = "OrientDB Server";
  private int sessionId = -1;
  private byte[] sessionToken;
  private OToken tokenInstance = null;
  private final OBinaryTokenSerializer tokenDeserializer = new OBinaryTokenSerializer();
  private final OContextConfiguration contextConfig = new OContextConfiguration();
  private final Date createdOn = new Date();

  private volatile int totalConsecutiveErrors = 0;
  private static final int MAX_CONSECUTIVE_ERRORS = 10;
  private final ExecutorService executor;
  private final OSocketFactory factory;

  public ORemoteServerChannel(
      final ORemoteServerAvailabilityCheck check,
      final ONodeId localNode,
      final ONodeId iServer,
      final String iURL,
      final String user,
      final String passwd,
      final int currentProtocolVersion,
      ExecutorService exec) {
    this.check = check;
    this.localNode = localNode;
    this.server = iServer;
    this.url = iURL;
    this.userName = user;
    this.userPassword = passwd;

    final int sepPos = iURL.lastIndexOf(":");
    remoteHost = iURL.substring(0, sepPos);
    remotePort = Integer.parseInt(iURL.substring(sepPos + 1));
    protocolVersion = currentProtocolVersion;

    this.executor = exec;
    factory = new OSocketFactory(contextConfig);
  }

  public void connect() {
    executor.execute(
        () -> {
          try {
            sendConnect();
          } catch (IOException e) {
            handleNewError();
          }
        });
  }

  public int getDistributedProtocolVersion() {
    return protocolVersion;
  }

  public void sendBinaryRequest(OBinaryRequest request) {
    executor.execute(
        () -> {
          networkOperation(
              request.getCommand(),
              (ch) -> {
                request.write(ch.getChannelDataOutput());
                ch.getChannelDataOutput().flush();
                return null;
              },
              "Cannot send distributed request " + request.getClass(),
              MAX_RETRY,
              true);
        });
  }

  public interface ORemoteClientOperation<T> {
    T execute(OChannelBinarySynchClient channel) throws IOException;
  }

  public void checkReconnect() {
    if (channel == null || tokenInstance == null || tokenInstance.isCloseToExpire()) {
      for (int retry = 1;
          retry <= MAX_RETRY && totalConsecutiveErrors < MAX_CONSECUTIVE_ERRORS;
          ++retry) {
        try {
          sendConnect();
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
      byte operationId, ORemoteClientOperation<T> operation, String errorMessage) {
    try {
      executor.execute(
          () -> {
            checkReconnect();
            networkOperation(operationId, operation, errorMessage, MAX_RETRY, true);
          });
    } catch (RejectedExecutionException e) {
      check.nodeDisconnected(server);
    }
  }

  public void sendMessage(final ONetworkMessage message) {
    executeNetworkOperation(
        OChannelBinaryProtocol.DISTRIBUTED_MESSAGE,
        (ch) -> {
          message.serialize(ch.getDataOutput());
          ch.getDataOutput().flush();
          return null;
        },
        "Cannot send distributed request " + message.getClass());
  }

  public void sendRequest(final ODistributedRequest request) {
    executeNetworkOperation(
        OChannelBinaryProtocol.DISTRIBUTED_REQUEST,
        (ch) -> {
          request.toStream(ch.getDataOutput());
          ch.getDataOutput().flush();
          return null;
        },
        "Cannot send distributed request " + request.getClass());
  }

  public void sendResponse(final ODistributedResponse response) {
    ORemoteClientOperation<Object> remoteOperation =
        (ch) -> {
          response.toStream(ch.getDataOutput());
          ch.getDataOutput().flush();
          return null;
        };
    executeNetworkOperation(
        OChannelBinaryProtocol.DISTRIBUTED_RESPONSE,
        remoteOperation,
        "Cannot send response back to the sender node '"
            + response.getSenderNodeName()
            + "' "
            + response.getClass());
  }

  public void sendConnect() throws IOException {
    networkClose();
    channel =
        new OChannelBinarySynchClient(
            factory,
            remoteHost,
            remotePort,
            null,
            contextConfig,
            OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION);

    networkOperation(
        OChannelBinaryProtocol.DISTRIBUTED_CONNECT,
        (ch) -> {
          ODistributedConnectRequest request =
              new ODistributedConnectRequest(protocolVersion, userName, userPassword);
          request.write(ch.getChannelDataOutput());
          ch.getChannelDataOutput().flush();

          readResponse(ch.getChannelDataInput(), ch.getSrvProtocolVersion());
          ODistributedConnectResponse response = request.createResponse();
          response.read(ch.getChannelDataInput());
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

  protected byte[] readResponse(OChannelDataInput input, int srvProtocolVersion)
      throws IOException {
    byte currentStatus = input.readByte();
    int currentSessionId = input.readInt();

    byte[] tokenBytes = input.readBytes();
    int opCode = input.readByte();
    handleStatus(currentStatus, currentSessionId, input, srvProtocolVersion);
    return tokenBytes;
  }

  public void close() {
    networkClose();
  }

  private void networkClose() {
    if (channel != null) channel.close();

    sessionId = -1;
    sessionToken = null;
  }

  public void beginRequest(
      OChannelDataOutput output, final byte iCommand, final int sessionId, final byte[] token)
      throws IOException {

    output.writeByte(iCommand);
    output.writeInt(sessionId);
    output.writeBytes(token);
  }

  protected synchronized <T> T networkOperation(
      final byte operationId,
      final ORemoteClientOperation<T> operation,
      final String errorMessage,
      final int maxRetry,
      final boolean autoReconnect) {
    Exception lastException = null;
    for (int retry = 1;
        retry <= maxRetry && totalConsecutiveErrors < MAX_CONSECUTIVE_ERRORS;
        ++retry) {
      try {
        channel.setWaitResponseTimeout();
        beginRequest(channel.getChannelDataOutput(), operationId, sessionId, sessionToken);

        T result = operation.execute(channel);

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
          sendConnect();

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

  public ONodeId getServer() {
    return server;
  }

  public Date getCreatedOn() {
    return createdOn;
  }

  private void handleNewError() {
    totalConsecutiveErrors++;

    if (totalConsecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
      logger.warnOut(
          localNode.getNode(),
          server.getNode(),
          "Reached %d consecutive errors on connection, remove the server '%s' from the cluster",
          totalConsecutiveErrors,
          server);

      try {
        check.nodeDisconnected(server);
      } catch (Exception e) {
        logger.warnOut(
            localNode.getNode(),
            server.getNode(),
            "Error on removing server '%s' from the cluster",
            server);
      }
    }
  }

  protected static int handleStatus(
      final byte iResult, final int iClientTxId, OChannelDataInput input, int srvProtocolVersion)
      throws IOException {
    if (iResult == OChannelBinaryProtocol.RESPONSE_STATUS_OK
        || iResult == OChannelBinaryProtocol.PUSH_DATA) {
      return iClientTxId;
    } else if (iResult == OChannelBinaryProtocol.RESPONSE_STATUS_ERROR) {

      final List<OPair<String, String>> exceptions = new ArrayList<OPair<String, String>>();

      // EXCEPTION
      while (input.readByte() == 1) {
        final String excClassName = input.readString();
        final String excMessage = input.readString();
        exceptions.add(new OPair<String, String>(excClassName, excMessage));
      }

      byte[] serializedException = null;
      if (srvProtocolVersion >= 19) serializedException = input.readBytes();

      Exception previous = null;

      if (serializedException != null && serializedException.length > 0)
        throwSerializedException(serializedException);

      for (int i = exceptions.size() - 1; i > -1; --i) {
        previous =
            createException(exceptions.get(i).getKey(), exceptions.get(i).getValue(), previous);
      }

      if (previous != null) {
        throw new RuntimeException(previous);
      } else throw new ONetworkProtocolException("Network response error");

    } else {
      // PROTOCOL ERROR
      // close();
      throw new ONetworkProtocolException("Error on reading response from the server");
    }
  }

  protected static void throwSerializedException(final byte[] serializedException)
      throws IOException {
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(serializedException);
    final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);

    Object throwable = null;
    try {
      throwable = objectInputStream.readObject();
    } catch (ClassNotFoundException e) {
      logger.error("Error during exception deserialization", e);
      throw new IOException("Error during exception deserialization: " + e.toString(), e);
    }

    objectInputStream.close();

    if (throwable instanceof OException) {
      try {
        final Class<? extends OException> cls = (Class<? extends OException>) throwable.getClass();
        final Constructor<? extends OException> constructor;
        constructor = cls.getConstructor(cls);
        final OException proxyInstance = constructor.newInstance(throwable);

        throw proxyInstance;

      } catch (NoSuchMethodException e) {
        logger.error("Error during exception deserialization", e);
      } catch (InvocationTargetException e) {
        logger.error("Error during exception deserialization", e);
      } catch (InstantiationException e) {
        logger.error("Error during exception deserialization", e);
      } catch (IllegalAccessException e) {
        logger.error("Error during exception deserialization", e);
      }
    }

    if (throwable instanceof Throwable) {
      throw new OResponseProcessingException(
          "Exception during response processing", (Throwable) throwable);
    } else {
      // WRAP IT
      String exceptionType = throwable != null ? throwable.getClass().getName() : "null";
      logger.error(
          "Error during exception serialization, serialized exception is not Throwable,"
              + " exception type is %s",
          null, exceptionType);
    }
  }

  @SuppressWarnings("unchecked")
  private static RuntimeException createException(
      final String iClassName, final String iMessage, final Exception iPrevious) {
    RuntimeException rootException = null;
    Constructor<?> c = null;
    try {
      final Class<RuntimeException> excClass = (Class<RuntimeException>) Class.forName(iClassName);
      if (iPrevious != null) {
        try {
          c = excClass.getConstructor(String.class, Throwable.class);
        } catch (NoSuchMethodException e) {
          c = excClass.getConstructor(String.class, Exception.class);
        }
      }

      if (c == null) c = excClass.getConstructor(String.class);

    } catch (Exception e) {
      // UNABLE TO REPRODUCE THE SAME SERVER-SIZE EXCEPTION: THROW AN IO EXCEPTION
      rootException = OException.wrapException(new OSystemException(iMessage), iPrevious);
    }

    if (c != null)
      try {
        final Exception cause;
        if (c.getParameterTypes().length > 1)
          cause = (Exception) c.newInstance(iMessage, iPrevious);
        else cause = (Exception) c.newInstance(iMessage);

        rootException =
            OException.wrapException(new OSystemException("Data processing exception"), cause);
      } catch (InstantiationException ignored) {
      } catch (IllegalAccessException ignored) {
      } catch (InvocationTargetException ignored) {
      }

    return rootException;
  }
}
