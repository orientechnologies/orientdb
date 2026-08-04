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
package com.orientechnologies.orient.client.binary;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.client.remote.message.OError37Response;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.enterprise.channel.OSocketFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInputBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutputBinary;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import com.orientechnologies.orient.enterprise.channel.binary.OResponseProcessingException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;

public class OChannelBinaryAsynchClient extends OChannelBinary {
  private static final OLogger logger =
      OLogManager.instance().logger(OChannelBinaryAsynchClient.class);
  private int socketTimeout; // IN MS
  protected final short srvProtocolVersion;
  private final String serverURL;
  private volatile long lastUse;
  private volatile boolean inUse;

  public OChannelBinaryAsynchClient(
      String remoteHost,
      int remotePort,
      OContextConfiguration config,
      OSocketFactory factory,
      int iProtocolVersion)
      throws IOException {
    super(factory.createSocket(), config);
    try {

      serverURL = remoteHost + ":" + remotePort;
      socketTimeout = config.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT);

      try {
        socket.connect(new InetSocketAddress(remoteHost, remotePort), getSocketTimeout());
        setReadResponseTimeout();
        connected();
      } catch (java.net.SocketTimeoutException e) {
        throw new IOException("Cannot connect to host " + remoteHost + ":" + remotePort, e);
      }
      try {
        if (socketBufferSize > 0) {
          inStream = new BufferedInputStream(socket.getInputStream(), socketBufferSize);
          outStream = new BufferedOutputStream(socket.getOutputStream(), socketBufferSize);
        } else {
          inStream = new BufferedInputStream(socket.getInputStream());
          outStream = new BufferedOutputStream(socket.getOutputStream());
        }

        in = new DataInputStream(inStream);
        out = new DataOutputStream(outStream);
        inChannel =
            new OChannelDataInputBinary(in, this.maxChunkSize, this::updateMetricReceivedBytes);
        outChannel =
            new OChannelDataOutputBinary(
                out,
                this.maxChunkSize,
                this::updateMetricTransmittedBytes,
                this::updateMetricFlushes);
        initDebug();

        srvProtocolVersion = inChannel.readShort();

        outChannel.writeByte(OChannelBinaryProtocol.REQUEST_HANDSHAKE);
        outChannel.writeShort((short) iProtocolVersion);
        outChannel.writeString("Java Client");
        outChannel.writeString(OConstants.getVersion());
        outChannel.writeByte(OChannelBinaryProtocol.ENCODING_DEFAULT);
        outChannel.writeByte(OChannelBinaryProtocol.ERROR_MESSAGE_JAVA);
        outChannel.flush();
      } catch (IOException e) {
        throw new ONetworkProtocolException(
            "Cannot read protocol version from remote server "
                + socket.getRemoteSocketAddress()
                + ": "
                + e);
      }

      if (srvProtocolVersion != iProtocolVersion) {
        logger.warn(
            "The Client driver version is different than Server version: client=%d, server=%d. You"
                + " could not use the full features of the newer version. Assure to have the same"
                + " versions on both",
            iProtocolVersion, srvProtocolVersion);
      }

    } catch (RuntimeException e) {
      if (socket.isConnected()) socket.close();
      throw e;
    }
  }

  @Override
  public void wrapStreams(WrapStreams stre) throws IOException {
    var wrapped = stre.wrap(socket.getInputStream(), socket.getOutputStream());
    if (socketBufferSize > 0) {
      inStream = new BufferedInputStream(wrapped.input(), socketBufferSize);
      outStream = new BufferedOutputStream(wrapped.output(), socketBufferSize);
    } else {
      inStream = new BufferedInputStream(wrapped.input());
      outStream = new BufferedOutputStream(wrapped.output());
    }

    in = new DataInputStream(inStream);
    out = new DataOutputStream(outStream);
    inChannel = new OChannelDataInputBinary(in, this.maxChunkSize, this::updateMetricReceivedBytes);
    outChannel =
        new OChannelDataOutputBinary(
            out, this.maxChunkSize, this::updateMetricTransmittedBytes, this::updateMetricFlushes);
    initDebug();
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
      // UNABLE TO REPRODUCE THE SAME SERVER-SIDE EXCEPTION: THROW AN SYSTEM EXCEPTION
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

  public byte[] beginResponse(final int iRequesterId, final boolean token) throws IOException {
    return beginResponse(iRequesterId, timeout, token);
  }

  public byte[] beginResponse(final int iRequesterId, final long iTimeout, final boolean token)
      throws IOException {
    try {
      // WAIT FOR THE RESPONSE
      if (iTimeout <= 0) acquireReadLock();

      if (!isConnected()) {
        releaseReadLock();
        throw new IOException("Channel is closed");
      }
      byte currentStatus;
      int currentSessionId;
      try {
        setWaitResponseTimeout();
        currentStatus = getChannelDataInput().readByte();
        currentSessionId = getChannelDataInput().readInt();

        if (debug)
          logger.debug(
              "%s - Read response: %d-%d",
              socket.getLocalAddress(), (int) currentStatus, currentSessionId);

      } finally {
        setReadResponseTimeout();
      }

      assert (currentSessionId == iRequesterId);

      if (debug)
        logger.debug("%s - Session %d handle response", socket.getLocalAddress(), iRequesterId);
      byte[] tokenBytes;
      if (token) tokenBytes = getChannelDataInput().readBytes();
      else tokenBytes = null;

      byte currentMessage = getChannelDataInput().readByte();
      handleStatus(currentStatus, currentSessionId);
      return tokenBytes;
    } catch (OLockException e) {
      Thread.currentThread().interrupt();
      // NEVER HAPPENS?
      logger.error("Unexpected error on reading response from channel", e);
    }
    return null;
  }

  public void endResponse() throws IOException {
    // WAKE UP ALL THE WAITING THREADS
    try {
      releaseReadLock();
    } catch (IllegalMonitorStateException e) {
      // IGNORE IT
      logger.debug("Error on unlocking network channel after reading response");
    }
  }

  public void endRequest() throws IOException {
    releaseWriteLock();
  }

  @Override
  public void close() {
    try {
      super.close();
    } catch (Exception e) {
      // IGNORE IT
    }
  }

  @Override
  public void clearInput() throws IOException {
    acquireReadLock();
    try {
      super.clearInput();
    } finally {
      releaseReadLock();
    }
  }

  /**
   * Tells if the channel is connected.
   *
   * @return true if it's connected, otherwise false.
   */
  public boolean isConnected() {
    final Socket s = socket;
    return s != null
        && !s.isClosed()
        && s.isConnected()
        && !s.isInputShutdown()
        && !s.isOutputShutdown();
  }

  /** Gets the major supported protocol version */
  public short getSrvProtocolVersion() {
    return srvProtocolVersion;
  }

  public String getServerURL() {
    return serverURL;
  }

  public boolean tryLock() {
    return getLockWrite().tryAcquireLock();
  }

  public void unlock() {
    getLockWrite().unlock();
  }

  public interface ExceptionHandler {
    void onException(Throwable ex);
  }

  public int handleStatus(
      final byte iResult, final int iClientTxId, ExceptionHandler exceptionHandler)
      throws IOException {
    if (iResult == OChannelBinaryProtocol.RESPONSE_STATUS_OK
        || iResult == OChannelBinaryProtocol.PUSH_DATA) {
      return iClientTxId;
    } else if (iResult == OChannelBinaryProtocol.RESPONSE_STATUS_ERROR) {

      OError37Response response = new OError37Response();
      response.read(this.getChannelDataInput());
      byte[] serializedException = response.getVerbose();
      Exception previous = null;
      if (serializedException != null && serializedException.length > 0) {
        Throwable deserializeException = deserializeException(serializedException);
        exceptionHandler.onException(deserializeException);
      }

      for (Map.Entry<String, String> entry : response.getMessages().entrySet()) {
        previous = createException(entry.getKey(), entry.getValue(), previous);
      }

      if (previous != null) {
        exceptionHandler.onException(new RuntimeException(previous));
      } else exceptionHandler.onException(new ONetworkProtocolException("Network response error"));

    } else {
      // PROTOCOL ERROR
      // close();
      exceptionHandler.onException(
          new ONetworkProtocolException("Error on reading response from the server"));
    }

    return iClientTxId;
  }

  public int handleStatus(final byte iResult, final int iClientTxId) throws IOException {
    return handleStatus(iResult, iClientTxId, this::handleException);
  }

  private void setReadResponseTimeout() throws SocketException {
    final Socket s = socket;
    if (s != null && s.isConnected() && !s.isClosed()) s.setSoTimeout(getSocketTimeout());
  }

  private Throwable deserializeException(final byte[] serializedException) throws IOException {
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
    return (Throwable) throwable;
  }

  public void handleException(Throwable throwable) {
    if (throwable instanceof OException) {
      try {
        final Class<? extends OException> cls = (Class<? extends OException>) throwable.getClass();
        final Constructor<? extends OException> constructor;
        constructor = cls.getConstructor(cls);
        final OException proxyInstance = constructor.newInstance(throwable);
        proxyInstance.addSuppressed((Exception) throwable);
        throw proxyInstance;

      } catch (NoSuchMethodException
          | InvocationTargetException
          | InstantiationException
          | IllegalAccessException e) {
        logger.error("Error during exception deserialization", e);
      }
    }

    if (throwable instanceof RuntimeException) {
      throw (RuntimeException) throwable;
    }
    if (throwable instanceof Throwable) {
      throw new OResponseProcessingException(
          "Exception during response processing", (Throwable) throwable);
    } else {
      // WRAP IT
      String exceptionType = throwable != null ? throwable.getClass().getName() : "null";
      logger.error(
          "Error during exception serialization, serialized exception is not Throwable,"
              + " exception type is "
              + exceptionType,
          null);
    }
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

  public void setSocketTimeout(int socketTimeout) {
    this.socketTimeout = socketTimeout;
  }

  private void markLastUse() {
    lastUse = System.currentTimeMillis();
  }

  public long getLastUse() {
    return lastUse;
  }

  public void markReturned() {
    markLastUse();
    inUse = false;
  }

  public void markInUse() {
    markLastUse();
    inUse = false;
  }

  public boolean isInUse() {
    return inUse;
  }
}
