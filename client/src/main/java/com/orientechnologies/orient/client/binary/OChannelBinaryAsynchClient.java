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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.client.remote.OStorageRemoteNodeSession;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.client.remote.message.OErrorResponse;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.enterprise.channel.OSocketFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;
import com.orientechnologies.orient.enterprise.channel.binary.OResponseProcessingException;

public class OChannelBinaryAsynchClient extends OChannelBinary {
  private         int   socketTimeout;                                               // IN MS
  protected final short srvProtocolVersion;
  private final Condition readCondition = getLockRead().getUnderlying().newCondition();
  private final int    maxUnreadResponses;
  private       String serverURL;
  private volatile boolean channelRead = false;
  private          byte                        currentStatus;
  private          int                         currentSessionId;
  private volatile OAsynchChannelServiceThread serviceThread;

  public OChannelBinaryAsynchClient(final String remoteHost, final int remotePort, final String iDatabaseName,
      final OContextConfiguration iConfig, final int iProtocolVersion) throws IOException {
    this(remoteHost, remotePort, iDatabaseName, iConfig, iProtocolVersion, null);
  }

  public OChannelBinaryAsynchClient(final String remoteHost, final int remotePort, final String iDatabaseName,
      final OContextConfiguration iConfig, final int protocolVersion, final ORemoteServerEventListener asynchEventListener)
      throws IOException {
    super(OSocketFactory.instance(iConfig).createSocket(), iConfig);
    try {

      maxUnreadResponses = iConfig.getValueAsInteger(OGlobalConfiguration.NETWORK_BINARY_READ_RESPONSE_MAX_TIMES);
      serverURL = remoteHost + ":" + remotePort;
      if (iDatabaseName != null)
        serverURL += "/" + iDatabaseName;
      socketTimeout = iConfig.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT);

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

        srvProtocolVersion = readShort();

        writeByte(OChannelBinaryProtocol.REQUEST_HANDSHAKE);
        writeShort((short) protocolVersion);
        writeString("Java Client");
        writeString(OConstants.getVersion());
        flush();
      } catch (IOException e) {
        throw new ONetworkProtocolException(
            "Cannot read protocol version from remote server " + socket.getRemoteSocketAddress() + ": " + e);
      }

      if (srvProtocolVersion != protocolVersion) {
        OLogManager.instance().warn(this,
            "The Client driver version is different than Server version: client=" + protocolVersion + ", server="
                + srvProtocolVersion
                + ". You could not use the full features of the newer version. Assure to have the same versions on both");
      }

//      if (asynchEventListener != null)
//        serviceThread = new OAsynchChannelServiceThread(asynchEventListener, this);
    } catch (RuntimeException e) {
      if (socket.isConnected())
        socket.close();
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  private static RuntimeException createException(final String iClassName, final String iMessage, final Exception iPrevious) {
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

      if (c == null)
        c = excClass.getConstructor(String.class);

    } catch (Exception e) {
      // UNABLE TO REPRODUCE THE SAME SERVER-SIDE EXCEPTION: THROW AN SYSTEM EXCEPTION
      rootException = OException.wrapException(new OSystemException(iMessage), iPrevious);
    }

    if (c != null)
      try {
        final Exception cause;
        if (c.getParameterTypes().length > 1)
          cause = (Exception) c.newInstance(iMessage, iPrevious);
        else
          cause = (Exception) c.newInstance(iMessage);

        rootException = OException.wrapException(new OSystemException("Data processing exception"), cause);
      } catch (InstantiationException ignored) {
      } catch (IllegalAccessException ignored) {
      } catch (InvocationTargetException ignored) {
      }

    return rootException;
  }

  public byte[] beginResponse(final int iRequesterId, final boolean token) throws IOException {
    return beginResponse(iRequesterId, timeout, token);
  }

  public byte[] beginResponse(final int iRequesterId, final long iTimeout, final boolean token) throws IOException {
    try {
      int unreadResponse = 0;
      final long startClock = iTimeout > 0 ? System.currentTimeMillis() : 0;

      // WAIT FOR THE RESPONSE
      do {
        if (iTimeout <= 0)
          acquireReadLock();
        else if (!getLockRead().tryAcquireLock(iTimeout, TimeUnit.MILLISECONDS))
          throw new OTimeoutException("Cannot acquire read lock against channel: " + this);

        boolean readLock = true;

        if (!isConnected()) {
          releaseReadLock();
          throw new IOException("Channel is closed");
        }

        if (!channelRead) {
          channelRead = true;

          try {
            setWaitResponseTimeout();
            currentStatus = readByte();
            currentSessionId = readInt();

            if (debug)
              OLogManager.instance()
                  .debug(this, "%s - Read response: %d-%d", socket.getLocalAddress(), (int) currentStatus, currentSessionId);

          } catch (IOException e) {
            // UNLOCK THE RESOURCE AND PROPAGATES THE EXCEPTION
            channelRead = false;
            readCondition.signalAll();
            releaseReadLock();
            readLock = false;

            throw e;
          } finally {
            setReadResponseTimeout();
          }
        }

        if (currentSessionId == iRequesterId)
          // IT'S FOR ME
          break;

        try {
          if (debug)
            OLogManager.instance()
                .debug(this, "%s - Session %d skip response, it is for %d", socket.getLocalAddress(), iRequesterId,
                    currentSessionId);

          if (iTimeout > 0 && (System.currentTimeMillis() - startClock) > iTimeout) {
            readLock = false;

            throw new IOException(
                "Timeout on reading response from the server " + (socket != null ? socket.getRemoteSocketAddress() : "")
                    + " for the request " + iRequesterId);
          }

          // IN CASE OF TOO MUCH TIME FOR READ A MESSAGE, ASYNC THREAD SHOULD NOT BE INCLUDE IN THIS CHECK
          if (unreadResponse > maxUnreadResponses && iRequesterId != Integer.MIN_VALUE) {
            if (debug)
              OLogManager.instance().info(this, "Unread responses %d > %d, consider the buffer as dirty: clean it", unreadResponse,
                  maxUnreadResponses);

            readLock = false;

            throw new IOException("Timeout on reading response");
          }

          readCondition.signalAll();

          if (debug)
            OLogManager.instance().debug(this, "Session %d is going to sleep...", iRequesterId);

          final long start = System.currentTimeMillis();

          // WAIT MAX 30 SECOND AND RETRY, THIS IS UNBLOCKED BY ANOTHER THREAD IN CASE THE RESPONSE FOR THIS IS ARRIVED
          readCondition.await(30, TimeUnit.SECONDS);

          if (debug) {
            final long now = System.currentTimeMillis();
            OLogManager.instance()
                .debug(this, "Waked up: slept %dms, checking again from %s for session %d", (now - start), socket.getLocalAddress(),
                    iRequesterId);
          }

          unreadResponse++;

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw OException.wrapException(new OInterruptedException("Thread interrupted while waiting for request"), e);
        } finally {
          if (readLock)
            releaseReadLock();
        }
      } while (true);

      if (debug)
        OLogManager.instance().debug(this, "%s - Session %d handle response", socket.getLocalAddress(), iRequesterId);
      byte[] tokenBytes;
      if (token)
        tokenBytes = this.readBytes();
      else
        tokenBytes = null;
      handleStatus(currentStatus, currentSessionId);
      return tokenBytes;
    } catch (OLockException e) {
      Thread.currentThread().interrupt();
      // NEVER HAPPENS?
      OLogManager.instance().error(this, "Unexpected error on reading response from channel", e);
    }
    return null;
  }

  public void endResponse() throws IOException {
    channelRead = false;

    // WAKE UP ALL THE WAITING THREADS
    try {
      readCondition.signalAll();
    } catch (IllegalMonitorStateException e) {
      // IGNORE IT
      OLogManager.instance().debug(this, "Error on signaling waiting clients after reading response");
    }
    try {
      releaseReadLock();
    } catch (IllegalMonitorStateException e) {
      // IGNORE IT
      OLogManager.instance().debug(this, "Error on unlocking network channel after reading response");
    }

  }

  public void endRequest() throws IOException {
    flush();
    releaseWriteLock();
  }

  @Override
  public void close() {
    if (getLockRead().tryAcquireLock())
      try {
        readCondition.signalAll();
      } finally {
        releaseReadLock();
      }

    try {
      super.close();
    } catch (Exception e) {
      // IGNORE IT
    }

    if (serviceThread != null) {
      final OAsynchChannelServiceThread s = serviceThread;
      serviceThread = null;
      if (s != null)
        // CHECK S BECAUSE IT COULD BE CONCURRENTLY RESET
        s.sendShutdown();
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
    return s != null && !s.isClosed() && s.isConnected() && !s.isInputShutdown() && !s.isOutputShutdown();
  }

  /**
   * Gets the major supported protocol version
   */
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

  public OAsynchChannelServiceThread getServiceThread() {
    return serviceThread;
  }

  public int handleStatus(final byte iResult, final int iClientTxId) throws IOException {
    if (iResult == OChannelBinaryProtocol.RESPONSE_STATUS_OK || iResult == OChannelBinaryProtocol.PUSH_DATA) {
      return iClientTxId;
    } else if (iResult == OChannelBinaryProtocol.RESPONSE_STATUS_ERROR) {

      OErrorResponse response = new OErrorResponse();
      response.read(this, null);
      byte[] serializedException = response.getResult();
      Exception previous = null;
      if (serializedException != null && serializedException.length > 0)
        throwSerializedException(serializedException);

      for (Map.Entry<String, String> entry : response.getMessages().entrySet()) {
        previous = createException(entry.getKey(), entry.getValue(), previous);
      }

      if (previous != null) {
        throw new RuntimeException(previous);
      } else
        throw new ONetworkProtocolException("Network response error");

    } else {
      // PROTOCOL ERROR
      // close();
      throw new ONetworkProtocolException("Error on reading response from the server");
    }
  }

  private void setReadResponseTimeout() throws SocketException {
    final Socket s = socket;
    if (s != null && s.isConnected() && !s.isClosed())
      s.setSoTimeout(getSocketTimeout());
  }

  public void setWaitResponseTimeout() throws SocketException {
    final Socket s = socket;
    if (s != null)
      s.setSoTimeout(OGlobalConfiguration.NETWORK_REQUEST_TIMEOUT.getValueAsInteger());
  }

  private void throwSerializedException(final byte[] serializedException) throws IOException {
    final OMemoryInputStream inputStream = new OMemoryInputStream(serializedException);
    final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);

    Object throwable = null;
    try {
      throwable = objectInputStream.readObject();
    } catch (ClassNotFoundException e) {
      OLogManager.instance().error(this, "Error during exception deserialization", e);
      throw new IOException("Error during exception deserialization: " + e.toString());
    }

    objectInputStream.close();

    if (throwable instanceof OException) {
      try {
        final Class<? extends OException> cls = (Class<? extends OException>) throwable.getClass();
        final Constructor<? extends OException> constructor;
        constructor = cls.getConstructor(cls);
        final OException proxyInstance = constructor.newInstance(throwable);
        proxyInstance.addSuppressed((Exception) throwable);
        throw proxyInstance;

      } catch (NoSuchMethodException e) {
        OLogManager.instance().error(this, "Error during exception deserialization", e);
      } catch (InvocationTargetException e) {
        OLogManager.instance().error(this, "Error during exception deserialization", e);
      } catch (InstantiationException e) {
        OLogManager.instance().error(this, "Error during exception deserialization", e);
      } catch (IllegalAccessException e) {
        OLogManager.instance().error(this, "Error during exception deserialization", e);
      }
    }

    if (throwable instanceof Throwable) {
      throw new OResponseProcessingException("Exception during response processing", (Throwable) throwable);
    }
    // WRAP IT
    else
      OLogManager.instance().error(this,
          "Error during exception serialization, serialized exception is not Throwable, exception type is " + (throwable != null ?
              throwable.getClass().getName() :
              "null"));
  }

  public void beginRequest(final byte iCommand, final OStorageRemoteSession session) throws IOException {
    final OStorageRemoteNodeSession nodeSession = session.getServerSession(getServerURL());
    beginRequest(iCommand, nodeSession);
  }

  public void beginRequest(byte iCommand, OStorageRemoteNodeSession nodeSession) throws IOException {
    if (nodeSession == null)
      throw new OIOException("Invalid session for URL '" + getServerURL() + "'");

    writeByte(iCommand);
    writeInt(nodeSession.getSessionId());
    if (nodeSession.getToken() != null) {
      // if (!session.hasConnection(this) || true) {
      writeBytes(nodeSession.getToken());
      // session.addConnection(this);
      // } else
      // writeBytes(new byte[] {});
    }
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

  public void setSocketTimeout(int socketTimeout) {
    this.socketTimeout = socketTimeout;
  }

}
