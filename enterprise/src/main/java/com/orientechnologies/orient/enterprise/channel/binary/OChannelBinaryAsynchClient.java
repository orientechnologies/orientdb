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
package com.orientechnologies.orient.enterprise.channel.binary;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.enterprise.channel.OSocketFactory;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

public class OChannelBinaryAsynchClient extends OChannelBinary {
  protected final int                          socketTimeout;                                               // IN MS
  protected final short                        srvProtocolVersion;
  private final Condition                      readCondition = getLockRead().getUnderlying().newCondition();
  private final int                            maxUnreadResponses;
  private String                               serverURL;
  private volatile boolean                     channelRead   = false;
  private byte                                 currentStatus;
  private int                                  currentSessionId;
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

      maxUnreadResponses = OGlobalConfiguration.NETWORK_BINARY_READ_RESPONSE_MAX_TIMES.getValueAsInteger();
      serverURL = remoteHost + ":" + remotePort;
      if (iDatabaseName != null)
        serverURL += "/" + iDatabaseName;
      socketTimeout = iConfig.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT);

      socket.setPerformancePreferences(0, 2, 1);

      socket.setKeepAlive(true);
      socket.setSendBufferSize(socketBufferSize);
      socket.setReceiveBufferSize(socketBufferSize);
      try {
        socket.connect(new InetSocketAddress(remoteHost, remotePort), socketTimeout);
        setReadResponseTimeout();
        connected();
      } catch (java.net.SocketTimeoutException e) {
        throw new IOException("Cannot connect to host " + remoteHost + ":" + remotePort, e);
      }
      try {
        inStream = new BufferedInputStream(socket.getInputStream(), socketBufferSize);
        outStream = new BufferedOutputStream(socket.getOutputStream(), socketBufferSize);

        in = new DataInputStream(inStream);
        out = new DataOutputStream(outStream);

        srvProtocolVersion = readShort();
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

      if (asynchEventListener != null)
        serviceThread = new OAsynchChannelServiceThread(asynchEventListener, this);
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
      // UNABLE TO REPRODUCE THE SAME SERVER-SIZE EXCEPTION: THROW A STORAGE EXCEPTION
      rootException = new OIOException(iMessage, iPrevious);
    }

    if (c != null)
      try {
        final Throwable e;
        if (c.getParameterTypes().length > 1)
          e = (Throwable) c.newInstance(iMessage, iPrevious);
        else
          e = (Throwable) c.newInstance(iMessage);

        if (e instanceof RuntimeException)
          rootException = (RuntimeException) e;
        else
          rootException = new OException(e);
      } catch (InstantiationException ignored) {
      } catch (IllegalAccessException ignored) {
      } catch (InvocationTargetException ignored) {
      }

    return rootException;
  }

  public void beginRequest() {
    acquireWriteLock();
  }

  public void endRequest() throws IOException {
    flush();
    releaseWriteLock();
  }

  public byte[] beginResponse(final int iRequesterId, boolean token) throws IOException {
    return beginResponse(iRequesterId, timeout, token);
  }

  public byte[] beginResponse(final int iRequesterId, final long iTimeout, boolean token) throws IOException {
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
          readLock = false;

          throw new IOException("Channel is closed");
        }

        if (!channelRead) {
          channelRead = true;

          try {
            setWaitResponseTimeout();
            currentStatus = readByte();
            currentSessionId = readInt();

            if (debug)
              OLogManager.instance().debug(this, "%s - Read response: %d-%d", socket.getLocalAddress(), (int) currentStatus,
                  currentSessionId);

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
            OLogManager.instance().debug(this, "%s - Session %d skip response, it is for %d", socket.getLocalAddress(),
                iRequesterId, currentSessionId);

          if (iTimeout > 0 && (System.currentTimeMillis() - startClock) > iTimeout) {
            // CLOSE THE SOCKET TO CHANNEL TO AVOID FURTHER DIRTY DATA
            close();
            readLock = false;

            throw new OTimeoutException("Timeout on reading response from the server "
                + (socket != null ? socket.getRemoteSocketAddress() : "") + " for the request " + iRequesterId);
          }

          // IN CASE OF TOO MUCH TIME FOR READ A MESSAGE, ASYNC THREAD SHOULD NOT BE INCLUDE IN THIS CHECK
          if (unreadResponse > maxUnreadResponses && iRequesterId != Integer.MIN_VALUE) {
            if (debug)
              OLogManager.instance().info(this, "Unread responses %d > %d, consider the buffer as dirty: clean it", unreadResponse,
                  maxUnreadResponses);

            close();
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
            OLogManager.instance().debug(this, "Waked up: slept %dms, checking again from %s for session %d", (now - start),
                socket.getLocalAddress(), iRequesterId);
          }

          unreadResponse++;

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();

        } finally {
          if (readLock)
            releaseReadLock();
        }
      } while (true);

      if (debug)
        OLogManager.instance().debug(this, "%s - Session %d handle response", socket.getLocalAddress(), iRequesterId);

      byte[] renew = null;
      if (token)
        renew = this.readBytes();
      handleStatus(currentStatus, currentSessionId);
      return renew;
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
   * 
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

  protected int handleStatus(final byte iResult, final int iClientTxId) throws IOException {
    if (iResult == OChannelBinaryProtocol.RESPONSE_STATUS_OK || iResult == OChannelBinaryProtocol.PUSH_DATA) {
      return iClientTxId;
    } else if (iResult == OChannelBinaryProtocol.RESPONSE_STATUS_ERROR) {

      final List<OPair<String, String>> exceptions = new ArrayList<OPair<String, String>>();

      // EXCEPTION
      while (readByte() == 1) {
        final String excClassName = readString();
        final String excMessage = readString();
        exceptions.add(new OPair<String, String>(excClassName, excMessage));
      }

      byte[] serializedException = null;
      if (srvProtocolVersion >= 19)
        serializedException = readBytes();

      Exception previous = null;

      if (serializedException != null && serializedException.length > 0)
        throwSerializedException(serializedException);

      for (int i = exceptions.size() - 1; i > -1; --i) {
        previous = createException(exceptions.get(i).getKey(), exceptions.get(i).getValue(), previous);
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
      s.setSoTimeout(socketTimeout);
  }

  private void setWaitResponseTimeout() throws SocketException {
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

    if (throwable instanceof OException)
      throw (OException) throwable;
    else if (throwable instanceof Throwable)
      // WRAP IT
      throw new OResponseProcessingException("Exception during response processing.", (Throwable) throwable);
    else
      OLogManager.instance().error(this,
          "Error during exception serialization, serialized exception is not Throwable, exception type is "
              + (throwable != null ? throwable.getClass().getName() : "null"));
  }

}
