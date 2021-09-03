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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.enterprise.channel.OSocketFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
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
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

/** Abstract implementation of binary channel. */
public abstract class OChannelBinaryClientAbstract extends OChannelBinary {
  protected final int socketTimeout; // IN MS
  protected final short srvProtocolVersion;
  protected String serverURL;
  protected byte currentStatus;
  protected int currentSessionId;

  public OChannelBinaryClientAbstract(
      final String remoteHost,
      final int remotePort,
      final String iDatabaseName,
      final OContextConfiguration iConfig,
      final int protocolVersion)
      throws IOException {
    super(OSocketFactory.instance(iConfig).createSocket(), iConfig);
    try {

      serverURL = remoteHost + ":" + remotePort;
      if (iDatabaseName != null) serverURL += "/" + iDatabaseName;
      socketTimeout = iConfig.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT);

      try {
        if (remoteHost.contains(":")) {
          // IPV6
          final InetAddress[] addresses = Inet6Address.getAllByName(remoteHost);
          socket.connect(new InetSocketAddress(addresses[0], remotePort), socketTimeout);

        } else {
          // IPV4
          socket.connect(new InetSocketAddress(remoteHost, remotePort), socketTimeout);
        }
        setReadResponseTimeout();
        connected();
      } catch (java.net.SocketTimeoutException e) {
        throw new IOException(
            "Cannot connect to host "
                + remoteHost
                + ":"
                + remotePort
                + " (timeout="
                + socketTimeout
                + ")",
            e);
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
      } catch (IOException e) {
        throw new ONetworkProtocolException(
            "Cannot read protocol version from remote server "
                + socket.getRemoteSocketAddress()
                + ": "
                + e);
      }

      if (srvProtocolVersion != protocolVersion) {
        OLogManager.instance()
            .warn(
                this,
                "The Client driver version is different than Server version: client="
                    + protocolVersion
                    + ", server="
                    + srvProtocolVersion
                    + ". You could not use the full features of the newer version. Assure to have the same versions on both");
      }

    } catch (RuntimeException e) {
      if (socket.isConnected()) socket.close();
      throw e;
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

  protected int handleStatus(final byte iResult, final int iClientTxId) throws IOException {
    if (iResult == OChannelBinaryProtocol.RESPONSE_STATUS_OK
        || iResult == OChannelBinaryProtocol.PUSH_DATA) {
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
      if (srvProtocolVersion >= 19) serializedException = readBytes();

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

  protected void setReadResponseTimeout() throws SocketException {
    final Socket s = socket;
    if (s != null && s.isConnected() && !s.isClosed()) s.setSoTimeout(socketTimeout);
  }

  protected void throwSerializedException(final byte[] serializedException) throws IOException {
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(serializedException);
    final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);

    Object throwable = null;
    try {
      throwable = objectInputStream.readObject();
    } catch (ClassNotFoundException e) {
      OLogManager.instance().error(this, "Error during exception deserialization", e);
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
      throw new OResponseProcessingException(
          "Exception during response processing", (Throwable) throwable);
    } else {
      // WRAP IT
      String exceptionType = throwable != null ? throwable.getClass().getName() : "null";
      OLogManager.instance()
          .error(
              this,
              "Error during exception serialization, serialized exception is not Throwable, exception type is "
                  + exceptionType,
              null);
    }
  }
}
