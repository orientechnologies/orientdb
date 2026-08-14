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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.enterprise.channel.OSocketFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary.WrapStreams;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

/** Synchronous implementation of binary channel. */
public class OChannelBinarySynchClient extends OChannelBinary {
  private static final OLogger logger =
      OLogManager.instance().logger(OChannelBinarySynchClient.class);
  protected final int socketTimeout; // IN MS
  protected final short srvProtocolVersion;
  protected String serverURL;

  public OChannelBinarySynchClient(
      OSocketFactory factory,
      final String remoteHost,
      final int remotePort,
      final String iDatabaseName,
      final OContextConfiguration iConfig,
      final int protocolVersion)
      throws IOException {
    super(factory.createSocket(), iConfig);
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
        initChannels(WrapStreams.Wrapped::new);
        srvProtocolVersion = getChannelDataInput().readShort();
      } catch (IOException e) {
        throw OException.wrapException(
            new ONetworkProtocolException(
                "Cannot read protocol version from remote server "
                    + socket.getRemoteSocketAddress()
                    + ": "
                    + e.getMessage()),
            e);
      }

      if (srvProtocolVersion != protocolVersion) {
        logger.warn(
            "The Client driver version is different than Server version: client=%d, server=%d. You"
                + " could not use the full features of the newer version. Assure to have the same"
                + " versions on both",
            protocolVersion, srvProtocolVersion);
      }

    } catch (RuntimeException e) {
      if (socket.isConnected()) socket.close();
      throw e;
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

  protected void setReadResponseTimeout() throws SocketException {
    final Socket s = socket;
    if (s != null && s.isConnected() && !s.isClosed()) s.setSoTimeout(socketTimeout);
  }
}
