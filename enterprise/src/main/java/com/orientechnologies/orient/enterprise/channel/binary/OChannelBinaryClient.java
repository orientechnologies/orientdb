/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.enterprise.channel.binary;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import com.orientechnologies.common.concur.resource.OAdaptiveLock;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

public class OChannelBinaryClient extends OChannelBinaryAsynch {
  final protected int                 socketTimeout;     // IN MS
  final private short                 srvProtocolVersion;
  private OAsynchChannelServiceThread serviceThread;

  public OChannelBinaryClient(final String remoteHost, final int remotePort, final OContextConfiguration iConfig,
      final int iProtocolVersion) throws IOException {
    this(remoteHost, remotePort, iConfig, iProtocolVersion, null);
  }

  public OChannelBinaryClient(final String remoteHost, final int remotePort, final OContextConfiguration iConfig,
      final int iProtocolVersion, final ORemoteServerEventListener asynchEventListener) throws IOException {
    super(new Socket(), iConfig);
    socketTimeout = iConfig.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT);

    socket.setPerformancePreferences(0, 2, 1);

    socket.setKeepAlive(true);
    socket.setSendBufferSize(socketBufferSize);
    socket.setReceiveBufferSize(socketBufferSize);
    try {
      socket.connect(new InetSocketAddress(remoteHost, remotePort), socketTimeout);
      connected();
    } catch (java.net.SocketTimeoutException e) {
      throw new IOException("Cannot connect to host " + remoteHost + ":" + remotePort, e);
    }

    inStream = new BufferedInputStream(socket.getInputStream(), socketBufferSize);
    outStream = new BufferedOutputStream(socket.getOutputStream(), socketBufferSize);

    in = new DataInputStream(inStream);
    out = new DataOutputStream(outStream);

    try {
      srvProtocolVersion = readShort();
      // if (srvProtocolVersion >= 10) {
      // // READ ALL THE SUPPORTED VERSIONS
      // short next;
      // while ((next = readShort()) > -1)
      // srvProtocolVersions.add(next);
      // }
    } catch (IOException e) {
      throw new ONetworkProtocolException("Cannot read protocol version from remote server " + socket.getRemoteSocketAddress()
          + ": " + e);
    }

    if (Math.abs(srvProtocolVersion - iProtocolVersion) > 2) {
      close();
      throw new ONetworkProtocolException("Binary protocol is incompatible with the Server connected: client=" + iProtocolVersion
          + ", server=" + srvProtocolVersion);
    }

    if (asynchEventListener != null)
      serviceThread = new OAsynchChannelServiceThread(asynchEventListener, this);
  }

  @Override
  public void close() {
    super.close();
    if (serviceThread != null) {
      final OAsynchChannelServiceThread s = serviceThread;
      serviceThread = null;
      s.sendShutdown();
    }
  }

  /**
   * Tells if the channel is connected.
   * 
   * @return true if it's connected, otherwise false.
   */
  public boolean isConnected() {
    if (socket != null && socket.isConnected() && !socket.isInputShutdown() && !socket.isOutputShutdown())
      return true;
    return false;
  }

  /**
   * Gets the major supported protocol version
   * 
   */
  public short getSrvProtocolVersion() {
    return srvProtocolVersion;
  }

  public OAdaptiveLock getLockRead() {
    return lockRead;
  }

  public OAdaptiveLock getLockWrite() {
    return lockWrite;
  }
}
