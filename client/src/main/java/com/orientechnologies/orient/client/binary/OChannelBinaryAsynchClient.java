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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.enterprise.channel.OSocketFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInputBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutputBinary;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

public class OChannelBinaryAsynchClient extends OChannelBinary {
  public static final OLogger logger =
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

  public byte waitResponse() throws IOException, SocketException {
    try {
      // WAIT FOR THE RESPONSE
      acquireReadLock();

      if (!isConnected()) {
        releaseReadLock();
        throw new IOException("Channel is closed");
      }
      OChannelDataInput input = getChannelDataInput();
      try {
        setWaitResponseTimeout();
        return input.readByte();
      } finally {
        setReadResponseTimeout();
      }

    } catch (OLockException e) {
      Thread.currentThread().interrupt();
      // NEVER HAPPENS?
      logger.error("Unexpected error on reading response from channel", e);
      throw e;
    }
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

  private void setReadResponseTimeout() throws SocketException {
    final Socket s = socket;
    if (s != null && s.isConnected() && !s.isClosed()) s.setSoTimeout(getSocketTimeout());
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
