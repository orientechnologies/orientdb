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
package com.orientechnologies.orient.enterprise.channel.binary;

import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;

/**
 * Abstract representation of a channel.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OChannelBinary extends OChannel {
  private static final OLogger logger = OLogManager.instance().logger(OChannelBinary.class);
  private static final int MAX_LENGTH_DEBUG = 150;
  protected final boolean debug;
  protected final int maxChunkSize;
  protected DataInputStream in;
  protected DataOutputStream out;
  protected OChannelDataInput inChannel;
  protected OChannelDataOutput outChannel;
  ;
  private int responseTimeout;
  private int networkTimeout;

  public OChannelBinary(final Socket socket, final OContextConfiguration config)
      throws IOException {
    super(socket, config);
    this.socket.setKeepAlive(true);
    maxChunkSize =
        config.getValueAsInteger(OGlobalConfiguration.NETWORK_BINARY_MAX_CONTENT_LENGTH) * 1024;
    debug = config.getValueAsBoolean(OGlobalConfiguration.NETWORK_BINARY_DEBUG);
    responseTimeout = config.getValueAsInteger(OGlobalConfiguration.NETWORK_REQUEST_TIMEOUT);
    networkTimeout = config.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT);

    if (debug) logger.info("%s - Connected", this.socket.getRemoteSocketAddress());
  }

  protected void initDebug() {
    if (debug) {
      inChannel = new OChannelDataInputDebug(inChannel, socket.getRemoteSocketAddress());
      outChannel = new OChannelDataOutputDebug(outChannel, socket.getRemoteSocketAddress());
    }
  }

  public void clearInput() throws IOException {
    if (in == null) return;

    final StringBuilder dirtyBuffer = new StringBuilder(MAX_LENGTH_DEBUG);
    int i = 0;
    while (in.available() > 0) {
      char c = (char) in.read();
      ++i;

      if (dirtyBuffer.length() < MAX_LENGTH_DEBUG) dirtyBuffer.append(c);
    }
    updateMetricReceivedBytes(i);

    final String message =
        "Received unread response from "
            + socket.getRemoteSocketAddress()
            + " probably corrupted data from the network connection. Cleared dirty data in the"
            + " buffer ("
            + i
            + " bytes): ["
            + dirtyBuffer
            + (i > dirtyBuffer.length() ? "..." : "")
            + "]";
    logger.error("%s", null, message);
    throw new OIOException(message);
  }

  @Override
  public void flush() throws IOException {

    updateMetricFlushes(1);

    if (out != null)
      // IT ALREADY CALL THE UNDERLYING FLUSH
      out.flush();
    else super.flush();
  }

  @Override
  public void close() {
    try {
      if (in != null) {
        in.close();
      }
    } catch (IOException e) {
      logger.debug("Error during closing of input stream", e);
    }

    try {
      if (out != null) {
        out.close();
      }
    } catch (IOException e) {
      logger.debug("Error during closing of output stream", e);
    }

    super.close();
  }

  public DataOutputStream getDataOutput() {
    return out;
  }

  public DataInputStream getDataInput() {
    return in;
  }

  public void setWaitResponseTimeout() throws SocketException {
    final Socket s = socket;
    if (s != null) s.setSoTimeout(responseTimeout);
  }

  public void setWaitRequestTimeout() throws SocketException {
    final Socket s = socket;
    if (s != null) s.setSoTimeout(0);
  }

  public void setReadRequestTimeout() throws SocketException {
    final Socket s = socket;
    if (s != null) s.setSoTimeout(networkTimeout);
  }

  public OChannelDataInput getChannelDataInput() {
    return inChannel;
  }

  public OChannelDataOutput getChannelDataOutput() {
    return outChannel;
  }

  public interface WrapStreams {
    public record Wrapped(InputStream input, OutputStream output) {}

    Wrapped wrap(InputStream input, OutputStream output) throws IOException;
  }

  public abstract void wrapStreams(WrapStreams stre) throws IOException;
}
