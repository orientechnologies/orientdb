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

import com.orientechnologies.common.exception.OInvalidBinaryChunkException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;

/**
 * Abstract representation of a channel.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OChannelBinary extends OChannel
    implements OChannelDataInput, OChannelDataOutput {
  private static final int MAX_LENGTH_DEBUG = 150;
  protected final boolean debug;
  private final int maxChunkSize;
  public DataInputStream in;
  public DataOutputStream out;
  private int responseTimeout;
  private int networkTimeout;

  public OChannelBinary(final Socket iSocket, final OContextConfiguration iConfig)
      throws IOException {
    super(iSocket, iConfig);
    socket.setKeepAlive(true);
    maxChunkSize =
        iConfig.getValueAsInteger(OGlobalConfiguration.NETWORK_BINARY_MAX_CONTENT_LENGTH) * 1024;
    debug = iConfig.getValueAsBoolean(OGlobalConfiguration.NETWORK_BINARY_DEBUG);
    responseTimeout = iConfig.getValueAsInteger(OGlobalConfiguration.NETWORK_REQUEST_TIMEOUT);
    networkTimeout = iConfig.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT);

    if (debug) OLogManager.instance().info(this, "%s - Connected", socket.getRemoteSocketAddress());
  }

  public byte readByte() throws IOException {
    updateMetricReceivedBytes(OBinaryProtocol.SIZE_BYTE);

    if (debug) {
      OLogManager.instance()
          .info(this, "%s - Reading byte (1 byte)...", socket.getRemoteSocketAddress());
      final byte value = in.readByte();
      OLogManager.instance()
          .info(this, "%s - Read byte: %d", socket.getRemoteSocketAddress(), (int) value);
      return value;
    }

    return in.readByte();
  }

  public boolean readBoolean() throws IOException {
    updateMetricReceivedBytes(OBinaryProtocol.SIZE_BYTE);

    if (debug) {
      OLogManager.instance()
          .info(this, "%s - Reading boolean (1 byte)...", socket.getRemoteSocketAddress());
      final boolean value = in.readBoolean();
      OLogManager.instance()
          .info(this, "%s - Read boolean: %b", socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readBoolean();
  }

  public int readInt() throws IOException {
    updateMetricReceivedBytes(OBinaryProtocol.SIZE_INT);

    if (debug) {
      OLogManager.instance()
          .info(this, "%s - Reading int (4 bytes)...", socket.getRemoteSocketAddress());
      final int value = in.readInt();
      OLogManager.instance()
          .info(this, "%s - Read int: %d", socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readInt();
  }

  public long readLong() throws IOException {
    updateMetricReceivedBytes(OBinaryProtocol.SIZE_LONG);

    if (debug) {
      OLogManager.instance()
          .info(this, "%s - Reading long (8 bytes)...", socket.getRemoteSocketAddress());
      final long value = in.readLong();
      OLogManager.instance()
          .info(this, "%s - Read long: %d", socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readLong();
  }

  public short readShort() throws IOException {
    updateMetricReceivedBytes(OBinaryProtocol.SIZE_SHORT);

    if (debug) {
      OLogManager.instance()
          .info(this, "%s - Reading short (2 bytes)...", socket.getRemoteSocketAddress());
      final short value = in.readShort();
      OLogManager.instance()
          .info(this, "%s - Read short: %d", socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readShort();
  }

  public String readString() throws IOException {
    if (debug) {
      OLogManager.instance()
          .info(this, "%s - Reading string (4+N bytes)...", socket.getRemoteSocketAddress());
      final int len = in.readInt();
      if (len > maxChunkSize) {
        throw new IOException(
            "Impossible to read a string chunk of length:"
                + len
                + " max allowed chunk length:"
                + maxChunkSize
                + " see NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");
      }
      if (debug)
        OLogManager.instance()
            .info(this, "%s - Read string chunk length: %d", socket.getRemoteSocketAddress(), len);
      if (len < 0) return null;

      // REUSE STATIC BUFFER?
      final byte[] tmp = new byte[len];
      in.readFully(tmp);

      updateMetricReceivedBytes(OBinaryProtocol.SIZE_INT + len);

      final String value = new String(tmp, "UTF-8");
      OLogManager.instance()
          .info(this, "%s - Read string: %s", socket.getRemoteSocketAddress(), value);
      return value;
    }

    final int len = in.readInt();
    if (len < 0) return null;

    final byte[] tmp = new byte[len];
    in.readFully(tmp);

    updateMetricReceivedBytes(OBinaryProtocol.SIZE_INT + len);

    return new String(tmp, "UTF-8");
  }

  public byte[] readBytes() throws IOException {
    if (debug)
      OLogManager.instance()
          .info(
              this,
              "%s - Reading chunk of bytes. Reading chunk length as int (4 bytes)...",
              socket.getRemoteSocketAddress());

    final int len = in.readInt();
    if (len > maxChunkSize) {
      throw new IOException(
          "Impossible to read a chunk of length:"
              + len
              + " max allowed chunk length:"
              + maxChunkSize
              + " see NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");
    }
    updateMetricReceivedBytes(OBinaryProtocol.SIZE_INT + len);

    if (debug)
      OLogManager.instance()
          .info(this, "%s - Read chunk length: %d", socket.getRemoteSocketAddress(), len);

    if (len < 0) return null;

    if (debug)
      OLogManager.instance()
          .info(this, "%s - Reading %d bytes...", socket.getRemoteSocketAddress(), len);

    // REUSE STATIC BUFFER?
    final byte[] tmp = new byte[len];
    in.readFully(tmp);

    if (debug)
      OLogManager.instance()
          .info(
              this,
              "%s - Read %d bytes: %s",
              socket.getRemoteSocketAddress(),
              len,
              new String(tmp));

    return tmp;
  }

  public ORecordId readRID() throws IOException {
    final int clusterId = readShort();
    final long clusterPosition = readLong();
    return new ORecordId(clusterId, clusterPosition);
  }

  public int readVersion() throws IOException {
    return readInt();
  }

  public OChannelBinary writeByte(final byte iContent) throws IOException {
    if (debug)
      OLogManager.instance()
          .info(this, "%s - Writing byte (1 byte): %d", socket.getRemoteSocketAddress(), iContent);

    out.write(iContent);
    updateMetricTransmittedBytes(OBinaryProtocol.SIZE_BYTE);
    return this;
  }

  public OChannelBinary writeBoolean(final boolean iContent) throws IOException {
    if (debug)
      OLogManager.instance()
          .info(
              this, "%s - Writing boolean (1 byte): %b", socket.getRemoteSocketAddress(), iContent);

    out.writeBoolean(iContent);
    updateMetricTransmittedBytes(OBinaryProtocol.SIZE_BYTE);
    return this;
  }

  public OChannelBinary writeInt(final int iContent) throws IOException {
    if (debug)
      OLogManager.instance()
          .info(this, "%s - Writing int (4 bytes): %d", socket.getRemoteSocketAddress(), iContent);

    out.writeInt(iContent);
    updateMetricTransmittedBytes(OBinaryProtocol.SIZE_INT);
    return this;
  }

  public OChannelBinary writeLong(final long iContent) throws IOException {
    if (debug)
      OLogManager.instance()
          .info(this, "%s - Writing long (8 bytes): %d", socket.getRemoteSocketAddress(), iContent);

    out.writeLong(iContent);
    updateMetricTransmittedBytes(OBinaryProtocol.SIZE_LONG);
    return this;
  }

  public OChannelBinary writeShort(final short iContent) throws IOException {
    if (debug)
      OLogManager.instance()
          .info(
              this, "%s - Writing short (2 bytes): %d", socket.getRemoteSocketAddress(), iContent);

    out.writeShort(iContent);
    updateMetricTransmittedBytes(OBinaryProtocol.SIZE_SHORT);
    return this;
  }

  public OChannelBinary writeString(final String iContent) throws IOException {
    if (debug)
      OLogManager.instance()
          .info(
              this,
              "%s - Writing string (4+%d=%d bytes): %s",
              socket.getRemoteSocketAddress(),
              iContent != null ? iContent.length() : 0,
              iContent != null ? iContent.length() + 4 : 4,
              iContent);

    if (iContent == null) {
      out.writeInt(-1);
      updateMetricTransmittedBytes(OBinaryProtocol.SIZE_INT);
    } else {
      final byte[] buffer = iContent.getBytes("UTF-8");
      if (buffer.length > maxChunkSize) {
        throw new OInvalidBinaryChunkException(
            "Impossible to write a chunk of length:"
                + buffer.length
                + " max allowed chunk length:"
                + maxChunkSize
                + " see NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");
      }

      out.writeInt(buffer.length);
      out.write(buffer, 0, buffer.length);
      updateMetricTransmittedBytes(OBinaryProtocol.SIZE_INT + buffer.length);
    }

    return this;
  }

  public OChannelBinary writeBytes(final byte[] iContent) throws IOException {
    return writeBytes(iContent, iContent != null ? iContent.length : 0);
  }

  public OChannelBinary writeBytes(final byte[] iContent, final int iLength) throws IOException {
    if (debug)
      OLogManager.instance()
          .info(
              this,
              "%s - Writing bytes (4+%d=%d bytes): %s",
              socket.getRemoteSocketAddress(),
              iLength,
              iLength + 4,
              Arrays.toString(iContent));

    if (iContent == null) {
      out.writeInt(-1);
      updateMetricTransmittedBytes(OBinaryProtocol.SIZE_INT);
    } else {
      if (iLength > maxChunkSize) {
        throw new OInvalidBinaryChunkException(
            "Impossible to write a chunk of length:"
                + iLength
                + " max allowed chunk length:"
                + maxChunkSize
                + " see NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");
      }

      out.writeInt(iLength);
      out.write(iContent, 0, iLength);
      updateMetricTransmittedBytes(OBinaryProtocol.SIZE_INT + iLength);
    }
    return this;
  }

  public void writeRID(final ORID iRID) throws IOException {
    writeShort((short) iRID.getClusterId());
    writeLong(iRID.getClusterPosition());
  }

  public void writeVersion(final int version) throws IOException {
    writeInt(version);
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
            + " probably corrupted data from the network connection. Cleared dirty data in the buffer ("
            + i
            + " bytes): ["
            + dirtyBuffer
            + (i > dirtyBuffer.length() ? "..." : "")
            + "]";
    OLogManager.instance().error(this, message, null);
    throw new OIOException(message);
  }

  @Override
  public void flush() throws IOException {
    if (debug)
      OLogManager.instance()
          .info(
              this,
              "%s - Flush",
              socket != null ? " null possible previous close" : socket.getRemoteSocketAddress());

    updateMetricFlushes();

    if (out != null)
      // IT ALREADY CALL THE UNDERLYING FLUSH
      out.flush();
    else super.flush();
  }

  @Override
  public void close() {
    if (debug)
      OLogManager.instance()
          .info(
              this,
              "%s - Closing socket...",
              socket != null ? " null possible previous close" : socket.getRemoteSocketAddress());

    try {
      if (in != null) {
        in.close();
      }
    } catch (IOException e) {
      OLogManager.instance().debug(this, "Error during closing of input stream", e);
    }

    try {
      if (out != null) {
        out.close();
      }
    } catch (IOException e) {
      OLogManager.instance().debug(this, "Error during closing of output stream", e);
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
}
