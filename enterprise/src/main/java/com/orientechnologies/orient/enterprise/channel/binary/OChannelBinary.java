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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.version.ODistributedVersion;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.enterprise.channel.OChannel;

public abstract class OChannelBinary extends OChannel {
  private static final int MAX_LENGTH_DEBUG = 150;

  public DataInputStream   in;
  public DataOutputStream  out;
  private final int        maxChunkSize;
  protected final boolean  debug;
  private final byte[]     buffer;

  public OChannelBinary(final Socket iSocket, final OContextConfiguration iConfig) throws IOException {
    super(iSocket, iConfig);

    maxChunkSize = iConfig.getValueAsInteger(OGlobalConfiguration.NETWORK_BINARY_MAX_CONTENT_LENGTH);
    debug = iConfig.getValueAsBoolean(OGlobalConfiguration.NETWORK_BINARY_DEBUG);
    buffer = new byte[maxChunkSize];

    if (debug)
      OLogManager.instance().info(this, "%s - Connected", socket.getRemoteSocketAddress());
  }

  public byte readByte() throws IOException {
    updateMetricReceivedBytes(OBinaryProtocol.SIZE_BYTE);

    if (debug) {
      OLogManager.instance().info(this, "%s - Reading byte (1 byte)...", socket.getRemoteSocketAddress());
      final byte value = in.readByte();
      OLogManager.instance().info(this, "%s - Read byte: %d", socket.getRemoteSocketAddress(), (int) value);
      return value;
    }

    return in.readByte();
  }

  public int readInt() throws IOException {
    updateMetricReceivedBytes(OBinaryProtocol.SIZE_INT);

    if (debug) {
      OLogManager.instance().info(this, "%s - Reading int (4 bytes)...", socket.getRemoteSocketAddress());
      final int value = in.readInt();
      OLogManager.instance().info(this, "%s - Read int: %d", socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readInt();
  }

  public long readLong() throws IOException {
    updateMetricReceivedBytes(OBinaryProtocol.SIZE_LONG);

    if (debug) {
      OLogManager.instance().info(this, "%s - Reading long (8 bytes)...", socket.getRemoteSocketAddress());
      final long value = in.readLong();
      OLogManager.instance().info(this, "%s - Read long: %d", socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readLong();
  }

  public short readShort() throws IOException {
    updateMetricReceivedBytes(OBinaryProtocol.SIZE_SHORT);

    if (debug) {
      OLogManager.instance().info(this, "%s - Reading short (2 bytes)...", socket.getRemoteSocketAddress());
      final short value = in.readShort();
      OLogManager.instance().info(this, "%s - Read short: %d", socket.getRemoteSocketAddress(), value);
      return value;
    }

    return in.readShort();
  }

  public String readString() throws IOException {
    if (debug) {
      OLogManager.instance().info(this, "%s - Reading string (4+N bytes)...", socket.getRemoteSocketAddress());
      final int len = in.readInt();
      if (len < 0)
        return null;

      // REUSE STATIC BUFFER?
      final byte[] tmp = new byte[len];
      in.readFully(tmp);

      updateMetricReceivedBytes(OBinaryProtocol.SIZE_INT + len);

      final String value = new String(tmp);
      OLogManager.instance().info(this, "%s - Read string: %s", socket.getRemoteSocketAddress(), value);
      return value;
    }

    final int len = in.readInt();
    if (len < 0)
      return null;

    final byte[] tmp = new byte[len];
    in.readFully(tmp);

    updateMetricReceivedBytes(OBinaryProtocol.SIZE_INT + len);

    return new String(tmp);
  }

  public byte[] readBytes() throws IOException {
    if (debug)
      OLogManager.instance().info(this, "%s - Reading chunk of bytes. Reading chunk length as int (4 bytes)...",
          socket.getRemoteSocketAddress());

    final int len = in.readInt();
    updateMetricReceivedBytes(OBinaryProtocol.SIZE_INT + len);

    if (debug)
      OLogManager.instance().info(this, "%s - Read chunk lenght: %d", socket.getRemoteSocketAddress(), len);

    if (len < 0)
      return null;

    if (debug)
      OLogManager.instance().info(this, "%s - Reading %d bytes...", socket.getRemoteSocketAddress(), len);

    // REUSE STATIC BUFFER?
    final byte[] tmp = new byte[len];
    in.readFully(tmp);

    if (debug)
      OLogManager.instance().info(this, "%s - Read %d bytes: %s", socket.getRemoteSocketAddress(), len, new String(tmp));

    return tmp;
  }

  public List<String> readStringList() throws IOException {
    if (debug)
      OLogManager.instance().info(this, "%s - Reading string list. Reading string list items as int (4 bytes)...",
          socket.getRemoteSocketAddress());

    final int items = in.readInt();
    updateMetricReceivedBytes(OBinaryProtocol.SIZE_INT);

    if (debug)
      OLogManager.instance().info(this, "%s - Read string list items: %d", socket.getRemoteSocketAddress(), items);

    if (items < 0)
      return null;

    List<String> result = new ArrayList<String>();
    for (int i = 0; i < items; ++i)
      result.add(readString());

    if (debug)
      OLogManager.instance().info(this, "%s - Read string list with %d items: %d", socket.getRemoteSocketAddress(), items);

    return result;
  }

  public Set<String> readStringSet() throws IOException {
    if (debug)
      OLogManager.instance().info(this, "%s - Reading string set. Reading string set items as int (4 bytes)...",
          socket.getRemoteSocketAddress());

    int items = in.readInt();
    updateMetricReceivedBytes(OBinaryProtocol.SIZE_INT);

    if (debug)
      OLogManager.instance().info(this, "%s - Read string set items: %d", socket.getRemoteSocketAddress(), items);

    if (items < 0)
      return null;

    Set<String> result = new HashSet<String>();
    for (int i = 0; i < items; ++i)
      result.add(readString());

    if (debug)
      OLogManager.instance().info(this, "%s - Read string set with %d items: %d", socket.getRemoteSocketAddress(), items, result);

    return result;
  }

  public ORecordId readRID() throws IOException {
    final int clusterId = readShort();

    final OClusterPosition clusterPosition = readClusterPosition();
    return new ORecordId(clusterId, clusterPosition);
  }

  public OClusterPosition readClusterPosition() throws IOException {
    final int serializedSize = OClusterPositionFactory.INSTANCE.getSerializedSize();

    if (debug)
      OLogManager.instance().info(this, "%s - Reading cluster position (%d bytes)....", socket.getRemoteSocketAddress(),
          serializedSize);

    final OClusterPosition clusterPosition = OClusterPositionFactory.INSTANCE.fromStream((InputStream) in);

    updateMetricReceivedBytes(serializedSize);

    if (debug)
      OLogManager.instance().info(this, "%s - Read cluster position: %s", socket.getRemoteSocketAddress(), clusterPosition);

    return clusterPosition;
  }

  public OChannelBinary writeClusterPosition(final OClusterPosition clusterPosition) throws IOException {
    final int serializedSize = OClusterPositionFactory.INSTANCE.getSerializedSize();

    if (debug)
      OLogManager.instance().info(this, "%s - Writing cluster position (%d bytes) : %s....", socket.getRemoteSocketAddress(),
          serializedSize, clusterPosition);

    out.write(clusterPosition.toStream());

    updateMetricTransmittedBytes(serializedSize);

    return this;
  }

  public ORecordVersion readVersion() throws IOException {
    if (OVersionFactory.instance().isDistributed()) {
      final int recordVersion = readInt();
      final long timestamp = readLong();
      final long macAddress = readLong();
      return OVersionFactory.instance().createDistributedVersion(recordVersion, timestamp, macAddress);
    } else {
      final ORecordVersion version = OVersionFactory.instance().createVersion();
      version.setCounter(readInt());
      return version;
    }
  }

  public OChannelBinary writeByte(final byte iContent) throws IOException {
    if (debug)
      OLogManager.instance().info(this, "%s - Writing byte (1 byte): %d", socket.getRemoteSocketAddress(), iContent);

    out.write(iContent);
    updateMetricTransmittedBytes(OBinaryProtocol.SIZE_BYTE);
    return this;
  }

  public OChannelBinary writeInt(final int iContent) throws IOException {
    if (debug)
      OLogManager.instance().info(this, "%s - Writing int (4 bytes): %d", socket.getRemoteSocketAddress(), iContent);

    out.writeInt(iContent);
    updateMetricTransmittedBytes(OBinaryProtocol.SIZE_INT);
    return this;
  }

  public OChannelBinary writeLong(final long iContent) throws IOException {
    if (debug)
      OLogManager.instance().info(this, "%s - Writing long (8 bytes): %d", socket.getRemoteSocketAddress(), iContent);

    out.writeLong(iContent);
    updateMetricTransmittedBytes(OBinaryProtocol.SIZE_LONG);
    return this;
  }

  public OChannelBinary writeShort(final short iContent) throws IOException {
    if (debug)
      OLogManager.instance().info(this, "%s - Writing short (2 bytes): %d", socket.getRemoteSocketAddress(), iContent);

    out.writeShort(iContent);
    updateMetricTransmittedBytes(OBinaryProtocol.SIZE_SHORT);
    return this;
  }

  public OChannelBinary writeString(final String iContent) throws IOException {
    if (debug)
      OLogManager.instance().info(this, "%s - Writing string (4+%d=%d bytes): %s", socket.getRemoteSocketAddress(),
          iContent != null ? iContent.length() : 0, iContent != null ? iContent.length() + 4 : 4, iContent);

    if (iContent == null) {
      out.writeInt(-1);
      updateMetricTransmittedBytes(OBinaryProtocol.SIZE_INT);
    } else {
      final byte[] buffer = iContent.getBytes();
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
      OLogManager.instance().info(this, "%s - Writing bytes (4+%d=%d bytes): %s", socket.getRemoteSocketAddress(), iLength,
          iLength + 4, Arrays.toString(iContent));

    if (iContent == null) {
      out.writeInt(-1);
      updateMetricTransmittedBytes(OBinaryProtocol.SIZE_INT);
    } else {
      out.writeInt(iLength);
      out.write(iContent, 0, iLength);
      updateMetricTransmittedBytes(OBinaryProtocol.SIZE_INT + iLength);
    }
    return this;
  }

  public OChannelBinary writeCollectionString(final Collection<String> iCollection) throws IOException {
    if (debug)
      OLogManager.instance().info(this, "%s - Writing strings (4+%d=%d items): %s", socket.getRemoteSocketAddress(),
          iCollection != null ? iCollection.size() : 0, iCollection != null ? iCollection.size() + 4 : 4, iCollection.toString());

    updateMetricTransmittedBytes(OBinaryProtocol.SIZE_INT);
    if (iCollection == null)
      writeInt(-1);
    else {
      writeInt(iCollection.size());

      for (String s : iCollection)
        writeString(s);
    }

    return this;
  }

  public void writeRID(final ORID iRID) throws IOException {
    writeShort((short) iRID.getClusterId());
    writeClusterPosition(iRID.getClusterPosition());
  }

  public void writeVersion(final ORecordVersion version) throws IOException {
    if (version instanceof ODistributedVersion) {
      final ODistributedVersion v = (ODistributedVersion) version;
      writeInt(v.getCounter());
      writeLong(v.getTimestamp());
      writeLong(v.getMacAddress());
    } else {
      // Usual serialization
      writeInt(version.getCounter());
    }
  }

  public void clearInput() throws IOException {
    if (in == null)
      return;

    final StringBuilder dirtyBuffer = new StringBuilder(MAX_LENGTH_DEBUG);
    int i = 0;
    while (in.available() > 0) {
      char c = (char) in.read();
      ++i;

      if (dirtyBuffer.length() < MAX_LENGTH_DEBUG)
        dirtyBuffer.append(c);
    }
    updateMetricReceivedBytes(i);

    OLogManager.instance().error(
        this,
        "Received unread response from " + socket.getRemoteSocketAddress()
            + " probably corrupted data from the network connection. Cleared dirty data in the buffer (" + i + " bytes): ["
            + dirtyBuffer + (i > dirtyBuffer.length() ? "..." : "") + "]", OIOException.class);
  }

  @Override
  public void flush() throws IOException {
    if (debug)
      OLogManager.instance().info(this, "%s - Flush", socket.getRemoteSocketAddress());

    updateMetricFlushes();

    super.flush();
    out.flush();
  }

  @Override
  public void close() {
    if (debug)
      OLogManager.instance().info(this, "%s - Closing socket...", socket.getRemoteSocketAddress());

    try {
      if (in != null)
        in.close();
    } catch (IOException e) {
    }

    try {
      if (out != null)
        out.close();
    } catch (IOException e) {
    }

    super.close();
  }

  public int readStatus() throws IOException {
    // READ THE RESPONSE
    return handleStatus(readByte(), readInt());
  }

  protected int handleStatus(final byte iResult, final int iClientTxId) throws IOException {
    if (iResult == OChannelBinaryProtocol.RESPONSE_STATUS_OK || iResult == OChannelBinaryProtocol.PUSH_DATA) {
    } else if (iResult == OChannelBinaryProtocol.RESPONSE_STATUS_ERROR) {
      StringBuilder buffer = new StringBuilder();

      final List<OPair<String, String>> exceptions = new ArrayList<OPair<String, String>>();

      // EXCEPTION
      while (readByte() == 1) {
        final String excClassName = readString();
        final String excMessage = readString();
        exceptions.add(new OPair<String, String>(excClassName, excMessage));
      }

      Exception previous = null;
      for (int i = exceptions.size() - 1; i > -1; --i) {
        previous = createException(exceptions.get(i).getKey(), exceptions.get(i).getValue(), previous);
      }

      if (previous != null) {
        if (previous instanceof RuntimeException)
          throw (RuntimeException) previous;
        else
          throw new ODatabaseException("Generic error, see the underlying cause", previous);
      } else
        throw new ONetworkProtocolException("Network response error: " + buffer.toString());

    } else {
      // PROTOCOL ERROR
      // close();
      throw new ONetworkProtocolException("Error on reading response from the server");
    }
    return iClientTxId;
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
      rootException = new OStorageException(iMessage, iPrevious);
    }

    if (c != null)
      try {
        final Exception e;
        if (c.getParameterTypes().length > 1)
          e = (Exception) c.newInstance(iMessage, iPrevious);
        else
          e = (Exception) c.newInstance(iMessage);

        if (e instanceof RuntimeException)
          rootException = (RuntimeException) e;
        else
          rootException = new OException(e);
      } catch (InstantiationException e) {
      } catch (IllegalAccessException e) {
      } catch (InvocationTargetException e) {
      }

    return rootException;
  }

  public byte[] getBuffer() {
    return buffer;
  }

  public int getMaxChunkSize() {
    return maxChunkSize;
  }
}
