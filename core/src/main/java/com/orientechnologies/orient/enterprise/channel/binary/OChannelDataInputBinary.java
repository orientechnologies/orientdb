package com.orientechnologies.orient.enterprise.channel.binary;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import java.io.DataInputStream;
import java.io.IOException;

public class OChannelDataInputBinary implements OChannelDataInput {

  private final DataInputStream in;
  private final int maxChunkSize;
  private final OChannelStatsCounter stats;

  public OChannelDataInputBinary(DataInputStream in, int maxChunkSize, OChannelStatsCounter stats) {
    super();
    this.in = in;
    this.maxChunkSize = maxChunkSize;
    this.stats = stats;
  }

  public byte readByte() throws IOException {
    stats.count(OBinaryProtocol.SIZE_BYTE);
    return in.readByte();
  }

  public boolean readBoolean() throws IOException {
    stats.count(OBinaryProtocol.SIZE_BYTE);
    return in.readBoolean();
  }

  public int readInt() throws IOException {
    stats.count(OBinaryProtocol.SIZE_INT);
    return in.readInt();
  }

  public long readLong() throws IOException {
    stats.count(OBinaryProtocol.SIZE_LONG);
    return in.readLong();
  }

  public short readShort() throws IOException {
    stats.count(OBinaryProtocol.SIZE_SHORT);
    return in.readShort();
  }

  public String readString() throws IOException {
    final int len = in.readInt();
    if (len < 0) return null;
    if (len > maxChunkSize) {
      throw new IOException(
          "Impossible to read a string chunk of length:"
              + len
              + " max allowed chunk length:"
              + maxChunkSize
              + " see NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");
    }
    stats.count(OBinaryProtocol.SIZE_INT + len);
    final byte[] tmp = new byte[len];
    in.readFully(tmp);

    return new String(tmp, "UTF-8");
  }

  public byte[] readBytes() throws IOException {
    final int len = in.readInt();
    if (len > maxChunkSize) {
      throw new IOException(
          "Impossible to read a chunk of length:"
              + len
              + " max allowed chunk length:"
              + maxChunkSize
              + " see NETWORK_BINARY_MAX_CONTENT_LENGTH settings ");
    }

    if (len < 0) return null;
    stats.count(OBinaryProtocol.SIZE_INT + len);
    // REUSE STATIC BUFFER?
    final byte[] tmp = new byte[len];
    in.readFully(tmp);

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
}
