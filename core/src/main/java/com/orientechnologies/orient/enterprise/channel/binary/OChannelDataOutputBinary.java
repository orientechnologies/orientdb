package com.orientechnologies.orient.enterprise.channel.binary;

import com.orientechnologies.common.exception.OInvalidBinaryChunkException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import java.io.DataOutputStream;
import java.io.IOException;

public class OChannelDataOutputBinary implements OChannelDataOutput {
  private final DataOutputStream out;
  private final int maxChunkSize;
  private final OChannelStatsCounter stats;
  private final OChannelStatsCounter flushStats;

  public OChannelDataOutputBinary(
      DataOutputStream out,
      int maxChunkSize,
      OChannelStatsCounter stats,
      OChannelStatsCounter flushStats) {
    super();
    this.out = out;
    this.maxChunkSize = maxChunkSize;
    this.stats = stats;
    this.flushStats = flushStats;
  }

  public OChannelDataOutputBinary writeByte(final byte iContent) throws IOException {
    out.write(iContent);
    stats.count(OBinaryProtocol.SIZE_BYTE);
    return this;
  }

  public OChannelDataOutputBinary writeBoolean(final boolean iContent) throws IOException {
    out.writeBoolean(iContent);
    stats.count(OBinaryProtocol.SIZE_BYTE);
    return this;
  }

  public OChannelDataOutputBinary writeInt(final int iContent) throws IOException {
    out.writeInt(iContent);
    stats.count(OBinaryProtocol.SIZE_INT);
    return this;
  }

  public OChannelDataOutputBinary writeLong(final long iContent) throws IOException {
    out.writeLong(iContent);
    stats.count(OBinaryProtocol.SIZE_LONG);
    return this;
  }

  public OChannelDataOutputBinary writeShort(final short iContent) throws IOException {
    out.writeShort(iContent);
    stats.count(OBinaryProtocol.SIZE_SHORT);
    return this;
  }

  public OChannelDataOutputBinary writeString(final String iContent) throws IOException {
    if (iContent == null) {
      out.writeInt(-1);
      stats.count(OBinaryProtocol.SIZE_INT);
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
      stats.count(OBinaryProtocol.SIZE_INT + buffer.length);
    }

    return this;
  }

  public OChannelDataOutputBinary writeBytes(final byte[] iContent) throws IOException {
    return writeBytes(iContent, iContent != null ? iContent.length : 0);
  }

  public OChannelDataOutputBinary writeBytes(final byte[] iContent, final int iLength)
      throws IOException {
    if (iContent == null) {
      out.writeInt(-1);
      stats.count(OBinaryProtocol.SIZE_INT);
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
      stats.count(OBinaryProtocol.SIZE_INT + iLength);
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

  @Override
  public void flush() throws IOException {
    this.flushStats.count(1);

    out.flush();
  }
}
