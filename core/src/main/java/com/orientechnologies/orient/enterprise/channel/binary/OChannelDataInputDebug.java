package com.orientechnologies.orient.enterprise.channel.binary;

import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.id.ORecordId;
import java.io.IOException;
import java.net.SocketAddress;

public class OChannelDataInputDebug implements OChannelDataInput {
  private static final OLogger logger = OLogger.get(OChannelDataInputDebug.class);
  private final OChannelDataInput in;
  private final SocketAddress address;

  public OChannelDataInputDebug(OChannelDataInput in, SocketAddress address) {
    super();
    this.in = in;
    this.address = address;
  }

  public byte readByte() throws IOException {
    logger.debug("%s - Reading byte (1 byte)...", address);
    final byte value = in.readByte();
    logger.debug("%s - Read byte: %d", address, (int) value);
    return value;
  }

  public boolean readBoolean() throws IOException {
    logger.debug("%s - Reading boolean (1 byte)...", address);
    final boolean value = in.readBoolean();
    logger.debug("%s - Read boolean: %b", address, value);
    return value;
  }

  public int readInt() throws IOException {
    logger.debug("%s - Reading int (4 bytes)...", address);
    final int value = in.readInt();
    logger.debug("%s - Read int: %d", address, value);
    return value;
  }

  public long readLong() throws IOException {
    logger.debug("%s - Reading long (8 bytes)...", address);
    final long value = in.readLong();
    logger.debug("%s - Read long: %d", address, value);
    return value;
  }

  public short readShort() throws IOException {
    logger.debug("%s - Reading short (2 bytes)...", address);
    final short value = in.readShort();
    logger.debug("%s - Read short: %d", address, value);
    return value;
  }

  public String readString() throws IOException {
    logger.debug("%s - Reading string (4+N bytes)...", address);
    final String value = in.readString();
    logger.debug("%s - Read string: %s", address, value);
    return value;
  }

  public byte[] readBytes() throws IOException {
    logger.debug("%s - Reading chunk of bytes. Reading chunk length as int (4 bytes)...", address);
    final byte[] tmp = in.readBytes();
    logger.debug("%s - Read %d bytes: %s", address, tmp.length, new String(tmp));

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
