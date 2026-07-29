package com.orientechnologies.orient.enterprise.channel.binary;

import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.id.ORID;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.Arrays;

public class OChannelDataOutputDebug implements OChannelDataOutput {
  private static final OLogger logger = OLogger.get(OChannelDataInputDebug.class);
  private final OChannelDataOutput out;
  private final SocketAddress address;

  public OChannelDataOutputDebug(OChannelDataOutput out, SocketAddress address) {
    super();
    this.out = out;
    this.address = address;
  }

  public OChannelDataOutputDebug writeByte(final byte iContent) throws IOException {
    logger.debug("%s - Writing byte (1 byte): %d", address, iContent);

    out.writeByte(iContent);
    return this;
  }

  public OChannelDataOutputDebug writeBoolean(final boolean iContent) throws IOException {
    logger.debug("%s - Writing boolean (1 byte): %b", address, iContent);

    out.writeBoolean(iContent);
    return this;
  }

  public OChannelDataOutputDebug writeInt(final int iContent) throws IOException {
    logger.debug("%s - Writing int (4 bytes): %d", address, iContent);
    out.writeInt(iContent);
    return this;
  }

  public OChannelDataOutputDebug writeLong(final long iContent) throws IOException {
    logger.debug("%s - Writing long (8 bytes): %d", address, iContent);
    out.writeLong(iContent);
    return this;
  }

  public OChannelDataOutputDebug writeShort(final short iContent) throws IOException {
    logger.debug("%s - Writing short (2 bytes): %d", address, iContent);
    out.writeShort(iContent);
    return this;
  }

  public OChannelDataOutputDebug writeString(final String iContent) throws IOException {
    logger.debug(
        "%s - Writing string (4+%d=%d bytes): %s",
        address,
        iContent != null ? iContent.length() : 0,
        iContent != null ? iContent.length() + 4 : 4,
        iContent);

    out.writeString(iContent);
    return this;
  }

  public OChannelDataOutputDebug writeBytes(final byte[] iContent) throws IOException {
    return writeBytes(iContent, iContent != null ? iContent.length : 0);
  }

  public OChannelDataOutputDebug writeBytes(final byte[] iContent, final int iLength)
      throws IOException {
    logger.debug(
        "%s - Writing bytes (4+%d=%d bytes): %s",
        address, iLength, iLength + 4, Arrays.toString(iContent));

    out.writeBytes(iContent, iLength);
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
    logger.info("%s - Flush", address);
    out.flush();
  }
}
