package com.orientechnologies.orient.enterprise.channel.binary;

import com.orientechnologies.orient.core.id.ORID;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by luigidellaquila on 12/12/16.
 */
public interface OChannelDataOutput {

  public OChannelBinary writeByte(final byte iContent) throws IOException;

  public OChannelBinary writeBoolean(final boolean iContent) throws IOException;

  public OChannelBinary writeInt(final int iContent) throws IOException ;

  public OChannelBinary writeLong(final long iContent) throws IOException;

  public OChannelBinary writeShort(final short iContent) throws IOException;

  public OChannelBinary writeString(final String iContent) throws IOException;

  public OChannelBinary writeBytes(final byte[] iContent) throws IOException;

  public OChannelBinary writeBytes(final byte[] iContent, final int iLength) throws IOException;

  public void writeRID(final ORID iRID) throws IOException;

  public void writeVersion(final int version) throws IOException;

  OutputStream getDataOutput();

}


