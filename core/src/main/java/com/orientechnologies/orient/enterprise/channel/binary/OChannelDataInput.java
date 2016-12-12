package com.orientechnologies.orient.enterprise.channel.binary;

import com.orientechnologies.orient.core.id.ORecordId;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by luigidellaquila on 12/12/16.
 */
public interface OChannelDataInput {

  public byte readByte() throws IOException;

  public boolean readBoolean() throws IOException;

  public int readInt() throws IOException;

  public long readLong() throws IOException;

  public short readShort() throws IOException;

  public String readString() throws IOException;

  public byte[] readBytes() throws IOException;

  public ORecordId readRID() throws IOException;

  public int readVersion() throws IOException;

  InputStream getDataInput();
}
