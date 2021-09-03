package com.orientechnologies.orient.enterprise.channel.binary;

import com.orientechnologies.orient.core.id.ORecordId;
import java.io.IOException;
import java.io.InputStream;

/** Created by luigidellaquila on 12/12/16. */
public interface OChannelDataInput {

  byte readByte() throws IOException;

  boolean readBoolean() throws IOException;

  int readInt() throws IOException;

  long readLong() throws IOException;

  short readShort() throws IOException;

  String readString() throws IOException;

  byte[] readBytes() throws IOException;

  ORecordId readRID() throws IOException;

  int readVersion() throws IOException;

  InputStream getDataInput();
}
