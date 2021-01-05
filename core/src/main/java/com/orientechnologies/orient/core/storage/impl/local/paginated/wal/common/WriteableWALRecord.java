package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;
import java.nio.ByteBuffer;

public interface WriteableWALRecord extends OWALRecord {
  void setBinaryContent(ByteBuffer buffer, long pointer);

  ByteBuffer getBinaryContent();

  void freeBinaryContent();

  int getBinaryContentLen();

  int toStream(byte[] content, int offset);

  void toStream(ByteBuffer buffer);

  int fromStream(byte[] content, int offset);

  int serializedSize();

  void written();

  boolean isWritten();

  int getId();
}
