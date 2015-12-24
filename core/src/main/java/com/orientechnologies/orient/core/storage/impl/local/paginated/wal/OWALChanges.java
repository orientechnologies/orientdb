package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;

/**
 * Created by tglman on 24/12/15.
 */
public interface OWALChanges {

  byte getByteValue(ODirectMemoryPointer pointer, int offset);

  byte[] getBinaryValue(ODirectMemoryPointer pointer, int offset, int len);

  short getShortValue(ODirectMemoryPointer pointer, int offset);

  int getIntValue(ODirectMemoryPointer pointer, int offset);

  long getLongValue(ODirectMemoryPointer pointer, int offset);

  void setLongValue(ODirectMemoryPointer pointer, int offset, long value);

  void setIntValue(ODirectMemoryPointer pointer, int offset, int value);

  void setByteValue(ODirectMemoryPointer pointer, int offset, byte value);

  void setBinaryValue(ODirectMemoryPointer pointer, int offset, byte[] value);

  void moveData(ODirectMemoryPointer pointer, int from, int to, int  len);

  PointerWrapper wrap(final ODirectMemoryPointer pointer);

  void applyChanges(ODirectMemoryPointer pointer);

  int serializedSize();

  int toStream(int offset, byte[] stream);

  int fromStream(int offset, byte[] stream);

}
