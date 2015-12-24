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

  void add(byte[] value, int start);

  OWALChangesTree.PointerWrapper wrap(final ODirectMemoryPointer pointer);

  void applyChanges(ODirectMemoryPointer pointer);


}
