package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;

/**
 * keep partial changes of a page for a transaction.
 * <p>
 * use get* to access to the original content decorated with the changes.
 * use set* to add a change.
 * <p>
 * <p>
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

  void moveData(ODirectMemoryPointer pointer, int from, int to, int len);

  /**
   * Create a pointer wrapper that keep a page plus the relative changes.
   *
   * @param pointer the pointer to wrap.
   * @return pointer wrapper.
   */
  PointerWrapper wrap(final ODirectMemoryPointer pointer);


  /**
   * Apply the changes to a page.
   *
   * @param pointer of the page where apply the changes.
   */
  void applyChanges(ODirectMemoryPointer pointer);

  /**
   * Return the size needed in a buffer in case of serialization.
   *
   * @return the required size.
   */
  int serializedSize();

  /**
   * Serialize the changes to a stream.
   * needed for write the changes to the WAL
   *
   * @param offset starting writing offset for the provided buffer.
   * @param stream buffer where write the content, should be of minimal size of offset+ @{link @serializedSize()}
   * @return the number of written bytes + the offset, can be used as offset of the next operation.
   */
  int toStream(int offset, byte[] stream);


  /**
   * Read changes from a stream.
   * Needed from restore from WAL.
   *
   * @param offset the offest in the buffer where start to read.
   * @param stream the buffer to read.
   * @return the offset+read bytes.
   */
  int fromStream(int offset, byte[] stream);

}
