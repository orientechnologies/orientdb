package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.nio.ByteBuffer;

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

  byte getByteValue(ByteBuffer buffer, int offset);

  byte[] getBinaryValue(ByteBuffer buffer, int offset, int len);

  short getShortValue(ByteBuffer buffer, int offset);

  int getIntValue(ByteBuffer buffer, int offset);

  long getLongValue(ByteBuffer buffer, int offset);

  void setLongValue(ByteBuffer buffer, long value, int offset);

  void setIntValue(ByteBuffer buffer, int value, int offset);

  void setByteValue(ByteBuffer buffer, byte value, int offset);

  void setBinaryValue(ByteBuffer buffer, byte[] value, int offset);

  void moveData(ByteBuffer buffer, int from, int to, int len);

  /**
   * Apply the changes to a page.
   *
   * @param buffer Presents page where apply the changes.
   */
  void applyChanges(ByteBuffer buffer);

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

  OWALChanges inverse(ByteBuffer buffer);

}
