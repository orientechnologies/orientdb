package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <a
 *     href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 12/12/14
 */
public class ONonTxOperationPerformedWALRecord extends OAbstractWALRecord {
  @Override
  public int toStream(byte[] content, int offset) {
    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {}

  @Override
  public int fromStream(byte[] content, int offset) {
    return offset;
  }

  @Override
  public int serializedSize() {
    return 0;
  }

  @Override
  public int getId() {
    return WALRecordTypes.NON_TX_OPERATION_PERFORMED_WAL_RECORD;
  }
}
