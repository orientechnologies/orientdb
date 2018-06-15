package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 12/12/14
 */
public final class ONonTxOperationPerformedWALRecord extends OAbstractWALRecord {
  @Override
  public int toStream(final byte[] content, final int offset) {
    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
  }

  @Override
  public int fromStream(final byte[] content, final int offset) {
    return offset;
  }

  @Override
  public int serializedSize() {
    return 0;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.NON_TX_OPERATION_PERFORMED_WAL_RECORD;
  }
}