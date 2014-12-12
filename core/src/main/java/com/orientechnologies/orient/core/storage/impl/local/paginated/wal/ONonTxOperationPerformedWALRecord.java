package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 12/12/14
 */
public class ONonTxOperationPerformedWALRecord extends OAbstractWALRecord {
  @Override
  public int toStream(byte[] content, int offset) {
    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
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
}