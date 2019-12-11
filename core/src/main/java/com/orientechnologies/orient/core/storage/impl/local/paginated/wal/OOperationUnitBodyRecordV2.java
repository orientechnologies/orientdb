package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 31/12/14
 */
public abstract class OOperationUnitBodyRecordV2 extends OOperationUnitRecordV2 {
  protected OOperationUnitBodyRecordV2() {
  }

  protected OOperationUnitBodyRecordV2(long operationUnitId) {
    super(operationUnitId);
  }
}