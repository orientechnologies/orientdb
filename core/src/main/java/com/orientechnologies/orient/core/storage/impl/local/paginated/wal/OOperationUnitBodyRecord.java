package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <a
 *     href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 31/12/14
 */
public abstract class OOperationUnitBodyRecord extends OOperationUnitRecord {
  protected OOperationUnitBodyRecord() {}

  protected OOperationUnitBodyRecord(long operationUnitId) {
    super(operationUnitId);
  }
}
