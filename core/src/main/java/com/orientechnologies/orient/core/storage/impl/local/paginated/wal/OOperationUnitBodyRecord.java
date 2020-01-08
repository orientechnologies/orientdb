package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 31/12/14
 */
public abstract class OOperationUnitBodyRecord<T> extends OOperationUnitRecord<T> {
  protected OOperationUnitBodyRecord() {
  }

  protected OOperationUnitBodyRecord(T operationUnitId) {
    super(operationUnitId);
  }
}