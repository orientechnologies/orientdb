package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

public class OAtomicUnitStartRecordV1 extends OAtomicUnitStartRecord<OOperationUnitId> implements OperationUnitOperationId {
  public OAtomicUnitStartRecordV1() {
  }

  public OAtomicUnitStartRecordV1(boolean isRollbackSupported, OOperationUnitId unitId) {
    super(isRollbackSupported, unitId);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.ATOMIC_UNIT_START_RECORD;
  }
}
