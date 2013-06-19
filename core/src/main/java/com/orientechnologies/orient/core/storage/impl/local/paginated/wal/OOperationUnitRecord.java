package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

/**
 * @author Andrey Lomakin
 * @since 30.05.13
 */
public abstract class OOperationUnitRecord implements OWALRecord {
  private OOperationUnitId operationUnitId;

  protected OOperationUnitRecord() {
  }

  protected OOperationUnitRecord(OOperationUnitId operationUnitId) {
    this.operationUnitId = operationUnitId;
  }

  public OOperationUnitId getOperationUnitId() {
    return operationUnitId;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    return operationUnitId.toStream(content, offset);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    operationUnitId = new OOperationUnitId();
    return operationUnitId.fromStream(content, offset);
  }

  @Override
  public int serializedSize() {
    return OOperationUnitId.SERIALIZED_SIZE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OOperationUnitRecord that = (OOperationUnitRecord) o;

    if (!operationUnitId.equals(that.operationUnitId))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return operationUnitId.hashCode();
  }

  @Override
  public String toString() {
    return "OOperationUnitRecord{" + "operationUnitId=" + operationUnitId + "} ";
  }
}
