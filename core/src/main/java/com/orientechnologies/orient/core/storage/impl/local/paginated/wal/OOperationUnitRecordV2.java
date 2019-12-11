/*
 * Copyright 2010-2013 OrientDB LTD (info--at--orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OLongSerializer;

import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 30.05.13
 */
public abstract class OOperationUnitRecordV2 extends OAbstractWALRecord {
  private long operationUnitId;

  protected OOperationUnitRecordV2() {
  }

  protected OOperationUnitRecordV2(long operationUnitId) {
    this.operationUnitId = operationUnitId;
  }

  public long getOperationUnitId() {
    return operationUnitId;
  }

  @Override
  public int toStream(final byte[] content, final int offset) {
    OLongSerializer.INSTANCE.serializeNative(operationUnitId, content, offset);
    return offset + OLongSerializer.LONG_SIZE;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putLong(operationUnitId);
  }

  @Override
  public int fromStream(final byte[] content, final int offset) {
    operationUnitId = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    return offset + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int serializedSize() {
    return OLongSerializer.LONG_SIZE;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final OOperationUnitRecordV2 that = (OOperationUnitRecordV2) o;

    return operationUnitId == that.operationUnitId;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(operationUnitId);
  }

  @Override
  public String toString() {
    return toString("operationUnitId=" + operationUnitId);
  }
}
