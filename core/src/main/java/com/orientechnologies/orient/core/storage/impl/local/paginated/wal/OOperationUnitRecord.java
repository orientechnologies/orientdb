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
public abstract class OOperationUnitRecord<T> extends OAbstractWALRecord {
  private T operationUnitId;

  protected OOperationUnitRecord() {
  }

  protected OOperationUnitRecord(T operationUnitId) {
    this.operationUnitId = operationUnitId;
  }

  public T getOperationUnitId() {
    return operationUnitId;
  }

  @Override
  public int toStream(final byte[] content, final int offset) {
    if (this instanceof OperationUnitOperationId) {
      return ((OOperationUnitId) operationUnitId).toStream(content, offset);
    } else if (this instanceof LongOperationId) {
      OLongSerializer.INSTANCE.serializeNative((Long) operationUnitId, content, offset);
      return offset + OLongSerializer.LONG_SIZE;
    } else {
      throw new IllegalStateException("Invalid type of operation unit id");
    }
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    if (this instanceof OperationUnitOperationId) {
      ((OOperationUnitId) operationUnitId).toStream(buffer);
    } else if (this instanceof LongOperationId) {
      buffer.putLong((Long) operationUnitId);
    } else {
      throw new IllegalStateException("Invalid type of operation unit id");
    }
  }

  @Override
  public int fromStream(final byte[] content, final int offset) {
    if (this instanceof OperationUnitOperationId) {
      final OOperationUnitId operationUnitId = new OOperationUnitId();
      final int position = operationUnitId.fromStream(content, offset);
      //noinspection unchecked
      this.operationUnitId = (T) operationUnitId;
      return position;
    } else if (this instanceof LongOperationId) {
      //noinspection unchecked
      operationUnitId = (T) OLongSerializer.INSTANCE.deserializeNativeObject(content, offset);
      return offset + OLongSerializer.LONG_SIZE;
    } else {
      throw new IllegalStateException("Invalid type of operation id");
    }
  }

  @Override
  public int serializedSize() {
    if (this instanceof OperationUnitOperationId) {
      return OOperationUnitId.SERIALIZED_SIZE;
    } else if (this instanceof LongOperationId) {
      return OLongSerializer.LONG_SIZE;
    } else {
      throw new IllegalStateException("Invalid type of operation id");
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final OOperationUnitRecord that = (OOperationUnitRecord) o;

    return operationUnitId.equals(that.operationUnitId);
  }

  @Override
  public int hashCode() {
    return operationUnitId.hashCode();
  }

  @Override
  public String toString() {
    return toString("operationUnitId=" + operationUnitId);
  }
}
