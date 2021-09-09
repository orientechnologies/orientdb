/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 26.04.13
 */
public class OUpdatePageRecord extends OAbstractPageWALRecord {
  private OWALChanges changes;

  @SuppressWarnings("WeakerAccess")
  public OUpdatePageRecord() {}

  public OUpdatePageRecord(
      final long pageIndex,
      final long fileId,
      final long operationUnitId,
      final OWALChanges changes) {
    super(pageIndex, fileId, operationUnitId);
    this.changes = changes;
  }

  public OWALChanges getChanges() {
    return changes;
  }

  @Override
  public int serializedSize() {
    int serializedSize = super.serializedSize();
    serializedSize += changes.serializedSize();

    serializedSize += 2 * OLongSerializer.LONG_SIZE;

    return serializedSize;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    changes.toStream(buffer);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    changes = new OWALPageChangesPortion();
    changes.fromStream(buffer);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    final OUpdatePageRecord that = (OUpdatePageRecord) o;

    if (logSequenceNumber == null && that.logSequenceNumber == null) {
      return true;
    }
    if (logSequenceNumber == null) {
      return false;
    }

    if (that.logSequenceNumber == null) {
      return false;
    }

    return Objects.equals(logSequenceNumber, that.logSequenceNumber);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (logSequenceNumber != null ? logSequenceNumber.hashCode() : 0);
    return result;
  }

  @Override
  public int getId() {
    return WALRecordTypes.UPDATE_PAGE_RECORD;
  }
}
