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

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordOperationMetadata;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 24.05.13
 */
public class OAtomicUnitEndRecord extends OOperationUnitBodyRecord {
  private boolean rollback;

  private Map<String, OAtomicOperationMetadata<?>> atomicOperationMetadataMap =
      new LinkedHashMap<>();

  public OAtomicUnitEndRecord() {}

  public OAtomicUnitEndRecord(
      final long operationUnitId,
      final boolean rollback,
      final Map<String, OAtomicOperationMetadata<?>> atomicOperationMetadataMap) {
    super(operationUnitId);

    this.rollback = rollback;

    if (atomicOperationMetadataMap != null && atomicOperationMetadataMap.size() > 0) {
      this.atomicOperationMetadataMap = atomicOperationMetadataMap;
    }
  }

  public boolean isRollback() {
    return rollback;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    buffer.put(rollback ? (byte) 1 : 0);

    if (atomicOperationMetadataMap.size() > 0) {
      for (final Map.Entry<String, OAtomicOperationMetadata<?>> entry :
          atomicOperationMetadataMap.entrySet()) {
        if (entry.getKey().equals(ORecordOperationMetadata.RID_METADATA_KEY)) {
          buffer.put((byte) 1);

          final ORecordOperationMetadata recordOperationMetadata =
              (ORecordOperationMetadata) entry.getValue();
          final Set<ORID> rids = recordOperationMetadata.getValue();
          buffer.putInt(rids.size());

          for (final ORID rid : rids) {
            buffer.putLong(rid.getClusterPosition());
            buffer.putInt(rid.getClusterId());
          }
        } else {
          throw new IllegalStateException(
              "Invalid metadata key " + ORecordOperationMetadata.RID_METADATA_KEY);
        }
      }
    } else {
      buffer.put((byte) 0);
    }
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    rollback = buffer.get() > 0;
    atomicOperationMetadataMap = new LinkedHashMap<>();

    final int metadataId = buffer.get();

    if (metadataId == 1) {
      final int collectionsSize = buffer.getInt();

      final ORecordOperationMetadata recordOperationMetadata = new ORecordOperationMetadata();
      for (int i = 0; i < collectionsSize; i++) {
        final long clusterPosition = buffer.getLong();
        final int clusterId = buffer.getInt();

        recordOperationMetadata.addRid(new ORecordId(clusterId, clusterPosition));
      }

      atomicOperationMetadataMap.put(recordOperationMetadata.getKey(), recordOperationMetadata);
    } else if (metadataId > 0) {
      throw new IllegalStateException("Invalid metadata entry id " + metadataId);
    }
  }

  public Map<String, OAtomicOperationMetadata<?>> getAtomicOperationMetadata() {
    return Collections.unmodifiableMap(atomicOperationMetadataMap);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OByteSerializer.BYTE_SIZE + metadataSize();
  }

  private int metadataSize() {
    int size = OByteSerializer.BYTE_SIZE;

    for (Map.Entry<String, OAtomicOperationMetadata<?>> entry :
        atomicOperationMetadataMap.entrySet()) {
      if (entry.getKey().equals(ORecordOperationMetadata.RID_METADATA_KEY)) {
        final ORecordOperationMetadata recordOperationMetadata =
            (ORecordOperationMetadata) entry.getValue();

        size += OIntegerSerializer.INT_SIZE;
        final Set<ORID> rids = recordOperationMetadata.getValue();
        size += rids.size() * (OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);
      } else {
        throw new IllegalStateException(
            "Invalid metadata key " + ORecordOperationMetadata.RID_METADATA_KEY);
      }
    }

    return size;
  }

  @Override
  public String toString() {
    return toString("rollback=" + rollback);
  }

  @Override
  public int getId() {
    return WALRecordTypes.ATOMIC_UNIT_END_RECORD;
  }
}
