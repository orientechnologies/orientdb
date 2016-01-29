/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
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

import java.io.IOException;
import java.util.*;

/**
 * @author Andrey Lomakin
 * @since 24.05.13
 */
public class OAtomicUnitEndRecord extends OOperationUnitBodyRecord {
  private boolean rollback;

  private Map<String, OAtomicOperationMetadata<?>> atomicOperationMetadataMap = new HashMap<String, OAtomicOperationMetadata<?>>();

  public OAtomicUnitEndRecord() {
  }

  OAtomicUnitEndRecord(final OOperationUnitId operationUnitId, final boolean rollback,
      final Map<String, OAtomicOperationMetadata<?>> atomicOperationMetadataMap) throws IOException {
    super(operationUnitId);

    this.rollback = rollback;

    assert operationUnitId != null;

    if (atomicOperationMetadataMap != null && atomicOperationMetadataMap.size() > 0) {
      this.atomicOperationMetadataMap = atomicOperationMetadataMap;
    }
  }

  public boolean isRollback() {
    return rollback;
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    content[offset] = rollback ? (byte) 1 : 0;
    offset++;

    if (atomicOperationMetadataMap.size() > 0) {
      for (Map.Entry<String, OAtomicOperationMetadata<?>> entry : atomicOperationMetadataMap.entrySet()) {
        if (entry.getKey().equals(ORecordOperationMetadata.RID_METADATA_KEY)) {
          content[offset] = 1;
          offset++;

          final ORecordOperationMetadata recordOperationMetadata = (ORecordOperationMetadata) entry.getValue();
          final Set<ORID> rids = recordOperationMetadata.getValue();
          OIntegerSerializer.INSTANCE.serializeNative(rids.size(), content, offset);
          offset += OIntegerSerializer.INT_SIZE;

          for (ORID rid : rids) {
            OLongSerializer.INSTANCE.serializeNative(rid.getClusterPosition(), content, offset);
            offset += OLongSerializer.LONG_SIZE;

            OIntegerSerializer.INSTANCE.serializeNative(rid.getClusterId(), content, offset);
            offset += OIntegerSerializer.INT_SIZE;
          }
        } else {
          throw new IllegalStateException("Invalid metadata key " + ORecordOperationMetadata.RID_METADATA_KEY);
        }
      }
    } else {
      offset++;
    }

    return offset;
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    rollback = content[offset] > 0;
    offset++;

    atomicOperationMetadataMap = new HashMap<String, OAtomicOperationMetadata<?>>();

    final int metadataId = content[offset];
    offset++;

    if (metadataId == 1) {
      final int collectionsSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      final ORecordOperationMetadata recordOperationMetadata = new ORecordOperationMetadata();
      for (int i = 0; i < collectionsSize; i++) {
        final long clusterPosition = OLongSerializer.INSTANCE.deserializeNative(content, offset);
        offset += OLongSerializer.LONG_SIZE;

        final int clusterId = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
        offset += OIntegerSerializer.INT_SIZE;

        recordOperationMetadata.addRid(new ORecordId(clusterId, clusterPosition));
      }

      atomicOperationMetadataMap.put(recordOperationMetadata.getKey(), recordOperationMetadata);
    } else if (metadataId > 0)
      throw new IllegalStateException("Invalid metadata entry id " + metadataId);

    return offset;
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

    for (Map.Entry<String, OAtomicOperationMetadata<?>> entry : atomicOperationMetadataMap.entrySet()) {
      if (entry.getKey().equals(ORecordOperationMetadata.RID_METADATA_KEY)) {
        final ORecordOperationMetadata recordOperationMetadata = (ORecordOperationMetadata) entry.getValue();

        size += OIntegerSerializer.INT_SIZE;
        final Set<ORID> rids = recordOperationMetadata.getValue();
        size += rids.size() * (OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);
      } else {
        throw new IllegalStateException("Invalid metadata key " + ORecordOperationMetadata.RID_METADATA_KEY);
      }
    }

    return size;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public String toString() {
    return toString("rollback=" + rollback);
  }
}
