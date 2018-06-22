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

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OEmptyWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OWriteableWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.OAllocatePositionOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.OCreateClusterOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.OCreateRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.ODeleteRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.OMakePositionAvailableOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.ORecycleRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.OUpdateRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable.OCreateHashTableOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable.OHashTablePutOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable.OHashTableRemoveOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.OCreateSBTreeOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.OSBTreePutOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.OSBTreeRemoveOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.OCreateSBTreeBonsaiOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.OCreateSBTreeBonsaiRawOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.OSBTreeBonsaiPutOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.OSBTreeBonsaiRemoveOperation;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.ALLOCATE_POSITION_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.ATOMIC_UNIT_END_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.ATOMIC_UNIT_START_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CHECKPOINT_END_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CREATE_CLUSTER_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CREATE_HASH_TABLE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CREATE_RECORD_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CREATE_SBTREE_BONSAI_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CREATE_SBTREE_BONSAI_RAW_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CREATE_SBTREE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.DELETE_RECORD_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.EMPTY_WAL_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.FILE_CREATED_WAL_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.FILE_DELETED_WAL_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.FILE_TRUNCATED_WAL_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.FULL_CHECKPOINT_START_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.FUZZY_CHECKPOINT_END_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.FUZZY_CHECKPOINT_START_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.HASH_TABLE_PUT_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.HASH_TABLE_REMOVE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.MAKE_POSITION_AVAILABLE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.NON_TX_OPERATION_PERFORMED_WAL_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.RECYCLE_RECORD_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_PUT_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_REMOVE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_PUT_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_REMOVE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.UPDATE_PAGE_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.UPDATE_RECORD_OPERATION;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 25.04.13
 */
public final class OWALRecordsFactory {
  private final Map<Byte, Class> idToTypeMap = new HashMap<>();

  public static final OWALRecordsFactory INSTANCE = new OWALRecordsFactory();

  private static final LZ4Factory factory                    = LZ4Factory.fastestInstance();
  private static final int        MIN_COMPRESSED_RECORD_SIZE = OGlobalConfiguration.WAL_MINIMAL_COMPRESSED_RECORD_SIZE.

      getValueAsInteger();

  public static OPair<ByteBuffer, Long> toStream(final OWriteableWALRecord walRecord) {
    final int contentSize = walRecord.serializedSize() + 1;

    final long pointer;
    final ByteBuffer content;

    if (contentSize > 256) {
      pointer = Native.malloc(contentSize);
      content = new Pointer(pointer).getByteBuffer(0, contentSize);
    } else {
      content = ByteBuffer.allocate(contentSize).order(ByteOrder.nativeOrder());
      pointer = -1;
    }

    final byte recordId = walRecord.getId();
    content.put(recordId);
    walRecord.toStream(content);

    if (MIN_COMPRESSED_RECORD_SIZE <= 0 || contentSize < MIN_COMPRESSED_RECORD_SIZE) {
      return new OPair<>(content, pointer);
    }

    final LZ4Compressor compressor = factory.fastCompressor();
    final int maxCompressedLength = compressor.maxCompressedLength(contentSize - 1);

    final long compressedPointer = Native.malloc(maxCompressedLength + 5);
    final ByteBuffer compressedContent = new Pointer(compressedPointer).
        getByteBuffer(0, maxCompressedLength + 5).order(ByteOrder.nativeOrder());

    content.position(1);
    compressedContent.position(5);
    final int compressedLength = compressor.compress(content, 1, contentSize - 1, compressedContent, 5, maxCompressedLength);

    if (pointer > 0) {
      Native.free(pointer);
    }

    compressedContent.limit(compressedLength + 5);
    compressedContent.put(0, (byte) (-(recordId + 1)));
    compressedContent.putInt(1, contentSize);

    return new OPair<>(compressedContent, compressedPointer);
  }

  public OWriteableWALRecord fromStream(byte[] content) {
    if (content[0] < 0) {
      final int originalLen = OIntegerSerializer.INSTANCE.deserializeNative(content, 1);
      final byte[] restored = new byte[originalLen];

      final LZ4FastDecompressor decompressor = factory.fastDecompressor();
      decompressor.decompress(content, 5, restored, 1, restored.length - 1);
      restored[0] = (byte) (-content[0] - 1);
      content = restored;
    }

    final OWriteableWALRecord walRecord;
    switch (content[0]) {
    case UPDATE_PAGE_RECORD:
      walRecord = new OUpdatePageRecord();
      break;
    case FUZZY_CHECKPOINT_START_RECORD:
      walRecord = new OFuzzyCheckpointStartRecord();
      break;
    case FUZZY_CHECKPOINT_END_RECORD:
      walRecord = new OFuzzyCheckpointEndRecord();
      break;
    case FULL_CHECKPOINT_START_RECORD:
      walRecord = new OFullCheckpointStartRecord();
      break;
    case CHECKPOINT_END_RECORD:
      walRecord = new OCheckpointEndRecord();
      break;
    case ATOMIC_UNIT_START_RECORD:
      walRecord = new OAtomicUnitStartRecord();
      break;
    case ATOMIC_UNIT_END_RECORD:
      walRecord = new OAtomicUnitEndRecord();
      break;
    case FILE_CREATED_WAL_RECORD:
      walRecord = new OFileCreatedWALRecord();
      break;
    case NON_TX_OPERATION_PERFORMED_WAL_RECORD:
      walRecord = new ONonTxOperationPerformedWALRecord();
      break;
    case FILE_DELETED_WAL_RECORD:
      walRecord = new OFileDeletedWALRecord();
      break;
    case FILE_TRUNCATED_WAL_RECORD:
      walRecord = new OFileTruncatedWALRecord();
      break;
    case EMPTY_WAL_RECORD:
      walRecord = new OEmptyWALRecord();
      break;
    case ALLOCATE_POSITION_OPERATION:
      walRecord = new OAllocatePositionOperation();
      break;
    case CREATE_RECORD_OPERATION:
      walRecord = new OCreateRecordOperation();
      break;
    case DELETE_RECORD_OPERATION:
      walRecord = new ODeleteRecordOperation();
      break;
    case RECYCLE_RECORD_OPERATION:
      walRecord = new ORecycleRecordOperation();
      break;
    case UPDATE_RECORD_OPERATION:
      walRecord = new OUpdateRecordOperation();
      break;
    case HASH_TABLE_PUT_OPERATION:
      walRecord = new OHashTablePutOperation();
      break;
    case HASH_TABLE_REMOVE_OPERATION:
      walRecord = new OHashTableRemoveOperation();
      break;
    case SBTREE_PUT_OPERATION:
      walRecord = new OSBTreePutOperation();
      break;
    case SBTREE_REMOVE_OPERATION:
      walRecord = new OSBTreeRemoveOperation();
      break;
    case SBTREE_BONSAI_PUT_OPERATION:
      walRecord = new OSBTreeBonsaiPutOperation();
      break;
    case SBTREE_BONSAI_REMOVE_OPERATION:
      walRecord = new OSBTreeBonsaiRemoveOperation();
      break;
    case CREATE_CLUSTER_OPERATION:
      walRecord = new OCreateClusterOperation();
      break;
    case CREATE_HASH_TABLE_OPERATION:
      walRecord = new OCreateHashTableOperation();
      break;
    case CREATE_SBTREE_OPERATION:
      walRecord = new OCreateSBTreeOperation();
      break;
    case CREATE_SBTREE_BONSAI_OPERATION:
      walRecord = new OCreateSBTreeBonsaiOperation();
      break;
    case CREATE_SBTREE_BONSAI_RAW_OPERATION:
      walRecord = new OCreateSBTreeBonsaiRawOperation();
      break;
    case MAKE_POSITION_AVAILABLE_OPERATION:
      walRecord = new OMakePositionAvailableOperation();
      break;
    default:
      if (idToTypeMap.containsKey(content[0]))
        try {
          walRecord = (OWriteableWALRecord) idToTypeMap.get(content[0]).newInstance();
        } catch (final InstantiationException | IllegalAccessException e) {
          throw new IllegalStateException("Cannot deserialize passed in record", e);
        }
      else
        throw new IllegalStateException("Cannot deserialize passed in wal record.");
    }

    walRecord.fromStream(content, 1);

    return walRecord;
  }

  public void registerNewRecord(final byte id, final Class<? extends OWriteableWALRecord> type) {
    idToTypeMap.put(id, type);
  }
}
