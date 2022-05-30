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

import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.*;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.EmptyWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.WriteableWALRecord;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 25.04.13
 */
public final class OWALRecordsFactory {
  private static final int RECORD_ID_OFFSET = 0;
  private static final int RECORD_ID_SIZE = 2;

  private static final int OPERATION_ID_OFFSET = RECORD_ID_OFFSET + RECORD_ID_SIZE;
  private static final int OPERATION_ID_SIZE = 4;

  private static final int ORIGINAL_CONTENT_SIZE_OFFSET = OPERATION_ID_OFFSET + OPERATION_ID_SIZE;
  private static final int ORIGINAL_CONTENT_SIZE = 4;

  private static final int METADATA_SIZE = RECORD_ID_SIZE + OPERATION_ID_SIZE;
  private static final int COMPRESSED_METADATA_SIZE =
      RECORD_ID_SIZE + OPERATION_ID_SIZE + ORIGINAL_CONTENT_SIZE;

  private final Map<Integer, Class<?>> idToTypeMap = new HashMap<>();

  public static final OWALRecordsFactory INSTANCE = new OWALRecordsFactory();

  private static final LZ4Factory factory = LZ4Factory.fastestInstance();
  private static final int MIN_COMPRESSED_RECORD_SIZE =
      OGlobalConfiguration.WAL_MIN_COMPRESSED_RECORD_SIZE.getValueAsInteger();

  public static ByteBuffer toStream(final WriteableWALRecord walRecord) {
    final int contentSize = walRecord.serializedSize() + METADATA_SIZE;

    final ByteBuffer content = ByteBuffer.allocate(contentSize).order(ByteOrder.nativeOrder());

    final int recordId = walRecord.getId();
    content.putShort(RECORD_ID_OFFSET, (short) recordId);
    content.position(METADATA_SIZE);

    walRecord.toStream(content);

    if (MIN_COMPRESSED_RECORD_SIZE <= 0 || contentSize < MIN_COMPRESSED_RECORD_SIZE) {
      return content;
    }

    final LZ4Compressor compressor = factory.fastCompressor();
    final int maxCompressedLength = compressor.maxCompressedLength(contentSize - 1);

    final ByteBuffer compressedContent =
        ByteBuffer.allocate(maxCompressedLength + COMPRESSED_METADATA_SIZE)
            .order(ByteOrder.nativeOrder());

    content.position(OPERATION_ID_OFFSET + OPERATION_ID_SIZE);
    compressedContent.position(COMPRESSED_METADATA_SIZE);
    final int compressedLength =
        compressor.compress(
            content,
            METADATA_SIZE,
            contentSize - METADATA_SIZE,
            compressedContent,
            COMPRESSED_METADATA_SIZE,
            maxCompressedLength);

    if (compressedLength + COMPRESSED_METADATA_SIZE < contentSize) {
      compressedContent.limit(compressedLength + COMPRESSED_METADATA_SIZE);
      compressedContent.putShort(RECORD_ID_OFFSET, (short) (-(recordId + 1)));
      compressedContent.putInt(ORIGINAL_CONTENT_SIZE_OFFSET, contentSize);

      return compressedContent;
    } else {
      return content;
    }
  }

  public WriteableWALRecord fromStream(byte[] content) {
    int recordId = OShortSerializer.INSTANCE.deserializeNative(content, RECORD_ID_OFFSET);

    if (recordId < 0) {
      final int originalLen =
          OIntegerSerializer.INSTANCE.deserializeNative(content, ORIGINAL_CONTENT_SIZE_OFFSET);
      final byte[] restored = new byte[originalLen];

      final LZ4FastDecompressor decompressor = factory.fastDecompressor();
      decompressor.decompress(
          content,
          COMPRESSED_METADATA_SIZE,
          restored,
          METADATA_SIZE,
          restored.length - METADATA_SIZE);
      recordId = -recordId - 1;
      content = restored;
    }

    final WriteableWALRecord walRecord = walRecordById(recordId);

    walRecord.fromStream(content, METADATA_SIZE);

    if (walRecord.getId() != recordId) {
      throw new IllegalStateException(
          "Deserialized WAL record id does not match to the serialized record id "
              + walRecord.getId()
              + " - "
              + content[0]);
    }

    return walRecord;
  }

  private WriteableWALRecord walRecordById(final int recordId) {
    final WriteableWALRecord walRecord;
    switch (recordId) {
      case UPDATE_PAGE_RECORD:
        walRecord = new OUpdatePageRecord();
        break;
      case HIGH_LEVEL_TRANSACTION_CHANGE_RECORD:
        walRecord = new OHighLevelTransactionChangeRecord();
        break;
      case ATOMIC_UNIT_START_RECORD:
        walRecord = new OAtomicUnitStartRecord();
        break;
      case ATOMIC_UNIT_START_METADATA_RECORD:
        walRecord = new OAtomicUnitStartMetadataRecord();
        break;
      case ATOMIC_UNIT_END_RECORD:
        walRecord = new OAtomicUnitEndRecord();
        break;
      case ATOMIC_UNIT_END_RECORD_WITH_PAGE_LSNS:
        walRecord = new AtomicUnitEndRecordWithPageLSNs();
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
      case EMPTY_WAL_RECORD:
        walRecord = new EmptyWALRecord();
        break;
      case CLUSTER_POSITION_MAP_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CLUSTER_POSITION_MAP_ADD_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CLUSTER_POSITION_MAP_ALLOCATE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CLUSTER_POSITION_MAP_TRUNCATE_LAST_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CLUSTER_POSITION_MAP_UPDATE_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CLUSTER_POSITION_MAP_UPDATE_STATUS_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CLUSTER_PAGE_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CLUSTER_PAGE_APPEND_RECORD_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CLUSTER_PAGE_REPLACE_RECORD_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CLUSTER_PAGE_DELETE_RECORD_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CLUSTER_PAGE_SET_NEXT_PAGE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CLUSTER_PAGE_SET_PREV_PAGE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CLUSTER_PAGE_SET_RECORD_LONG_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case PAGINATED_CLUSTER_STATE_V0_SET_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case PAGINATED_CLUSTER_STATE_V0_SET_RECORDS_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case PAGINATED_CLUSTER_STATE_V0_SET_FREE_LIST_PAGE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case PAGINATED_CLUSTER_STATE_V1_SET_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case PAGINATED_CLUSTER_STATE_V1_SET_RECORDS_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case PAGINATED_CLUSTER_STATE_V1_SET_FREE_LIST_PAGE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case PAGINATED_CLUSTER_STATE_V1_SET_FILE_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case PAGINATED_CLUSTER_STATE_V2_SET_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case PAGINATED_CLUSTER_STATE_V2_SET_RECORDS_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case PAGINATED_CLUSTER_STATE_V2_SET_FREE_LIST_PAGE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case PAGINATED_CLUSTER_STATE_V2_SET_FILE_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_ADD_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_ADD_NON_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_REMOVE_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_REMOVE_NON_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_ADD_ALL_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_SHRINK_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_UPDATE_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_SET_LEFT_SIBLING_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_SET_RIGHT_SIBLING_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V1_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V1_SET_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V1_REMOVE_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V1_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V1_SET_TREE_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V1_SET_PAGES_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case WALRecordTypes.CELL_BTREE_BUCKET_SINGLE_VALUE_V1_SWITCH_BUCKET_TYPE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_ADD_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_ADD_NON_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_REMOVE_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_ADD_ALL_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_SHRINK_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_UPDATE_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_REMOVE_NON_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_SET_LEFT_SIBLING_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_SET_RIGHT_SIBLING_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V3_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V3_SET_PAGES_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V3_SET_TREE_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V3_SET_FREE_LIST_HEAD_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_SET_NEXT_FREE_LIST_PAGE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V3_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V3_SET_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V3_REMOVE_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case WALRecordTypes.CELL_BTREE_BUCKET_SINGLE_VALUE_V3_SWITCH_BUCKET_TYPE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V1_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V1_ADD_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case WALRecordTypes.SBTREE_BUCKET_V1_ADD_NON_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V1_REMOVE_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V1_REMOVE_NON_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V1_ADD_ALL_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V1_SHRINK_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V1_UPDATE_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V1_SWITCH_BUCKET_TYPE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V1_SET_LEFT_SIBLING_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V1_SET_RIGHT_SIBLING_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_NULL_BUCKET_V1_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_NULL_BUCKET_V1_SET_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_NULL_BUCKET_V1_REMOVE_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V2_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V2_ADD_ALL_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V2_ADD_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case WALRecordTypes.SBTREE_BUCKET_V2_ADD_NON_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V2_REMOVE_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V2_REMOVE_NON_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V2_SET_LEFT_SIBLING_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V2_SET_RIGHT_SIBLING_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V2_SHRINK_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V2_SWITCH_BUCKET_TYPE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V2_UPDATE_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V1_SET_TREE_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_BUCKET_V2_SET_TREE_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_NULL_BUCKET_V2_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_NULL_BUCKET_V2_REMOVE_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case SBTREE_NULL_BUCKET_V2_SET_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_CREATE_MAIN_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_REMOVE_MAIN_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_APPEND_NEW_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_REMOVE_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_INCREMENT_ENTRIES_COUNT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_DECREMENT_ENTRIES_COUNT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_ADD_NON_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_REMOVE_NON_LEAF_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_ADD_ALL_LEAF_ENTRIES_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_ADD_ALL_NON_LEAF_ENTRIES_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_SHRINK_LEAF_ENTRIES_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_SHRINK_NON_LEAF_ENTRIES_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_SET_LEFT_SIBLING_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_SET_RIGHT_SIBLING_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_NULL_BUCKET_MULTI_VALUE_V2_SET_RIGHT_SIBLING_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_NULL_BUCKET_MULTI_VALUE_V2_ADD_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_NULL_BUCKET_MULTI_VALUE_V2_INCREMENT_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_NULL_BUCKET_MULTI_VALUE_V2_DECREMENT_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_NULL_BUCKET_MULTI_VALUE_V2_REMOVE_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_ENTRY_POINT_MULTI_VALUE_V2_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_ENTRY_POINT_MULTI_VALUE_V2_SET_TREE_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_ENTRY_POINT_MULTI_VALUE_V2_SET_PAGES_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_ENTRY_POINT_MULTI_VALUE_V2_SET_ENTRY_ID_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_SWITCH_BUCKET_TYPE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_BUCKET_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_BUCKET_UPDATE_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_BUCKET_DELETE_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_BUCKET_ADD_ENTRY_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_BUCKET_SET_DEPTH_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_DIRECTORY_PAGE_SET_MAX_LEFT_CHILDREN_DEPTH_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_DIRECTORY_PAGE_SET_MAX_RIGHT_CHILDREN_DEPTH_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_DIRECTORY_PAGE_SET_NODE_LOCAL_DEPTH_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_DIRECTORY_PAGE_SET_POINTER_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_DIRECTORY_FIRST_PAGE_SET_TREE_SIZE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_DIRECTORY_FIRST_PAGE_SET_TOMBSTONE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_METADATA_PAGE_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_METADATA_PAGE_SET_RECORDS_COUNT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_NULL_BUCKET_INIT_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_NULL_BUCKET_SET_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_NULL_BUCKET_REMOVE_VALUE_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_DIRECTORY_FIRST_PAGE_SET_MAX_LEFT_CHILDREN_DEPTH_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_DIRECTORY_FIRST_PAGE_SET_MAX_RIGHT_CHILDREN_DEPTH_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_DIRECTORY_FIRST_PAGE_SET_NODE_LOCAL_DEPTH_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case LOCAL_HASH_TABLE_V2_DIRECTORY_FIRST_PAGE_SET_POINTER_PO:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case TX_METADATA:
        walRecord = new MetaDataRecord();
        break;
      case FREE_SPACE_MAP_INIT:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      case FREE_SPACE_MAP_UPDATE:
        throw new IllegalStateException(
            "Cannot deserialize passed in wal record not exists anymore.");
      default:
        if (idToTypeMap.containsKey(recordId))
          try {
            walRecord =
                (WriteableWALRecord)
                    idToTypeMap.get(recordId).getDeclaredConstructor().newInstance();
          } catch (final InstantiationException
              | NoSuchMethodException
              | InvocationTargetException
              | IllegalAccessException e) {
            throw new IllegalStateException("Cannot deserialize passed in record", e);
          }
        else throw new IllegalStateException("Cannot deserialize passed in wal record.");
    }
    return walRecord;
  }

  public void registerNewRecord(final int id, final Class<? extends WriteableWALRecord> type) {
    idToTypeMap.put(id, type);
  }
}
