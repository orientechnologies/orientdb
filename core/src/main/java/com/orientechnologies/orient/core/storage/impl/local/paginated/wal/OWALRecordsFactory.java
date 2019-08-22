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
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OEmptyWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OWriteableWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreemultivalue.OCellBTreeMultiValuePutCO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreemultivalue.OCellBtreeMultiValueRemoveEntryCO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreesinglevalue.OCellBTreeSingleValuePutCO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreesinglevalue.OCellBTreeSingleValueRemoveCO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.indexengine.OIndexEngineCreateCO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.indexengine.OIndexEngineDeleteCO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.localhashtable.OLocalHashTablePutCO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.localhashtable.OLocalHashTableRemoveCO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.paginatedcluster.*;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.sbtree.OSBTreePutCO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.sbtree.OSBTreeRemoveCO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.sbtreebonsai.OSBTreeBonsaiCreateCO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.sbtreebonsai.OSBTreeBonsaiCreateComponentCO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.sbtreebonsai.OSBTreeBonsaiDeleteCO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.cellbtreebucketsinglevalue.*;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage.*;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpositionmapbucket.*;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v0.paginatedclusterstate.PaginatedClusterStateV0SetFreeListPagePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v0.paginatedclusterstate.PaginatedClusterStateV0SetRecordsSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v0.paginatedclusterstate.PaginatedClusterStateV0SetSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v1.paginatedclusterstate.PaginatedClusterStateV1SetFileSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v1.paginatedclusterstate.PaginatedClusterStateV1SetFreeListPagePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v1.paginatedclusterstate.PaginatedClusterStateV1SetRecordsSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v1.paginatedclusterstate.PaginatedClusterStateV1SetSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.paginatedclusterstate.PaginatedClusterStateV2SetFileSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.paginatedclusterstate.PaginatedClusterStateV2SetFreeListPagePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.paginatedclusterstate.PaginatedClusterStateV2SetRecordsSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.paginatedclusterstate.PaginatedClusterStateV2SetSizePO;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.*;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 25.04.13
 */
public final class OWALRecordsFactory {
  private final Map<Byte, Class> idToTypeMap = new HashMap<>();

  public static final OWALRecordsFactory INSTANCE = new OWALRecordsFactory();

  private static final LZ4Factory factory                    = LZ4Factory.fastestInstance();
  private static final int        MIN_COMPRESSED_RECORD_SIZE = 8 * 1024;

  public static OPair<ByteBuffer, Long> toStream(final OWriteableWALRecord walRecord) {
    final int contentSize = walRecord.serializedSize() + 1;

    final ByteBuffer content = ByteBuffer.allocate(contentSize).order(ByteOrder.nativeOrder());

    final byte recordId = walRecord.getId();
    content.put(recordId);
    walRecord.toStream(content);

    if (MIN_COMPRESSED_RECORD_SIZE <= 0 || contentSize < MIN_COMPRESSED_RECORD_SIZE) {
      return new OPair<>(content, 0L);
    }

    final LZ4Compressor compressor = factory.fastCompressor();
    final int maxCompressedLength = compressor.maxCompressedLength(contentSize - 1);

    final ByteBuffer compressedContent = ByteBuffer.allocate(maxCompressedLength + 5).order(ByteOrder.nativeOrder());

    content.position(1);
    compressedContent.position(5);
    final int compressedLength = compressor.compress(content, 1, contentSize - 1, compressedContent, 5, maxCompressedLength);

    if (compressedLength + 5 < contentSize) {
      compressedContent.limit(compressedLength + 5);
      compressedContent.put(0, (byte) (-(recordId + 1)));
      compressedContent.putInt(1, contentSize);

      return new OPair<>(compressedContent, 0L);
    } else {
      return new OPair<>(content, 0L);
    }
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
    case EMPTY_WAL_RECORD:
      walRecord = new OEmptyWALRecord();
      break;
    case CREATE_CLUSTER_CO:
      walRecord = new OPaginatedClusterCreateCO();
      break;
    case DELETE_CLUSTER_CO:
      walRecord = new OPaginatedClusterDeleteCO();
      break;
    case CLUSTER_CREATE_RECORD_CO:
      walRecord = new OPaginatedClusterCreateRecordCO();
      break;
    case CLUSTER_DELETE_RECORD_CO:
      walRecord = new OPaginatedClusterDeleteRecordCO();
      break;
    case CLUSTER_ALLOCATE_RECORD_POSITION_CO:
      walRecord = new OPaginatedClusterAllocatePositionCO();
      break;
    case CLUSTER_UPDATE_RECORD_CO:
      walRecord = new OPaginatedClusterUpdateRecordCO();
      break;
    case INDEX_ENGINE_CREATE_CO:
      walRecord = new OIndexEngineCreateCO();
      break;
    case INDEX_ENGINE_DELETE_CO:
      walRecord = new OIndexEngineDeleteCO();
      break;
    case CELL_BTREE_SINGLE_VALUE_PUT_CO:
      walRecord = new OCellBTreeSingleValuePutCO();
      break;
    case CELL_BTREE_SINGLE_VALUE_REMOVE_CO:
      walRecord = new OCellBTreeSingleValueRemoveCO();
      break;
    case CELL_BTREE_MULTI_VALUE_PUT_CO:
      walRecord = new OCellBTreeMultiValuePutCO();
      break;
    case CELL_BTREE_MULTI_VALUE_REMOVE_ENTRY_CO:
      walRecord = new OCellBtreeMultiValueRemoveEntryCO();
      break;
    case SBTREE_PUT_CO:
      walRecord = new OSBTreePutCO();
      break;
    case SBTREE_REMOVE_CO:
      walRecord = new OSBTreeRemoveCO();
      break;
    case LOCAL_HASHTABLE_PUT_CO:
      walRecord = new OLocalHashTablePutCO();
      break;
    case LOCAL_HASHTABLE_REMOVE_CO:
      walRecord = new OLocalHashTableRemoveCO();
      break;
    case SBTREE_BONSAI_CREATE_COMPONENT_CO:
      walRecord = new OSBTreeBonsaiCreateComponentCO();
      break;
    case SBTREE_BONSAI_CREATE_CO:
      walRecord = new OSBTreeBonsaiCreateCO();
      break;
    case SBTREE_BONSAI_DELETE_COMPONENT_CO:
      walRecord = new com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.sbteebonsai.OSBTreeBonsaiDeleteComponentCO();
      break;
    case SBTREE_BONSAI_DELETE_CO:
      walRecord = new OSBTreeBonsaiDeleteCO();
      break;
    case CLUSTER_POSITION_MAP_INIT_PO:
      walRecord = new ClusterPositionMapBucketInitPO();
      break;
    case CLUSTER_POSITION_MAP_ADD_PO:
      walRecord = new ClusterPositionMapBucketAddPO();
      break;
    case CLUSTER_POSITION_MAP_ALLOCATE_PO:
      walRecord = new ClusterPositionMapBucketAllocatePO();
      break;
    case CLUSTER_POSITION_MAP_TRUNCATE_LAST_ENTRY_PO:
      walRecord = new ClusterPositionMapBucketTruncateLastEntryPO();
      break;
    case CLUSTER_POSITION_MAP_UPDATE_ENTRY_PO:
      walRecord = new ClusterPositionMapBucketUpdateEntryPO();
      break;
    case CLUSTER_POSITION_MAP_UPDATE_STATUS_PO:
      walRecord = new ClusterPositionMapBucketUpdateStatusPO();
      break;
    case CLUSTER_PAGE_INIT_PO:
      walRecord = new ClusterPageInitPO();
      break;
    case CLUSTER_PAGE_APPEND_RECORD_PO:
      walRecord = new ClusterPageAppendRecordPO();
      break;
    case CLUSTER_PAGE_REPLACE_RECORD_PO:
      walRecord = new ClusterPageReplaceRecordPO();
      break;
    case CLUSTER_PAGE_DELETE_RECORD_PO:
      walRecord = new ClusterPageDeleteRecordPO();
      break;
    case CLUSTER_PAGE_SET_NEXT_PAGE_PO:
      walRecord = new ClusterPageSetNextPagePO();
      break;
    case CLUSTER_PAGE_SET_PREV_PAGE_PO:
      walRecord = new ClusterPageSetPrevPagePO();
      break;
    case CLUSTER_PAGE_SET_RECORD_LONG_VALUE_PO:
      walRecord = new ClusterPageSetRecordLongValuePO();
      break;
    case PAGINATED_CLUSTER_STATE_V0_SET_SIZE_PO:
      walRecord = new PaginatedClusterStateV0SetSizePO();
      break;
    case PAGINATED_CLUSTER_STATE_V0_SET_RECORDS_SIZE_PO:
      walRecord = new PaginatedClusterStateV0SetRecordsSizePO();
      break;
    case PAGINATED_CLUSTER_STATE_V0_SET_FREE_LIST_PAGE_PO:
      walRecord = new PaginatedClusterStateV0SetFreeListPagePO();
      break;
    case PAGINATED_CLUSTER_STATE_V1_SET_SIZE_PO:
      walRecord = new PaginatedClusterStateV1SetSizePO();
      break;
    case PAGINATED_CLUSTER_STATE_V1_SET_RECORDS_SIZE_PO:
      walRecord = new PaginatedClusterStateV1SetRecordsSizePO();
      break;
    case PAGINATED_CLUSTER_STATE_V1_SET_FREE_LIST_PAGE_PO:
      walRecord = new PaginatedClusterStateV1SetFreeListPagePO();
      break;
    case PAGINATED_CLUSTER_STATE_V1_SET_FILE_SIZE_PO:
      walRecord = new PaginatedClusterStateV1SetFileSizePO();
      break;
    case PAGINATED_CLUSTER_STATE_V2_SET_SIZE_PO:
      walRecord = new PaginatedClusterStateV2SetSizePO();
      break;
    case PAGINATED_CLUSTER_STATE_V2_SET_RECORDS_SIZE_PO:
      walRecord = new PaginatedClusterStateV2SetRecordsSizePO();
      break;
    case PAGINATED_CLUSTER_STATE_V2_SET_FREE_LIST_PAGE_PO:
      walRecord = new PaginatedClusterStateV2SetFreeListPagePO();
      break;
    case PAGINATED_CLUSTER_STATE_V2_SET_FILE_SIZE_PO:
      walRecord = new PaginatedClusterStateV2SetFileSizePO();
      break;
    case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_INIT_PO:
      walRecord = new CellBTreeBucketSingleValueV1InitPO();
      break;
    case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_ADD_LEAF_ENTRY_PO:
      walRecord = new CellBTreeBucketSingleValueV1AddLeafEntryPO();
      break;
    case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_ADD_NON_LEAF_ENTRY_PO:
      walRecord = new CellBTreeBucketSingleValueV1AddNonLeafEntryPO();
      break;
    case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_REMOVE_LEAF_ENTRY_PO:
      walRecord = new CellBTreeBucketSingleValueV1RemoveLeafEntryPO();
      break;
    case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_REMOVE_NON_LEAF_ENTRY_PO:
      walRecord = new CellBTreeBucketSingleValueV1RemoveNonLeafEntryPO();
      break;
    case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_ADD_ALL_PO:
      walRecord = new CellBTreeBucketSingleValueV1AddAllPO();
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

    if (walRecord.getId() != content[0]) {
      throw new IllegalStateException(
          "Deserialized WAL record id does not match to the serialized record id " + walRecord.getId() + " - " + content[0]);
    }

    return walRecord;
  }

  public void registerNewRecord(final byte id, final Class<? extends OWriteableWALRecord> type) {
    idToTypeMap.put(id, type);
  }
}
