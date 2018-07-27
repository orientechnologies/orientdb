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

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OEmptyWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OWriteableWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.OAllocatePositionOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.OCreateClusterOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.OCreateRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.ODeleteRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.OMakePositionAvailableOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.ORecycleRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.OUpdateRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpage.OClusterPageAddRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpage.OClusterPageDeleteRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpage.OClusterPageReplaceRecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpage.OClusterPageSetNextPagePointerOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpage.OClusterPageSetPrevPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpositionmapbucket.OClusterPositionMapBucketAddOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpositionmapbucket.OClusterPositionMapBucketAllocateOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpositionmapbucket.OClusterPositionMapBucketMakeAvailableOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpositionmapbucket.OClusterPositionMapBucketRemoveOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpositionmapbucket.OClusterPositionMapBucketResurrectOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpositionmapbucket.OClusterPositionMapBucketSetOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.paginatedclusterstate.OPaginatedClusterStateSetFreeListPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.paginatedclusterstate.OPaginatedClusterStateSetRecordSizeOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.paginatedclusterstate.OPaginatedClusterStateSetSizeOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable.OCreateHashTableOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable.OHashTablePutOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable.OHashTableRemoveOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.OCreateSBTreeOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.OSBTreePutOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.OSBTreeRemoveOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket.OSBTreeBucketAddAllOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket.OSBTreeBucketAddEntryOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket.OSBTreeBucketRemoveOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket.OSBTreeBucketSetLeftSiblingOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket.OSBTreeBucketSetRightSiblingOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket.OSBTreeBucketSetSizeOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket.OSBTreeBucketSetValueFreeListFirstIndexOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket.OSBTreeBucketShrinkOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket.OSBTreeBucketUpdateValueOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.OCreateSBTreeBonsaiOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.OCreateSBTreeBonsaiRawOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.OSBTreeBonsaiPutOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.OSBTreeBonsaiRemoveOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket.OSBTreeBonsaiBucketAddAllOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket.OSBTreeBonsaiBucketRemoveOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket.OSBTreeBonsaiBucketSetDeletedOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket.OSBTreeBonsaiBucketSetFreeListPointerOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket.OSBTreeBonsaiBucketSetLeftSiblingOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket.OSBTreeBonsaiBucketSetRightSiblingOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket.OSBTreeBonsaiBucketSetTreeSizeOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket.OSBTreeBonsaiBucketShrinkOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket.OSBTreeBonsaiBucketUpdateValueOperation;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.ALLOCATE_POSITION_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.ATOMIC_UNIT_END_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.ATOMIC_UNIT_START_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CHECKPOINT_END_RECORD;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_PAGE_ADD_RECORD_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_PAGE_DELETER_RECORD_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_PAGE_REPLACE_RECORD_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_PAGE_SET_NEXT_PAGE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_PAGE_SET_NEXT_PAGE_POINTER_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_PAGE_SET_PREV_PAGE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_BUCKET_ADD_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_BUCKET_ALLOCATE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_BUCKET_MAKE_AVAILABLE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_BUCKET_REMOVE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_BUCKET_RESURRECT_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.CLUSTER_POSITION_MAP_BUCKET_SET_OPERATION;
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
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.PAGINATED_CLUSTER_STATE_SET_FREE_LIST_PAGE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.PAGINATED_CLUSTER_STATE_SET_RECORD_SIZE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.PAGINATED_CLUSTER_STATE_SET_SIZE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.RECYCLE_RECORD_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_ADD_ALL_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_ADD_ENTRY_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_REMOVE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_DELETED_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_FREE_LIST_POINTER_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_LEFT_SIBLING_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_RIGHT_SIBLING_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_SET_SIZE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_SHRINK_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_BUCKET_UPDATE_VALUE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_PUT_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BONSAI_REMOVE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_ADD_ALL_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_ADD_ENTRY_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_REMOVE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_SET_LEFT_SIBLING_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_SET_RIGHT_SIBLING_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_SET_SIZE_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_SET_VALUE_FREE_LIST_FIRST_INDEX_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_SHRINK_OPERATION;
import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.SBTREE_BUCKET_UPDATE_VALUE_OPERATION;
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

  public static OPair<ByteBuffer, Long> toStream(final OWriteableWALRecord walRecord) {
    final int contentSize = walRecord.serializedSize() + 1;

    final long pointer = Native.malloc(contentSize);
    final ByteBuffer content = new Pointer(pointer).getByteBuffer(0, contentSize);

    final byte recordId = walRecord.getId();
    content.put(recordId);
    walRecord.toStream(content);

    return new OPair<>(content, pointer);
  }

  public OWriteableWALRecord fromStream(byte[] content) {
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
    case CLUSTER_PAGE_ADD_RECORD_OPERATION:
      walRecord = new OClusterPageAddRecordOperation();
      break;
    case CLUSTER_PAGE_DELETER_RECORD_OPERATION:
      walRecord = new OClusterPageDeleteRecordOperation();
      break;
    case CLUSTER_PAGE_REPLACE_RECORD_OPERATION:
      walRecord = new OClusterPageReplaceRecordOperation();
      break;
    case CLUSTER_PAGE_SET_NEXT_PAGE_OPERATION:
      walRecord = new OClusterPageSetNextPagePointerOperation();
      break;
    case CLUSTER_PAGE_SET_NEXT_PAGE_POINTER_OPERATION:
      walRecord = new OClusterPageSetNextPagePointerOperation();
      break;
    case CLUSTER_PAGE_SET_PREV_PAGE_OPERATION:
      walRecord = new OClusterPageSetPrevPageOperation();
      break;
    case CLUSTER_POSITION_MAP_BUCKET_ADD_OPERATION:
      walRecord = new OClusterPositionMapBucketAddOperation();
      break;

    case CLUSTER_POSITION_MAP_BUCKET_ALLOCATE_OPERATION:
      walRecord = new OClusterPositionMapBucketAllocateOperation();
      break;

    case CLUSTER_POSITION_MAP_BUCKET_SET_OPERATION:
      walRecord = new OClusterPositionMapBucketSetOperation();
      break;

    case CLUSTER_POSITION_MAP_BUCKET_RESURRECT_OPERATION:
      walRecord = new OClusterPositionMapBucketResurrectOperation();
      break;

    case CLUSTER_POSITION_MAP_BUCKET_MAKE_AVAILABLE_OPERATION:
      walRecord = new OClusterPositionMapBucketMakeAvailableOperation();
      break;

    case CLUSTER_POSITION_MAP_BUCKET_REMOVE_OPERATION:
      walRecord = new OClusterPositionMapBucketRemoveOperation();
      break;

    case PAGINATED_CLUSTER_STATE_SET_SIZE_OPERATION:
      walRecord = new OPaginatedClusterStateSetSizeOperation();
      break;

    case PAGINATED_CLUSTER_STATE_SET_RECORD_SIZE_OPERATION:
      walRecord = new OPaginatedClusterStateSetRecordSizeOperation();
      break;

    case PAGINATED_CLUSTER_STATE_SET_FREE_LIST_PAGE_OPERATION:
      walRecord = new OPaginatedClusterStateSetFreeListPageOperation();
      break;

    case SBTREE_BUCKET_SET_SIZE_OPERATION:
      walRecord = new OSBTreeBucketSetSizeOperation();
      break;

    case SBTREE_BUCKET_SET_VALUE_FREE_LIST_FIRST_INDEX_OPERATION:
      walRecord = new OSBTreeBucketSetValueFreeListFirstIndexOperation();
      break;

    case SBTREE_BUCKET_REMOVE_OPERATION:
      walRecord = new OSBTreeBucketRemoveOperation();
      break;

    case SBTREE_BUCKET_ADD_ALL_OPERATION:
      walRecord = new OSBTreeBucketAddAllOperation();
      break;

    case SBTREE_BUCKET_SHRINK_OPERATION:
      walRecord = new OSBTreeBucketShrinkOperation();
      break;

    case SBTREE_BUCKET_ADD_ENTRY_OPERATION:
      walRecord = new OSBTreeBucketAddEntryOperation();
      break;

    case SBTREE_BUCKET_UPDATE_VALUE_OPERATION:
      walRecord = new OSBTreeBucketUpdateValueOperation();
      break;

    case SBTREE_BUCKET_SET_LEFT_SIBLING_OPERATION:
      walRecord = new OSBTreeBucketSetLeftSiblingOperation();
      break;

    case SBTREE_BUCKET_SET_RIGHT_SIBLING_OPERATION:
      walRecord = new OSBTreeBucketSetRightSiblingOperation();
      break;

    case SBTREE_BONSAI_BUCKET_SET_SIZE_OPERATION:
      walRecord = new OSBTreeBonsaiBucketSetTreeSizeOperation();
      break;

    case SBTREE_BONSAI_BUCKET_REMOVE_OPERATION:
      walRecord = new OSBTreeBonsaiBucketRemoveOperation();
      break;

    case SBTREE_BONSAI_BUCKET_ADD_ALL_OPERATION:
      walRecord = new OSBTreeBonsaiBucketAddAllOperation();
      break;

    case SBTREE_BONSAI_BUCKET_SHRINK_OPERATION:
      walRecord = new OSBTreeBonsaiBucketShrinkOperation();
      break;

    case SBTREE_BONSAI_BUCKET_ADD_ENTRY_OPERATION:
      walRecord = new OSBTreeBucketAddEntryOperation();
      break;

    case SBTREE_BONSAI_BUCKET_UPDATE_VALUE_OPERATION:
      walRecord = new OSBTreeBonsaiBucketUpdateValueOperation();
      break;

    case SBTREE_BONSAI_BUCKET_SET_FREE_LIST_POINTER_OPERATION:
      walRecord = new OSBTreeBonsaiBucketSetFreeListPointerOperation();
      break;

    case SBTREE_BONSAI_BUCKET_SET_DELETED_OPERATION:
      walRecord = new OSBTreeBonsaiBucketSetDeletedOperation();
      break;

    case SBTREE_BONSAI_BUCKET_SET_LEFT_SIBLING_OPERATION:
      walRecord = new OSBTreeBonsaiBucketSetLeftSiblingOperation();
      break;

    case SBTREE_BONSAI_BUCKET_SET_RIGHT_SIBLING_OPERATION:
      walRecord = new OSBTreeBonsaiBucketSetRightSiblingOperation();
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
