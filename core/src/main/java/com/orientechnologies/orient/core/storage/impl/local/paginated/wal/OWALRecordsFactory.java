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
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketAddAllLeafEntriesPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketAddAllNonLeafEntriesPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketAddNonLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketAppendNewLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketCreateMainLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketDecrementEntriesCountPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketIncrementEntriesCountPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketInitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketRemoveLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketRemoveMainLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketRemoveNonLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketSetLeftSiblingPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketSetRightSiblingPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketShrinkLeafEntriesPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketShrinkNonLeafEntriesPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket.CellBTreeMultiValueV2BucketSwitchBucketTypePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.entrypoint.CellBTreeMultiValueV2EntryPointInitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.entrypoint.CellBTreeMultiValueV2EntryPointSetEntryIdPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.entrypoint.CellBTreeMultiValueV2EntryPointSetPagesSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.entrypoint.CellBTreeMultiValueV2EntryPointSetTreeSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.nullbucket.CellBTreeMultiValueV2NullBucketAddValuePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.nullbucket.CellBTreeMultiValueV2NullBucketDecrementSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.nullbucket.CellBTreeMultiValueV2NullBucketIncrementSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.nullbucket.CellBTreeMultiValueV2NullBucketInitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.nullbucket.CellBTreeMultiValueV2NullBucketRemoveValuePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.bucket.CellBTreeBucketSingleValueV1AddAllPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.bucket.CellBTreeBucketSingleValueV1AddLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.bucket.CellBTreeBucketSingleValueV1AddNonLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.bucket.CellBTreeBucketSingleValueV1InitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.bucket.CellBTreeBucketSingleValueV1RemoveLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.bucket.CellBTreeBucketSingleValueV1RemoveNonLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.bucket.CellBTreeBucketSingleValueV1SetLeftSiblingPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.bucket.CellBTreeBucketSingleValueV1SetRightSiblingPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.bucket.CellBTreeBucketSingleValueV1ShrinkPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.bucket.CellBTreeBucketSingleValueV1SwitchBucketTypePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.bucket.CellBTreeBucketSingleValueV1UpdateValuePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.entrypoint.CellBTreeEntryPointSingleValueV1InitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.entrypoint.CellBTreeEntryPointSingleValueV1SetPagesSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.entrypoint.CellBTreeEntryPointSingleValueV1SetTreeSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.nullbucket.CellBTreeNullBucketSingleValueV1InitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.nullbucket.CellBTreeNullBucketSingleValueV1RemoveValuePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.nullbucket.CellBTreeNullBucketSingleValueV1SetValuePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.bucket.*;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.entrypoint.CellBTreeEntryPointSingleValueV3InitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.entrypoint.CellBTreeEntryPointSingleValueV3SetPagesSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.entrypoint.CellBTreeEntryPointSingleValueV3SetTreeSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.entrypoint.CellBTreeSingleValueEntryPointV3SetFreeListHeadPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.nullbucket.CellBTreeNullBucketSingleValueV3InitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.nullbucket.CellBTreeNullBucketSingleValueV3RemoveValuePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.nullbucket.CellBTreeNullBucketSingleValueV3SetValuePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage.ClusterPageAppendRecordPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage.ClusterPageDeleteRecordPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage.ClusterPageInitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage.ClusterPageReplaceRecordPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage.ClusterPageSetNextPagePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage.ClusterPageSetPrevPagePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage.ClusterPageSetRecordLongValuePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpositionmapbucket.ClusterPositionMapBucketAddPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpositionmapbucket.ClusterPositionMapBucketAllocatePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpositionmapbucket.ClusterPositionMapBucketInitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpositionmapbucket.ClusterPositionMapBucketTruncateLastEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpositionmapbucket.ClusterPositionMapBucketUpdateEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpositionmapbucket.ClusterPositionMapBucketUpdateStatusPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v0.paginatedclusterstate.PaginatedClusterStateV0SetFreeListPagePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v0.paginatedclusterstate.PaginatedClusterStateV0SetRecordsSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v0.paginatedclusterstate.PaginatedClusterStateV0SetSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v1.paginatedclusterstate.PaginatedClusterStateV1SetFileSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v1.paginatedclusterstate.PaginatedClusterStateV1SetFreeListPagePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v1.paginatedclusterstate.PaginatedClusterStateV1SetRecordsSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v1.paginatedclusterstate.PaginatedClusterStateV1SetSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.freespacemap.InitFreeSpaceMapPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.freespacemap.UpdateMaxFreeSpacePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.paginatedclusterstate.PaginatedClusterStateV2SetFileSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.paginatedclusterstate.PaginatedClusterStateV2SetFreeListPagePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.paginatedclusterstate.PaginatedClusterStateV2SetRecordsSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.paginatedclusterstate.PaginatedClusterStateV2SetSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.bucket.LocalHashTableV2BucketAddEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.bucket.LocalHashTableV2BucketDeleteEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.bucket.LocalHashTableV2BucketInitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.bucket.LocalHashTableV2BucketSetDepthPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.bucket.LocalHashTableV2BucketUpdateEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directoryfirstpage.LocalHashTableV2DirectoryFirstPageSetMaxLeftChildDepthPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directoryfirstpage.LocalHashTableV2DirectoryFirstPageSetMaxRightChildDepthPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directoryfirstpage.LocalHashTableV2DirectoryFirstPageSetNodeLocalDepthPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directoryfirstpage.LocalHashTableV2DirectoryFirstPageSetPointerPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directoryfirstpage.LocalHashTableV2DirectoryFirstPageSetTombstonePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directoryfirstpage.LocalHashTableV2DirectoryFirstPageSetTreeSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directorypage.LocalHashTableV2DirectoryPageSetMaxLeftChildDepthPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directorypage.LocalHashTableV2DirectoryPageSetMaxRightChildDepthPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directorypage.LocalHashTableV2DirectoryPageSetNodeLocalDepthPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directorypage.LocalHashTableV2DirectoryPageSetPointerPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.metadatapage.LocalHashTableV2MetadataPageInitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.metadatapage.LocalHashTableV2MetadataPageSetRecordsCountPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.nullbucket.LocalHashTableV2NullBucketInitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.nullbucket.LocalHashTableV2NullBucketRemoveValuePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.nullbucket.LocalHashTableV2NullBucketSetValuePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket.SBTreeBucketV1AddAllPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket.SBTreeBucketV1AddLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket.SBTreeBucketV1AddNonLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket.SBTreeBucketV1InitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket.SBTreeBucketV1RemoveLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket.SBTreeBucketV1RemoveNonLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket.SBTreeBucketV1SetLeftSiblingPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket.SBTreeBucketV1SetRightSiblingPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket.SBTreeBucketV1SetTreeSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket.SBTreeBucketV1ShrinkPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket.SBTreeBucketV1SwitchBucketTypePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket.SBTreeBucketV1UpdateValuePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.nullbucket.SBTreeNullBucketV1InitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.nullbucket.SBTreeNullBucketV1RemoveValuePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.nullbucket.SBTreeNullBucketV1SetValuePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket.SBTreeBucketV2AddAllPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket.SBTreeBucketV2AddLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket.SBTreeBucketV2AddNonLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket.SBTreeBucketV2InitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket.SBTreeBucketV2RemoveLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket.SBTreeBucketV2RemoveNonLeafEntryPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket.SBTreeBucketV2SetLeftSiblingPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket.SBTreeBucketV2SetRightSiblingPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket.SBTreeBucketV2SetTreeSizePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket.SBTreeBucketV2ShrinkPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket.SBTreeBucketV2SwitchBucketTypePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket.SBTreeBucketV2UpdateValuePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.nullbucket.SBTreeNullBucketV2InitPO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.nullbucket.SBTreeNullBucketV2RemoveValuePO;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.nullbucket.SBTreeNullBucketV2SetValuePO;
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

  public static void serializeRecordId(final ByteBuffer buffer, final int operationId) {
    buffer.putInt(OPERATION_ID_OFFSET, operationId);
  }

  public WriteableWALRecord fromStream(byte[] content) {
    int recordId = OShortSerializer.INSTANCE.deserializeNative(content, RECORD_ID_OFFSET);
    int operationId = OIntegerSerializer.INSTANCE.deserializeNative(content, OPERATION_ID_OFFSET);

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
    walRecord.setOperationIdLsn(null, operationId);

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
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_SHRINK_PO:
        walRecord = new CellBTreeBucketSingleValueV1ShrinkPO();
        break;
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_UPDATE_VALUE_PO:
        walRecord = new CellBTreeBucketSingleValueV1UpdateValuePO();
        break;
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_SET_LEFT_SIBLING_PO:
        walRecord = new CellBTreeBucketSingleValueV1SetLeftSiblingPO();
        break;
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V1_SET_RIGHT_SIBLING_PO:
        walRecord = new CellBTreeBucketSingleValueV1SetRightSiblingPO();
        break;
      case CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V1_INIT_PO:
        walRecord = new CellBTreeNullBucketSingleValueV1InitPO();
        break;
      case CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V1_SET_VALUE_PO:
        walRecord = new CellBTreeNullBucketSingleValueV1SetValuePO();
        break;
      case CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V1_REMOVE_VALUE_PO:
        walRecord = new CellBTreeNullBucketSingleValueV1RemoveValuePO();
        break;
      case CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V1_INIT_PO:
        walRecord = new CellBTreeEntryPointSingleValueV1InitPO();
        break;
      case CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V1_SET_TREE_SIZE_PO:
        walRecord = new CellBTreeEntryPointSingleValueV1SetTreeSizePO();
        break;
      case CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V1_SET_PAGES_SIZE_PO:
        walRecord = new CellBTreeEntryPointSingleValueV1SetPagesSizePO();
        break;
      case WALRecordTypes.CELL_BTREE_BUCKET_SINGLE_VALUE_V1_SWITCH_BUCKET_TYPE_PO:
        walRecord = new CellBTreeBucketSingleValueV1SwitchBucketTypePO();
        break;
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_INIT_PO:
        walRecord = new CellBTreeBucketSingleValueV3InitPO();
        break;
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_ADD_LEAF_ENTRY_PO:
        walRecord = new CellBTreeBucketSingleValueV3AddLeafEntryPO();
        break;
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_ADD_NON_LEAF_ENTRY_PO:
        walRecord = new CellBTreeBucketSingleValueV3AddNonLeafEntryPO();
        break;
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_REMOVE_LEAF_ENTRY_PO:
        walRecord = new CellBTreeBucketSingleValueV3RemoveLeafEntryPO();
        break;
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_ADD_ALL_PO:
        walRecord = new CellBTreeBucketSingleValueV3AddAllPO();
        break;
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_SHRINK_PO:
        walRecord = new CellBTreeBucketSingleValueV3ShrinkPO();
        break;
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_UPDATE_VALUE_PO:
        walRecord = new CellBTreeBucketSingleValueV3UpdateValuePO();
        break;
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_REMOVE_NON_LEAF_ENTRY_PO:
        walRecord = new CellBTreeBucketSingleValueV3RemoveNonLeafEntryPO();
        break;
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_SET_LEFT_SIBLING_PO:
        walRecord = new CellBTreeBucketSingleValueV3SetLeftSiblingPO();
        break;
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_SET_RIGHT_SIBLING_PO:
        walRecord = new CellBTreeBucketSingleValueV3SetRightSiblingPO();
        break;
      case CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V3_INIT_PO:
        walRecord = new CellBTreeEntryPointSingleValueV3InitPO();
        break;
      case CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V3_SET_PAGES_SIZE_PO:
        walRecord = new CellBTreeEntryPointSingleValueV3SetPagesSizePO();
        break;
      case CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V3_SET_TREE_SIZE_PO:
        walRecord = new CellBTreeEntryPointSingleValueV3SetTreeSizePO();
        break;
      case CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V3_SET_FREE_LIST_HEAD_PO:
        walRecord = new CellBTreeSingleValueEntryPointV3SetFreeListHeadPO();
        break;
      case CELL_BTREE_BUCKET_SINGLE_VALUE_V3_SET_NEXT_FREE_LIST_PAGE_PO:
        walRecord = new CellBTreeBucketSingleValueV3SetNextFreeListPagePO();
        break;
      case CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V3_INIT_PO:
        walRecord = new CellBTreeNullBucketSingleValueV3InitPO();
        break;
      case CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V3_SET_VALUE_PO:
        walRecord = new CellBTreeNullBucketSingleValueV3SetValuePO();
        break;
      case CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V3_REMOVE_VALUE_PO:
        walRecord = new CellBTreeNullBucketSingleValueV3RemoveValuePO();
        break;
      case WALRecordTypes.CELL_BTREE_BUCKET_SINGLE_VALUE_V3_SWITCH_BUCKET_TYPE_PO:
        walRecord = new CellBTreeBucketSingleValueV3SwitchBucketTypePO();
        break;
      case SBTREE_BUCKET_V1_INIT_PO:
        walRecord = new SBTreeBucketV1InitPO();
        break;
      case SBTREE_BUCKET_V1_ADD_LEAF_ENTRY_PO:
        walRecord = new SBTreeBucketV1AddLeafEntryPO();
        break;
      case WALRecordTypes.SBTREE_BUCKET_V1_ADD_NON_LEAF_ENTRY_PO:
        walRecord = new SBTreeBucketV1AddNonLeafEntryPO();
        break;
      case SBTREE_BUCKET_V1_REMOVE_LEAF_ENTRY_PO:
        walRecord = new SBTreeBucketV1RemoveLeafEntryPO();
        break;
      case SBTREE_BUCKET_V1_REMOVE_NON_LEAF_ENTRY_PO:
        walRecord = new SBTreeBucketV1RemoveNonLeafEntryPO();
        break;
      case SBTREE_BUCKET_V1_ADD_ALL_PO:
        walRecord = new SBTreeBucketV1AddAllPO();
        break;
      case SBTREE_BUCKET_V1_SHRINK_PO:
        walRecord = new SBTreeBucketV1ShrinkPO();
        break;
      case SBTREE_BUCKET_V1_UPDATE_VALUE_PO:
        walRecord = new SBTreeBucketV1UpdateValuePO();
        break;
      case SBTREE_BUCKET_V1_SWITCH_BUCKET_TYPE_PO:
        walRecord = new SBTreeBucketV1SwitchBucketTypePO();
        break;
      case SBTREE_BUCKET_V1_SET_LEFT_SIBLING_PO:
        walRecord = new SBTreeBucketV1SetLeftSiblingPO();
        break;
      case SBTREE_BUCKET_V1_SET_RIGHT_SIBLING_PO:
        walRecord = new SBTreeBucketV1SetRightSiblingPO();
        break;
      case SBTREE_NULL_BUCKET_V1_INIT_PO:
        walRecord = new SBTreeNullBucketV1InitPO();
        break;
      case SBTREE_NULL_BUCKET_V1_SET_VALUE_PO:
        walRecord = new SBTreeNullBucketV1SetValuePO();
        break;
      case SBTREE_NULL_BUCKET_V1_REMOVE_VALUE_PO:
        walRecord = new SBTreeNullBucketV1RemoveValuePO();
        break;
      case SBTREE_BUCKET_V2_INIT_PO:
        walRecord = new SBTreeBucketV2InitPO();
        break;
      case SBTREE_BUCKET_V2_ADD_ALL_PO:
        walRecord = new SBTreeBucketV2AddAllPO();
        break;
      case SBTREE_BUCKET_V2_ADD_LEAF_ENTRY_PO:
        walRecord = new SBTreeBucketV2AddLeafEntryPO();
        break;
      case WALRecordTypes.SBTREE_BUCKET_V2_ADD_NON_LEAF_ENTRY_PO:
        walRecord = new SBTreeBucketV2AddNonLeafEntryPO();
        break;
      case SBTREE_BUCKET_V2_REMOVE_LEAF_ENTRY_PO:
        walRecord = new SBTreeBucketV2RemoveLeafEntryPO();
        break;
      case SBTREE_BUCKET_V2_REMOVE_NON_LEAF_ENTRY_PO:
        walRecord = new SBTreeBucketV2RemoveNonLeafEntryPO();
        break;
      case SBTREE_BUCKET_V2_SET_LEFT_SIBLING_PO:
        walRecord = new SBTreeBucketV2SetLeftSiblingPO();
        break;
      case SBTREE_BUCKET_V2_SET_RIGHT_SIBLING_PO:
        walRecord = new SBTreeBucketV2SetRightSiblingPO();
        break;
      case SBTREE_BUCKET_V2_SHRINK_PO:
        walRecord = new SBTreeBucketV2ShrinkPO();
        break;
      case SBTREE_BUCKET_V2_SWITCH_BUCKET_TYPE_PO:
        walRecord = new SBTreeBucketV2SwitchBucketTypePO();
        break;
      case SBTREE_BUCKET_V2_UPDATE_VALUE_PO:
        walRecord = new SBTreeBucketV2UpdateValuePO();
        break;
      case SBTREE_BUCKET_V1_SET_TREE_SIZE_PO:
        walRecord = new SBTreeBucketV1SetTreeSizePO();
        break;
      case SBTREE_BUCKET_V2_SET_TREE_SIZE_PO:
        walRecord = new SBTreeBucketV2SetTreeSizePO();
        break;
      case SBTREE_NULL_BUCKET_V2_INIT_PO:
        walRecord = new SBTreeNullBucketV2InitPO();
        break;
      case SBTREE_NULL_BUCKET_V2_REMOVE_VALUE_PO:
        walRecord = new SBTreeNullBucketV2RemoveValuePO();
        break;
      case SBTREE_NULL_BUCKET_V2_SET_VALUE_PO:
        walRecord = new SBTreeNullBucketV2SetValuePO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_INIT_PO:
        walRecord = new CellBTreeMultiValueV2BucketInitPO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_CREATE_MAIN_LEAF_ENTRY_PO:
        walRecord = new CellBTreeMultiValueV2BucketCreateMainLeafEntryPO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_REMOVE_MAIN_LEAF_ENTRY_PO:
        walRecord = new CellBTreeMultiValueV2BucketRemoveMainLeafEntryPO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_APPEND_NEW_LEAF_ENTRY_PO:
        walRecord = new CellBTreeMultiValueV2BucketAppendNewLeafEntryPO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_REMOVE_LEAF_ENTRY_PO:
        walRecord = new CellBTreeMultiValueV2BucketRemoveLeafEntryPO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_INCREMENT_ENTRIES_COUNT_PO:
        walRecord = new CellBTreeMultiValueV2BucketIncrementEntriesCountPO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_DECREMENT_ENTRIES_COUNT_PO:
        walRecord = new CellBTreeMultiValueV2BucketDecrementEntriesCountPO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_ADD_NON_LEAF_ENTRY_PO:
        walRecord = new CellBTreeMultiValueV2BucketAddNonLeafEntryPO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_REMOVE_NON_LEAF_ENTRY_PO:
        walRecord = new CellBTreeMultiValueV2BucketRemoveNonLeafEntryPO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_ADD_ALL_LEAF_ENTRIES_PO:
        walRecord = new CellBTreeMultiValueV2BucketAddAllLeafEntriesPO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_ADD_ALL_NON_LEAF_ENTRIES_PO:
        walRecord = new CellBTreeMultiValueV2BucketAddAllNonLeafEntriesPO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_SHRINK_LEAF_ENTRIES_PO:
        walRecord = new CellBTreeMultiValueV2BucketShrinkLeafEntriesPO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_SHRINK_NON_LEAF_ENTRIES_PO:
        walRecord = new CellBTreeMultiValueV2BucketShrinkNonLeafEntriesPO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_SET_LEFT_SIBLING_PO:
        walRecord = new CellBTreeMultiValueV2BucketSetLeftSiblingPO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_SET_RIGHT_SIBLING_PO:
        walRecord = new CellBTreeMultiValueV2BucketSetRightSiblingPO();
        break;
      case CELL_BTREE_NULL_BUCKET_MULTI_VALUE_V2_SET_RIGHT_SIBLING_PO:
        walRecord = new CellBTreeMultiValueV2NullBucketInitPO();
        break;
      case CELL_BTREE_NULL_BUCKET_MULTI_VALUE_V2_ADD_VALUE_PO:
        walRecord = new CellBTreeMultiValueV2NullBucketAddValuePO();
        break;
      case CELL_BTREE_NULL_BUCKET_MULTI_VALUE_V2_INCREMENT_SIZE_PO:
        walRecord = new CellBTreeMultiValueV2NullBucketIncrementSizePO();
        break;
      case CELL_BTREE_NULL_BUCKET_MULTI_VALUE_V2_DECREMENT_SIZE_PO:
        walRecord = new CellBTreeMultiValueV2NullBucketDecrementSizePO();
        break;
      case CELL_BTREE_NULL_BUCKET_MULTI_VALUE_V2_REMOVE_VALUE_PO:
        walRecord = new CellBTreeMultiValueV2NullBucketRemoveValuePO();
        break;
      case CELL_BTREE_ENTRY_POINT_MULTI_VALUE_V2_INIT_PO:
        walRecord = new CellBTreeMultiValueV2EntryPointInitPO();
        break;
      case CELL_BTREE_ENTRY_POINT_MULTI_VALUE_V2_SET_TREE_SIZE_PO:
        walRecord = new CellBTreeMultiValueV2EntryPointSetTreeSizePO();
        break;
      case CELL_BTREE_ENTRY_POINT_MULTI_VALUE_V2_SET_PAGES_SIZE_PO:
        walRecord = new CellBTreeMultiValueV2EntryPointSetPagesSizePO();
        break;
      case CELL_BTREE_ENTRY_POINT_MULTI_VALUE_V2_SET_ENTRY_ID_PO:
        walRecord = new CellBTreeMultiValueV2EntryPointSetEntryIdPO();
        break;
      case CELL_BTREE_BUCKET_MULTI_VALUE_V2_SWITCH_BUCKET_TYPE_PO:
        walRecord = new CellBTreeMultiValueV2BucketSwitchBucketTypePO();
        break;
      case LOCAL_HASH_TABLE_V2_BUCKET_INIT_PO:
        walRecord = new LocalHashTableV2BucketInitPO();
        break;
      case LOCAL_HASH_TABLE_V2_BUCKET_UPDATE_ENTRY_PO:
        walRecord = new LocalHashTableV2BucketUpdateEntryPO();
        break;
      case LOCAL_HASH_TABLE_V2_BUCKET_DELETE_ENTRY_PO:
        walRecord = new LocalHashTableV2BucketDeleteEntryPO();
        break;
      case LOCAL_HASH_TABLE_V2_BUCKET_ADD_ENTRY_PO:
        walRecord = new LocalHashTableV2BucketAddEntryPO();
        break;
      case LOCAL_HASH_TABLE_V2_BUCKET_SET_DEPTH_PO:
        walRecord = new LocalHashTableV2BucketSetDepthPO();
        break;
      case LOCAL_HASH_TABLE_V2_DIRECTORY_PAGE_SET_MAX_LEFT_CHILDREN_DEPTH_PO:
        walRecord = new LocalHashTableV2DirectoryPageSetMaxLeftChildDepthPO();
        break;
      case LOCAL_HASH_TABLE_V2_DIRECTORY_PAGE_SET_MAX_RIGHT_CHILDREN_DEPTH_PO:
        walRecord = new LocalHashTableV2DirectoryPageSetMaxRightChildDepthPO();
        break;
      case LOCAL_HASH_TABLE_V2_DIRECTORY_PAGE_SET_NODE_LOCAL_DEPTH_PO:
        walRecord = new LocalHashTableV2DirectoryPageSetNodeLocalDepthPO();
        break;
      case LOCAL_HASH_TABLE_V2_DIRECTORY_PAGE_SET_POINTER_PO:
        walRecord = new LocalHashTableV2DirectoryPageSetPointerPO();
        break;
      case LOCAL_HASH_TABLE_V2_DIRECTORY_FIRST_PAGE_SET_TREE_SIZE_PO:
        walRecord = new LocalHashTableV2DirectoryFirstPageSetTreeSizePO();
        break;
      case LOCAL_HASH_TABLE_V2_DIRECTORY_FIRST_PAGE_SET_TOMBSTONE_PO:
        walRecord = new LocalHashTableV2DirectoryFirstPageSetTombstonePO();
        break;
      case LOCAL_HASH_TABLE_V2_METADATA_PAGE_INIT_PO:
        walRecord = new LocalHashTableV2MetadataPageInitPO();
        break;
      case LOCAL_HASH_TABLE_V2_METADATA_PAGE_SET_RECORDS_COUNT_PO:
        walRecord = new LocalHashTableV2MetadataPageSetRecordsCountPO();
        break;
      case LOCAL_HASH_TABLE_V2_NULL_BUCKET_INIT_PO:
        walRecord = new LocalHashTableV2NullBucketInitPO();
        break;
      case LOCAL_HASH_TABLE_V2_NULL_BUCKET_SET_VALUE_PO:
        walRecord = new LocalHashTableV2NullBucketSetValuePO();
        break;
      case LOCAL_HASH_TABLE_V2_NULL_BUCKET_REMOVE_VALUE_PO:
        walRecord = new LocalHashTableV2NullBucketRemoveValuePO();
        break;
      case LOCAL_HASH_TABLE_V2_DIRECTORY_FIRST_PAGE_SET_MAX_LEFT_CHILDREN_DEPTH_PO:
        walRecord = new LocalHashTableV2DirectoryFirstPageSetMaxLeftChildDepthPO();
        break;
      case LOCAL_HASH_TABLE_V2_DIRECTORY_FIRST_PAGE_SET_MAX_RIGHT_CHILDREN_DEPTH_PO:
        walRecord = new LocalHashTableV2DirectoryFirstPageSetMaxRightChildDepthPO();
        break;
      case LOCAL_HASH_TABLE_V2_DIRECTORY_FIRST_PAGE_SET_NODE_LOCAL_DEPTH_PO:
        walRecord = new LocalHashTableV2DirectoryFirstPageSetNodeLocalDepthPO();
        break;
      case LOCAL_HASH_TABLE_V2_DIRECTORY_FIRST_PAGE_SET_POINTER_PO:
        walRecord = new LocalHashTableV2DirectoryFirstPageSetPointerPO();
        break;
      case TX_METADATA:
        walRecord = new MetaDataRecord();
        break;
      case FREE_SPACE_MAP_INIT:
        walRecord = new InitFreeSpaceMapPO();
        break;
      case FREE_SPACE_MAP_UPDATE:
        walRecord = new UpdateMaxFreeSpacePO();
        break;
      default:
        if (idToTypeMap.containsKey(recordId))
          try {
            //noinspection unchecked
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
