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
package com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTree;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v1.OSBTreeV1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Bucket which is intended to save values stored in sbtree under <code>null</code> key. Bucket has following layout:
 * <ol>
 * <li>First byte is flag which indicates presence of value in bucket</li>
 * <li>Second byte indicates whether value is presented by link to the "bucket list" where actual value is stored or real value
 * passed be user.</li>
 * <li>The rest is serialized value whether link or passed in value.</li>
 * </ol>
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/15/14
 */
final class ONullBucket extends ODurablePage {
  private static final int EMBEDDED_RIDS_BOUNDARY = 64;

  private static final int RID_SIZE = OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;

  private static final int M_ID_OFFSET               = NEXT_FREE_POSITION;
  private static final int EMBEDDED_RIDS_SIZE_OFFSET = M_ID_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int RIDS_SIZE_OFFSET          = EMBEDDED_RIDS_SIZE_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int RIDS_OFFSET               = RIDS_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  private final OSBTreeV1<OMultiValueEntry, Byte> multiContainer;

  ONullBucket(final OCacheEntry cacheEntry, final OSBTreeV1<OMultiValueEntry, Byte> multiContainer) {
    super(cacheEntry);

    this.multiContainer = multiContainer;
  }

  protected void init(final long mId) {
    setLongValue(M_ID_OFFSET, mId);
    setByteValue(EMBEDDED_RIDS_SIZE_OFFSET, (byte) 0);
    setIntValue(RIDS_SIZE_OFFSET, 0);
  }

  void addValue(final ORID rid) throws IOException {
    final int embeddedSize = getByteValue(EMBEDDED_RIDS_SIZE_OFFSET);

    if (embeddedSize < EMBEDDED_RIDS_BOUNDARY) {
      final int position = embeddedSize * RID_SIZE + RIDS_OFFSET;

      setShortValue(position, (short) rid.getClusterId());
      setLongValue(position + OShortSerializer.SHORT_SIZE, rid.getClusterPosition());

      setByteValue(EMBEDDED_RIDS_SIZE_OFFSET, (byte) (embeddedSize + 1));
    } else {
      final long mId = getLongValue(M_ID_OFFSET);
      multiContainer.put(new OMultiValueEntry(mId, rid.getClusterId(), rid.getClusterPosition()), (byte) 1);
    }

    final int size = getIntValue(RIDS_SIZE_OFFSET);
    setIntValue(RIDS_SIZE_OFFSET, size + 1);

  }

  public List<ORID> getValues() {
    final int size = getIntValue(RIDS_SIZE_OFFSET);
    final List<ORID> rids = new ArrayList<>(size);

    final int embeddedSize = getByteValue(EMBEDDED_RIDS_SIZE_OFFSET);
    final int end = embeddedSize * RID_SIZE + RIDS_OFFSET;

    for (int position = RIDS_OFFSET; position < end; position += RID_SIZE) {
      final int clusterId = getShortValue(position);
      final long clusterPosition = getLongValue(position + OShortSerializer.SHORT_SIZE);

      rids.add(new ORecordId(clusterId, clusterPosition));
    }

    if (size > embeddedSize) {
      final long mId = getLongValue(M_ID_OFFSET);

      final OSBTree.OSBTreeCursor<OMultiValueEntry, Byte> cursor = multiContainer
          .iterateEntriesBetween(new OMultiValueEntry(mId, 0, 0), true, new OMultiValueEntry(mId + 1, 0, 0), false, true);

      Map.Entry<OMultiValueEntry, Byte> mapEntry = cursor.next(-1);
      while (mapEntry != null) {
        final OMultiValueEntry entry = mapEntry.getKey();
        rids.add(new ORecordId(entry.clusterId, entry.clusterPosition));

        mapEntry = cursor.next(-1);
      }
    }

    assert rids.size() == size;

    return rids;
  }

  public int getSize() {
    return getIntValue(RIDS_SIZE_OFFSET);
  }

  boolean removeValue(final ORID rid) throws IOException {
    final int size = getIntValue(RIDS_SIZE_OFFSET);

    final int embeddedSize = getByteValue(EMBEDDED_RIDS_SIZE_OFFSET);
    final int end = embeddedSize * RID_SIZE + RIDS_OFFSET;

    for (int position = RIDS_OFFSET; position < end; position += RID_SIZE) {
      final int clusterId = getShortValue(position);
      if (clusterId != rid.getClusterId()) {
        continue;
      }

      final long clusterPosition = getLongValue(position + OShortSerializer.SHORT_SIZE);
      if (clusterPosition == rid.getClusterPosition()) {
        moveData(position + RID_SIZE, position, end - (position + RID_SIZE));
        setByteValue(EMBEDDED_RIDS_SIZE_OFFSET, (byte) (embeddedSize - 1));
        setIntValue(RIDS_SIZE_OFFSET, size - 1);
        return true;
      }
    }

    if (size > embeddedSize) {
      final long mId = getLongValue(M_ID_OFFSET);
      final Byte result = multiContainer.remove(new OMultiValueEntry(mId, rid.getClusterId(), rid.getClusterPosition()));
      if (result != null) {
        setIntValue(RIDS_SIZE_OFFSET, size - 1);
        return true;
      }
    }

    return false;
  }

  int remove() throws IOException {
    final long mId = getLongValue(M_ID_OFFSET);
    final int embeddedSize = getByteValue(EMBEDDED_RIDS_SIZE_OFFSET);
    final int size = getIntValue(RIDS_SIZE_OFFSET);

    if (size > embeddedSize) {
      final List<OMultiValueEntry> entriesToRemove = new ArrayList<>(size - embeddedSize);

      final OSBTree.OSBTreeCursor<OMultiValueEntry, Byte> cursor = multiContainer
          .iterateEntriesBetween(new OMultiValueEntry(mId, 0, 0), true, new OMultiValueEntry(mId + 1, 0, 0), false, true);

      Map.Entry<OMultiValueEntry, Byte> mapEntry = cursor.next(-1);
      while (mapEntry != null) {
        final OMultiValueEntry entry = mapEntry.getKey();
        entriesToRemove.add(entry);

        mapEntry = cursor.next(-1);
      }

      for (final OMultiValueEntry entry : entriesToRemove) {
        multiContainer.remove(entry);
      }
    }

    setByteValue(EMBEDDED_RIDS_SIZE_OFFSET, (byte) 0);
    setIntValue(RIDS_SIZE_OFFSET, 0);

    return size;
  }

  void clear() {
    setByteValue(EMBEDDED_RIDS_SIZE_OFFSET, (byte) 0);
    setIntValue(RIDS_SIZE_OFFSET, 0);
  }

  boolean isEmpty() {
    return getIntValue(RIDS_SIZE_OFFSET) == 0;
  }
}
