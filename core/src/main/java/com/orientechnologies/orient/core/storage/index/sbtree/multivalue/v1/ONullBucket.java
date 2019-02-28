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
package com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v1;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.util.ArrayList;
import java.util.List;

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
  private static final int RID_SIZE = OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;

  private static final int NEXT_FREE_LIST_OFFSET = NEXT_FREE_POSITION;
  private static final int NEXT_OFFSET           = NEXT_FREE_LIST_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int RIDS_SIZE_OFFSET = NEXT_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int RIDS_OFFSET      = RIDS_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  ONullBucket(final OCacheEntry cacheEntry, final boolean isNew) {
    super(cacheEntry);

    if (isNew) {
      setIntValue(RIDS_SIZE_OFFSET, 0);
      setIntValue(NEXT_FREE_LIST_OFFSET, -1);
      setIntValue(NEXT_OFFSET, -1);
    }
  }

  void setNext(final int pageIndex) {
    setIntValue(NEXT_OFFSET, pageIndex);
  }

  int getNext() {
    return getIntValue(NEXT_OFFSET);
  }

  void setNextFreeList(final int pageIndex) {
    setIntValue(NEXT_FREE_LIST_OFFSET, pageIndex);
  }

  int getNextFreeList() {
    return getIntValue(NEXT_FREE_LIST_OFFSET);
  }

  boolean addValue(final ORID rid) {
    final int size = getIntValue(RIDS_SIZE_OFFSET);
    final int position = size * RID_SIZE + RIDS_OFFSET;

    if (position + RID_SIZE > MAX_PAGE_SIZE_BYTES) {
      return false;
    }

    setShortValue(position, (short) rid.getClusterId());
    setLongValue(position + OShortSerializer.SHORT_SIZE, rid.getClusterPosition());

    setIntValue(RIDS_SIZE_OFFSET, size + 1);

    return true;
  }

  public List<ORID> getValues() {
    final List<ORID> rids = new ArrayList<>();
    final int size = getIntValue(RIDS_SIZE_OFFSET);
    final int end = size * RID_SIZE + RIDS_OFFSET;

    for (int position = RIDS_OFFSET; position < end; position += RID_SIZE) {
      final int clusterId = getShortValue(position);
      final long clusterPosition = getLongValue(position + OShortSerializer.SHORT_SIZE);

      rids.add(new ORecordId(clusterId, clusterPosition));
    }

    return rids;
  }

  public int getSize() {
    return getIntValue(RIDS_SIZE_OFFSET);
  }

  boolean removeValue(final ORID rid) {
    final int size = getIntValue(RIDS_SIZE_OFFSET);
    final int end = size * RID_SIZE + RIDS_OFFSET;

    for (int position = RIDS_OFFSET; position < end; position += RID_SIZE) {
      final int clusterId = getShortValue(position);
      if (clusterId != rid.getClusterId()) {
        continue;
      }

      final long clusterPosition = getLongValue(position + OShortSerializer.SHORT_SIZE);
      if (clusterPosition == rid.getClusterPosition()) {
        moveData(position + RID_SIZE, position, end - (position + RID_SIZE));
        setIntValue(RIDS_SIZE_OFFSET, size - 1);
        return true;
      }
    }

    return false;
  }

  boolean isEmpty() {
    return getIntValue(RIDS_SIZE_OFFSET) == 0;
  }

  boolean isFull() {
    final int size = getIntValue(RIDS_SIZE_OFFSET);
    final int position = size * RID_SIZE + RIDS_OFFSET;

    return position + RID_SIZE > MAX_PAGE_SIZE_BYTES;

  }
}
