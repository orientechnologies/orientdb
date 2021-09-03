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

package com.orientechnologies.orient.core.storage.index.sbtreebonsai.local;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import java.io.IOException;

/**
 * A system bucket for bonsai tree pages. Single per file.
 *
 * <p>Holds an information about:
 *
 * <ul>
 *   <li>head of free list
 *   <li>length of free list
 *   <li>pointer to free space
 * </ul>
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public final class OSysBucket extends OBonsaiBucketAbstract {
  private static final int SYS_MAGIC_OFFSET = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int FREE_SPACE_OFFSET = SYS_MAGIC_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int FREE_LIST_HEAD_OFFSET = FREE_SPACE_OFFSET + OBonsaiBucketPointer.SIZE;
  private static final int FREE_LIST_LENGTH_OFFSET =
      FREE_LIST_HEAD_OFFSET + OBonsaiBucketPointer.SIZE;
  private static final int TREES_COUNT_OFFSET = FREE_LIST_LENGTH_OFFSET + OLongSerializer.LONG_SIZE;

  /** Magic number to check if the sys bucket is initialized. */
  private static final byte SYS_MAGIC = (byte) 41;

  public OSysBucket(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() throws IOException {
    setByteValue(SYS_MAGIC_OFFSET, SYS_MAGIC);
    setBucketPointer(
        FREE_SPACE_OFFSET, new OBonsaiBucketPointer(0, OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES));
    setBucketPointer(FREE_LIST_HEAD_OFFSET, OBonsaiBucketPointer.NULL);
    setLongValue(FREE_LIST_LENGTH_OFFSET, 0L);
    setIntValue(TREES_COUNT_OFFSET, 0);
  }

  public boolean isNotInitialized() {
    return getByteValue(SYS_MAGIC_OFFSET) != 41;
  }

  public long freeListLength() {
    return getLongValue(FREE_LIST_LENGTH_OFFSET);
  }

  public void setFreeListLength(long length) throws IOException {
    setLongValue(FREE_LIST_LENGTH_OFFSET, length);
  }

  public OBonsaiBucketPointer getFreeSpacePointer() {
    return getBucketPointer(FREE_SPACE_OFFSET);
  }

  public void setFreeSpacePointer(OBonsaiBucketPointer pointer) throws IOException {
    setBucketPointer(FREE_SPACE_OFFSET, pointer);
  }

  public OBonsaiBucketPointer getFreeListHead() {
    return getBucketPointer(FREE_LIST_HEAD_OFFSET);
  }

  public void setFreeListHead(OBonsaiBucketPointer pointer) throws IOException {
    setBucketPointer(FREE_LIST_HEAD_OFFSET, pointer);
  }

  void incrementTreesCount() {
    final int count = getIntValue(TREES_COUNT_OFFSET);
    assert count >= 0;

    setIntValue(TREES_COUNT_OFFSET, count + 1);
  }

  void decrementTreesCount() {
    final int count = getIntValue(TREES_COUNT_OFFSET);
    assert count > 0;

    setIntValue(TREES_COUNT_OFFSET, count - 1);
  }

  int getTreesCount() {
    return getIntValue(TREES_COUNT_OFFSET);
  }
}
