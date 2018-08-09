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

package com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.v2;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;

/**
 * <p>
 * A system bucket for bonsai tree pages. Single per file.
 * </p>
 * <p>
 * Holds an information about:
 * </p>
 * <ul>
 * <li>head of free list</li>
 * <li>length of free list</li>
 * <li>pointer to free space</li>
 * </ul>
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public final class OSysBucketV2 extends OBonsaiBucketAbstractV2 {

  /**
   * Magic number to check if the sys bucket is initialized.
   */
  private static final byte SYS_MAGIC = (byte) 41;

  OSysBucketV2(final OCacheEntry cacheEntry, int maxBucketSizeInBytes) {
    super(cacheEntry, maxBucketSizeInBytes);
  }

  public void init() {
    buffer.put(sysMagicOffset, SYS_MAGIC);
    setBucketPointer(freeSpaceOffset, new OBonsaiBucketPointer(0, maxBucketSizeInBytes, OSBTreeBonsaiLocalV2.BINARY_VERSION));
    setBucketPointer(freeListHeadOffset, OBonsaiBucketPointer.NULL);
    buffer.putLong(freeListLengthOffset, 0L);
  }

  public boolean isInitialized() {
    return buffer.get(sysMagicOffset) != 41;
  }

  long freeListLength() {
    return buffer.getLong(freeListLengthOffset);
  }

  void setFreeListLength(final long length) {
    buffer.putLong(freeListLengthOffset, length);
  }

  OBonsaiBucketPointer getFreeSpacePointer() {
    return getBucketPointer(freeSpaceOffset);
  }

  void setFreeSpacePointer(final OBonsaiBucketPointer pointer) {
    setBucketPointer(freeSpaceOffset, pointer);
  }

  OBonsaiBucketPointer getFreeListHead() {
    return getBucketPointer(freeListHeadOffset);
  }

  void setFreeListHead(final OBonsaiBucketPointer pointer) {
    setBucketPointer(freeListHeadOffset, pointer);
  }
}
