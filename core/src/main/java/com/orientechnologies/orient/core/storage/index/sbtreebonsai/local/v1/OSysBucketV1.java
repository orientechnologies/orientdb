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

package com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.v1;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
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
public final class OSysBucketV1 extends OBonsaiBucketAbstractV1 {
  private static final int  SYS_MAGIC_OFFSET        = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int  FREE_SPACE_OFFSET       = SYS_MAGIC_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int  FREE_LIST_HEAD_OFFSET   = FREE_SPACE_OFFSET + OBonsaiBucketPointer.SIZE;
  private static final int  FREE_LIST_LENGTH_OFFSET = FREE_LIST_HEAD_OFFSET + OBonsaiBucketPointer.SIZE;

  /**
   * Magic number to check if the sys bucket is initialized.
   */
  private static final byte SYS_MAGIC               = (byte) 41;

  OSysBucketV1(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() {
    buffer.put(SYS_MAGIC_OFFSET, SYS_MAGIC);
    setBucketPointer(FREE_SPACE_OFFSET,
        new OBonsaiBucketPointer(0, OSBTreeBonsaiBucketV1.MAX_BUCKET_SIZE_BYTES, OSBTreeBonsaiLocalV1.BINARY_VERSION));
    setBucketPointer(FREE_LIST_HEAD_OFFSET, OBonsaiBucketPointer.NULL);
    buffer.putLong(FREE_LIST_LENGTH_OFFSET, 0L);
  }

  public boolean isInitialized() {
    return buffer.get(SYS_MAGIC_OFFSET) != 41;
  }

  long freeListLength() {
    return buffer.getLong(FREE_LIST_LENGTH_OFFSET);
  }

  void setFreeListLength(final long length) {
    buffer.putLong(FREE_LIST_LENGTH_OFFSET, length);
  }

  OBonsaiBucketPointer getFreeSpacePointer() {
    return getBucketPointer(FREE_SPACE_OFFSET);
  }

  void setFreeSpacePointer(final OBonsaiBucketPointer pointer) {
    setBucketPointer(FREE_SPACE_OFFSET, pointer);
  }

  OBonsaiBucketPointer getFreeListHead() {
    return getBucketPointer(FREE_LIST_HEAD_OFFSET);
  }

  void setFreeListHead(final OBonsaiBucketPointer pointer) {
    setBucketPointer(FREE_LIST_HEAD_OFFSET, pointer);
  }
}
