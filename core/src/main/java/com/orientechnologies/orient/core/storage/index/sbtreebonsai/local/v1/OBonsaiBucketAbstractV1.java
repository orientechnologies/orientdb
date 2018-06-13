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

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;

/**
 * A base class for bonsai buckets. Bonsai bucket size is usually less than page size and one page could contain multiple bonsai
 * buckets.
 * Adds methods to read and write bucket pointers.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 * @see com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer
 * @see com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai
 */
class OBonsaiBucketAbstractV1 extends ODurablePage {
  OBonsaiBucketAbstractV1(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  /**
   * Write a bucket pointer to specific location.
   *
   * @param pageOffset where to write
   * @param value      - pointer to write
   */
  void setBucketPointer(int pageOffset, OBonsaiBucketPointer value) {
    buffer.putLong(pageOffset, value.getPageIndex());
    buffer.putInt(pageOffset + OLongSerializer.LONG_SIZE, value.getPageOffset());

    cacheEntry.markDirty();
  }

  /**
   * Read bucket pointer from page.
   *
   * @param offset where the pointer should be read from
   *
   * @return bucket pointer
   */
  OBonsaiBucketPointer getBucketPointer(int offset) {
    final long pageIndex = buffer.getLong(offset);
    final int pageOffset = buffer.getInt(offset + OLongSerializer.LONG_SIZE);
    return new OBonsaiBucketPointer((int) pageIndex, pageOffset, OSBTreeBonsaiLocalV1.BINARY_VERSION);
  }
}
