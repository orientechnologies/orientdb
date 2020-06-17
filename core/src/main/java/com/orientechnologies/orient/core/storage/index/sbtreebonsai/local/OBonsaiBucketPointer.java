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

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * A pointer to bucket in disk page. Defines the page and the offset in page where the bucket is
 * placed. Is immutable.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OBonsaiBucketPointer {
  public static final int SIZE = OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;
  public static final OBonsaiBucketPointer NULL = new OBonsaiBucketPointer(-1, -1);

  private final long pageIndex;
  private final int pageOffset;

  public OBonsaiBucketPointer(long pageIndex, int pageOffset) {
    this.pageIndex = pageIndex;
    this.pageOffset = pageOffset;
  }

  public long getPageIndex() {
    return pageIndex;
  }

  public int getPageOffset() {
    return pageOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OBonsaiBucketPointer that = (OBonsaiBucketPointer) o;

    if (pageIndex != that.pageIndex) return false;
    if (pageOffset != that.pageOffset) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + pageOffset;
    return result;
  }

  public boolean isValid() {
    return pageIndex >= 0;
  }
}
