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

import java.util.Objects;

/**
 * A pointer to bucket in disk page. Defines the page and the offset in page where the bucket is placed. Is immutable.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OBonsaiBucketPointer {
  private static final long PAGE_MASK      = 0xFFFFFFFFL;
  private static final int  VERSION_OFFSET = 32;

  public static final int                  SIZE = OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;
  public static final OBonsaiBucketPointer NULL = new OBonsaiBucketPointer(-1, -1);

  private final int version;
  private final int pageIndex;
  private final int pageOffset;

  public OBonsaiBucketPointer(int pageIndex, int pageOffset, int version) {
    this.pageIndex = pageIndex;
    this.pageOffset = pageOffset;
    this.version = version;
  }

  public OBonsaiBucketPointer(long pageIndexVersion, int pageOffset) {
    this.pageOffset = pageOffset;
    this.pageIndex = (int) (pageIndexVersion & PAGE_MASK);
    this.version = (int) (pageIndexVersion >>> VERSION_OFFSET);
  }

  public long getPageIndexVersion() {
    return (((long) version) << VERSION_OFFSET) | pageIndex;
  }

  public int getVersion() {
    return version;
  }

  public int getPageIndex() {
    return pageIndex;
  }

  public int getPageOffset() {
    return pageOffset;
  }

  public boolean isValid() {
    return pageIndex >= 0 && version >= 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    OBonsaiBucketPointer pointer = (OBonsaiBucketPointer) o;
    return version == pointer.version && pageIndex == pointer.pageIndex && pageOffset == pointer.pageOffset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, pageIndex, pageOffset);
  }

  @Override
  public String toString() {
    return "OBonsaiBucketPointer{" + "version=" + version + ", pageIndex=" + pageIndex + ", pageOffset=" + pageOffset + '}';
  }
}
