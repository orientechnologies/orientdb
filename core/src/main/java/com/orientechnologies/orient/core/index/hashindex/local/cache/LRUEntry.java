/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.index.hashindex.local.cache;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

/**
 * @author Andrey Lomakin
 * @since 25.02.13
 */
class LRUEntry {
  long               fileId;
  long               pageIndex;

  OLogSequenceNumber loadedLSN;

  long               dataPointer;
  boolean            isDirty;

  long               hashCode;

  int                usageCounter = 0;

  LRUEntry           next;

  LRUEntry           after;
  LRUEntry           before;

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    LRUEntry lruEntry = (LRUEntry) o;

    if (dataPointer != lruEntry.dataPointer)
      return false;
    if (fileId != lruEntry.fileId)
      return false;
    if (isDirty != lruEntry.isDirty)
      return false;
    if (pageIndex != lruEntry.pageIndex)
      return false;
    if (loadedLSN != null ? !loadedLSN.equals(lruEntry.loadedLSN) : lruEntry.loadedLSN != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (fileId ^ (fileId >>> 32));
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + (loadedLSN != null ? loadedLSN.hashCode() : 0);
    result = 31 * result + (int) (dataPointer ^ (dataPointer >>> 32));
    result = 31 * result + (isDirty ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "LRUEntry{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + ", loadedLSN=" + loadedLSN + ", dataPointer="
        + dataPointer + ", isDirty=" + isDirty + ", hashCode=" + hashCode + ", usageCounter=" + usageCounter + '}';
  }
}
