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

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;

/**
 * @author Andrey Lomakin
 * @since 27.05.13
 */
public abstract class OPageDiff<T> {
  protected final ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

  protected T                   newValue;

  protected int                 pageOffset;

  OPageDiff() {
  }

  public OPageDiff(T newValue, int pageOffset) {
    this.newValue = newValue;
    this.pageOffset = pageOffset;
  }

  public T getNewValue() {
    return newValue;
  }

  public int serializedSize() {
    return OIntegerSerializer.INT_SIZE;
  }

  public int toStream(byte[] stream, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(pageOffset, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  public int fromStream(byte[] stream, int offset) {
    pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  public int getPageOffset() {
    return pageOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OPageDiff oPageDiff = (OPageDiff) o;

    if (pageOffset != oPageDiff.pageOffset)
      return false;
    if (!newValue.equals(oPageDiff.newValue))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = newValue.hashCode();
    result = 31 * result + pageOffset;
    return result;
  }

  public abstract void restorePageData(long pagePointer);
}
