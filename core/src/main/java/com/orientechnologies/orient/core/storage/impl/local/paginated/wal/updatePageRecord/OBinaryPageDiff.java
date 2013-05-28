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

import java.util.Arrays;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;

/**
 * @author Andrey Lomakin
 * @since 27.05.13
 */
public class OBinaryPageDiff extends OPageDiff<byte[]> {
  public OBinaryPageDiff() {
  }

  public OBinaryPageDiff(byte[] newValue, int pageOffset) {
    super(newValue, pageOffset);
  }

  @Override
  public void restorePageData(long pagePointer) {
    directMemory.set(pagePointer + pageOffset, newValue, 0, newValue.length);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + newValue.length;
  }

  @Override
  public int toStream(byte[] stream, int offset) {
    offset = super.toStream(stream, offset);
    OIntegerSerializer.INSTANCE.serializeNative(newValue.length, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(newValue, 0, stream, offset, newValue.length);
    offset += newValue.length;

    return offset;
  }

  @Override
  public int fromStream(byte[] stream, int offset) {
    offset = super.fromStream(stream, offset);

    int len = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    newValue = new byte[len];
    System.arraycopy(stream, offset, newValue, 0, len);
    offset += len;

    return offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OBinaryPageDiff diff = (OBinaryPageDiff) o;

    if (pageOffset != diff.pageOffset)
      return false;
    if (!Arrays.equals(newValue, diff.newValue))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(newValue);
    result = 31 * result + pageOffset;
    return result;
  }
}
