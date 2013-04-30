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

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.util.Arrays;

import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;

/**
 * @author Andrey Lomakin
 * @since 26.04.13
 */
public class OSetPageDataRecord extends OAbstractWALRecord {
  private byte[] data;
  private int    pageOffset;

  public OSetPageDataRecord() {
  }

  public OSetPageDataRecord(byte[] data, int pageOffset, long pageIndex, String fileName) {
    super(pageIndex, fileName);
    this.data = data;
    this.pageOffset = pageOffset;
  }

  public byte[] getData() {
    return data;
  }

  public int getPageOffset() {
    return pageOffset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + OBinaryTypeSerializer.INSTANCE.getObjectSize(data);
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OBinaryTypeSerializer.INSTANCE.serializeNative(data, content, offset);
    offset += OBinaryTypeSerializer.INSTANCE.getObjectSize(data);

    OIntegerSerializer.INSTANCE.serializeNative(pageOffset, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    data = OBinaryTypeSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OBinaryTypeSerializer.INSTANCE.getObjectSize(data);

    pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;

    OSetPageDataRecord that = (OSetPageDataRecord) o;

    if (pageOffset != that.pageOffset)
      return false;
    if (!Arrays.equals(data, that.data))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + Arrays.hashCode(data);
    result = 31 * result + pageOffset;
    return result;
  }
}
