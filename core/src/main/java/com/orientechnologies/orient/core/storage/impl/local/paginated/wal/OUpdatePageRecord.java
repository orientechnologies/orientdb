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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;

/**
 * @author Andrey Lomakin
 * @since 26.04.13
 */
public class OUpdatePageRecord extends OAbstractPageWALRecord {
  private List<Diff> diffs = new ArrayList<Diff>();

  public OUpdatePageRecord() {
  }

  public OUpdatePageRecord(long pageIndex, int clusterId) {
    super(pageIndex, clusterId);
  }

  public List<Diff> getDiffs() {
    return diffs;
  }

  public void addDiff(int pageOffset, byte[] data) {
    diffs.add(new Diff(data, pageOffset));
  }

  @Override
  public int serializedSize() {
    int serializedSize = super.serializedSize();
    serializedSize += OIntegerSerializer.INT_SIZE;

    for (Diff diff : diffs) {
      serializedSize += OIntegerSerializer.INT_SIZE;
      serializedSize += OBinaryTypeSerializer.INSTANCE.getObjectSize(diff.data);
    }

    return serializedSize;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(diffs.size(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (Diff diff : diffs) {
      OIntegerSerializer.INSTANCE.serializeNative(diff.pageOffset, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      OBinaryTypeSerializer.INSTANCE.serializeNative(diff.data, content, offset);
      offset += OBinaryTypeSerializer.INSTANCE.getObjectSize(diff.data);
    }

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    int size = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    diffs = new ArrayList<Diff>(size);
    for (int i = 0; i < size; i++) {
      int pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      byte[] data = OBinaryTypeSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OBinaryTypeSerializer.INSTANCE.getObjectSize(data);

      diffs.add(new Diff(data, pageOffset));
    }

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

    OUpdatePageRecord that = (OUpdatePageRecord) o;

    if (!diffs.equals(that.diffs))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + diffs.hashCode();
    return result;
  }

  public static final class Diff {
    private final byte[] data;
    private final int    pageOffset;

    public Diff(byte[] data, int pageOffset) {
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
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      Diff diff = (Diff) o;

      if (pageOffset != diff.pageOffset)
        return false;
      if (!Arrays.equals(data, diff.data))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(data);
      result = 31 * result + pageOffset;
      return result;
    }
  }
}
