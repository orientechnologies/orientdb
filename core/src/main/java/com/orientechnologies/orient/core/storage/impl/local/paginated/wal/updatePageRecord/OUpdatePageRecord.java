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

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAbstractPageWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

/**
 * @author Andrey Lomakin
 * @since 26.04.13
 */
public class OUpdatePageRecord extends OAbstractPageWALRecord {
  private List<OPageDiff<?>> diffs = new ArrayList<OPageDiff<?>>();

  public OUpdatePageRecord() {
  }

  public OUpdatePageRecord(long pageIndex, int clusterId, OLogSequenceNumber prevUnitRecord, List<OPageDiff<?>> diffs) {
    super(pageIndex, clusterId, prevUnitRecord);
    this.diffs = diffs;
  }

  public List<OPageDiff<?>> getChanges() {
    return diffs;
  }

  @Override
  public int serializedSize() {
    int serializedSize = super.serializedSize();
    serializedSize += OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE * diffs.size();

    for (OPageDiff diff : diffs) {
      serializedSize += diff.serializedSize();
    }

    return serializedSize;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(diffs.size(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (OPageDiff diff : diffs) {
      content[offset] = typeToId(diff.getClass());
      offset++;

      diff.toStream(content, offset);
      offset += diff.serializedSize();
    }

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    int size = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    diffs = new ArrayList<OPageDiff<?>>(size);
    for (int i = 0; i < size; i++) {
      byte typeId = content[offset];
      offset++;

      OPageDiff<?> diff = newDiffInstance(typeId);
      diff.fromStream(content, offset);
      offset += diff.serializedSize();

      diffs.add(diff);
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

  private byte typeToId(Class<? extends OPageDiff> diffClass) {
    if (diffClass.equals(OBinaryPageDiff.class))
      return 1;

    if (diffClass.equals(OIntPageDiff.class))
      return 2;

    if (diffClass.equals(OLongPageDiff.class))
      return 3;

    if (diffClass.equals(OBinaryFullPageDiff.class))
      return 4;

    if (diffClass.equals(OIntFullPageDiff.class))
      return 5;

    if (diffClass.equals(OLongFullPageDiff.class))
      return 6;

    throw new IllegalArgumentException("Unknown Diff class " + diffClass);
  }

  private OPageDiff<?> newDiffInstance(byte typeId) {
    if (typeId == 1)
      return new OBinaryPageDiff();

    if (typeId == 2)
      return new OIntPageDiff();

    if (typeId == 3)
      return new OLongPageDiff();

    if (typeId == 4)
      return new OBinaryFullPageDiff();

    if (typeId == 5)
      return new OIntFullPageDiff();

    if (typeId == 6)
      return new OLongFullPageDiff();

    throw new IllegalArgumentException("Unknown Diff id " + typeId);
  }

}
