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

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 26.04.13
 */
public class OUpdatePageRecord extends OAbstractPageWALRecord {
  private List<Diff<?>> diffs = new ArrayList<Diff<?>>();

  public OUpdatePageRecord() {
  }

  public OUpdatePageRecord(long pageIndex, int clusterId, OLogSequenceNumber prevUnitRecord, List<Diff<?>> diffs) {
    super(pageIndex, clusterId, prevUnitRecord);
    this.diffs = diffs;
  }

  public List<Diff<?>> getDiffs() {
    return diffs;
  }

  @Override
  public int serializedSize() {
    int serializedSize = super.serializedSize();
    serializedSize += OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE * diffs.size();

    for (Diff diff : diffs) {
      serializedSize += diff.serializedSize();
    }

    return serializedSize;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(diffs.size(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (Diff diff : diffs) {
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

    diffs = new ArrayList<Diff<?>>(size);
    for (int i = 0; i < size; i++) {
      byte typeId = content[offset];
      offset++;

      Diff<?> diff = newDiffInstance(typeId);
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

  private byte typeToId(Class<? extends Diff> diffClass) {
    if (diffClass.equals(BinaryDiff.class))
      return 1;

    if (diffClass.equals(IntDiff.class))
      return 2;

    if (diffClass.equals(LongDiff.class))
      return 3;

    throw new IllegalArgumentException("Unknown Diff class " + diffClass);
  }

  private Diff<?> newDiffInstance(byte typeId) {
    if (typeId == 1)
      return new BinaryDiff();

    if (typeId == 2)
      return new IntDiff();

    if (typeId == 3)
      return new LongDiff();

    throw new IllegalArgumentException("Unknown Diff id " + typeId);
  }

  public abstract static class Diff<T> {
    protected final ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    protected T                   newValue;
    protected T                   oldValue;

    protected int                 pageOffset;

    protected Diff() {
    }

    public Diff(T oldValue, T newValue, int pageOffset) {
      this.oldValue = oldValue;
      this.newValue = newValue;
      this.pageOffset = pageOffset;
    }

    public T getNewValue() {
      return newValue;
    }

    public T getOldValue() {
      return oldValue;
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

      Diff diff = (Diff) o;

      if (pageOffset != diff.pageOffset)
        return false;
      if (!newValue.equals(diff.newValue))
        return false;
      if (!oldValue.equals(diff.oldValue))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = newValue.hashCode();
      result = 31 * result + oldValue.hashCode();
      result = 31 * result + pageOffset;
      return result;
    }

    public abstract void restoredPageData(long pagePointer);

    public abstract void revertPageData(long pagePointer);
  }

  public static final class BinaryDiff extends Diff<byte[]> {
    public BinaryDiff() {
    }

    public BinaryDiff(byte[] oldValue, byte[] newValue, int pageOffset) {
      super(oldValue, newValue, pageOffset);
    }

    @Override
    public int serializedSize() {
      return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + newValue.length + oldValue.length;
    }

    @Override
    public int toStream(byte[] stream, int offset) {
      offset = super.toStream(stream, offset);
      OIntegerSerializer.INSTANCE.serializeNative(oldValue.length, stream, offset);
      offset += OIntegerSerializer.INT_SIZE;

      System.arraycopy(oldValue, 0, stream, offset, oldValue.length);
      offset += oldValue.length;

      OIntegerSerializer.INSTANCE.serializeNative(newValue.length, stream, offset);
      offset += OIntegerSerializer.INT_SIZE;

      System.arraycopy(newValue, 0, stream, offset, newValue.length);
      offset += newValue.length;

      return offset;
    }

    @Override
    public int fromStream(byte[] stream, int offset) {
      offset = super.fromStream(stream, offset);

      int oldValLen = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
      offset += OIntegerSerializer.INT_SIZE;

      oldValue = new byte[oldValLen];
      System.arraycopy(stream, offset, oldValue, 0, oldValLen);
      offset += oldValLen;

      int newValLen = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
      offset += OIntegerSerializer.INT_SIZE;

      newValue = new byte[newValLen];
      System.arraycopy(stream, offset, newValue, 0, newValLen);
      offset += newValLen;

      return offset;
    }

    @Override
    public void restoredPageData(long pagePointer) {
      directMemory.set(pagePointer + pageOffset, newValue, 0, newValue.length);
    }

    @Override
    public void revertPageData(long pagePointer) {
      directMemory.set(pagePointer + pageOffset, oldValue, 0, oldValue.length);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      BinaryDiff diff = (BinaryDiff) o;

      if (getPageOffset() != diff.getPageOffset())
        return false;
      if (!Arrays.equals(newValue, diff.newValue))
        return false;
      if (!Arrays.equals(oldValue, diff.oldValue))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(newValue);
      result = 31 * result + Arrays.hashCode(oldValue);
      result = 31 * result + getPageOffset();
      return result;
    }
  }

  public static final class IntDiff extends Diff<Integer> {
    public IntDiff() {
    }

    public IntDiff(Integer oldValue, Integer newValue, int pageOffset) {
      super(oldValue, newValue, pageOffset);
    }

    @Override
    public int serializedSize() {
      return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
    }

    @Override
    public int toStream(byte[] stream, int offset) {
      offset = super.toStream(stream, offset);
      OIntegerSerializer.INSTANCE.serializeNative(oldValue, stream, offset);
      offset += OIntegerSerializer.INT_SIZE;

      OIntegerSerializer.INSTANCE.serializeNative(newValue, stream, offset);
      offset += OIntegerSerializer.INT_SIZE;

      return offset;
    }

    @Override
    public int fromStream(byte[] stream, int offset) {
      offset = super.fromStream(stream, offset);

      oldValue = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
      offset += OIntegerSerializer.INT_SIZE;

      newValue = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
      offset += OIntegerSerializer.INT_SIZE;

      return offset;
    }

    @Override
    public void restoredPageData(long pagePointer) {
      directMemory.setInt(pagePointer + pageOffset, newValue);
    }

    @Override
    public void revertPageData(long pagePointer) {
      directMemory.setInt(pagePointer + pageOffset, oldValue);
    }
  }

  public static final class LongDiff extends Diff<Long> {
    public LongDiff() {
    }

    public LongDiff(Long oldValue, Long newValue, int pageOffset) {
      super(oldValue, newValue, pageOffset);
    }

    @Override
    public int serializedSize() {
      return super.serializedSize() + 2 * OLongSerializer.LONG_SIZE;
    }

    @Override
    public int toStream(byte[] stream, int offset) {
      offset = super.toStream(stream, offset);

      OLongSerializer.INSTANCE.serializeNative(oldValue, stream, offset);
      offset += OLongSerializer.LONG_SIZE;

      OLongSerializer.INSTANCE.serializeNative(newValue, stream, offset);
      offset += OLongSerializer.LONG_SIZE;

      return offset;
    }

    @Override
    public int fromStream(byte[] stream, int offset) {
      offset = super.fromStream(stream, offset);

      oldValue = OLongSerializer.INSTANCE.deserializeNative(stream, offset);
      offset += OLongSerializer.LONG_SIZE;

      newValue = OLongSerializer.INSTANCE.deserializeNative(stream, offset);
      offset += OLongSerializer.LONG_SIZE;

      return offset;
    }

    @Override
    public void restoredPageData(long pagePointer) {
      directMemory.setLong(pagePointer + pageOffset, newValue);
    }

    @Override
    public void revertPageData(long pagePointer) {
      directMemory.setLong(pagePointer + pageOffset, oldValue);
    }
  }
}
