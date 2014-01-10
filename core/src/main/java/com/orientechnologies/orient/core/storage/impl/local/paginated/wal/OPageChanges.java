/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
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
import java.util.List;
import java.util.ListIterator;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;

public class OPageChanges {
  private List<ChangeUnit> changeUnits    = new ArrayList<ChangeUnit>();
  private int              serializedSize = OIntegerSerializer.INT_SIZE;

  public void addChanges(int pageOffset, byte[] newValues, byte[] oldValues) {
    assert newValues == null || newValues.length == oldValues.length;

    changeUnits.add(new ChangeUnit(pageOffset, oldValues, newValues));

    serializedSize += compressedIntegerSize(pageOffset) + compressedIntegerSize(oldValues.length)
        + (newValues == null ? 0 : newValues.length) + oldValues.length + OByteSerializer.BYTE_SIZE;
  }

  public boolean isEmpty() {
    return changeUnits.isEmpty();
  }

  public void applyChanges(ODirectMemoryPointer pointer) {
    for (ChangeUnit changeUnit : changeUnits) {
      // some components work in rollback only mode, so we do not have new values.
      if (changeUnit.newValues != null)
        pointer.set(changeUnit.pageOffset, changeUnit.newValues, 0, changeUnit.newValues.length);
    }
  }

  public void revertChanges(ODirectMemoryPointer pointer) {
    ListIterator<ChangeUnit> iterator = changeUnits.listIterator(changeUnits.size());
    while (iterator.hasPrevious()) {
      ChangeUnit changeUnit = iterator.previous();
      pointer.set(changeUnit.pageOffset, changeUnit.oldValues, 0, changeUnit.oldValues.length);
    }
  }

  public int serializedSize() {
    return serializedSize;
  }

  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(changeUnits.size(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (ChangeUnit changeUnit : changeUnits) {
      offset = serializeCompressedInteger(content, offset, changeUnit.pageOffset);
      offset = serializeCompressedInteger(content, offset, changeUnit.oldValues.length);

      if (changeUnit.newValues != null) {
        content[offset] = 1;
        offset++;

        System.arraycopy(changeUnit.newValues, 0, content, offset, changeUnit.newValues.length);
        offset += changeUnit.newValues.length;
      } else
        offset++;

      System.arraycopy(changeUnit.oldValues, 0, content, offset, changeUnit.oldValues.length);
      offset += changeUnit.oldValues.length;
    }

    return offset;
  }

  public int fromStream(byte[] content, int offset) {
    final int changesSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    changeUnits = new ArrayList<ChangeUnit>(changesSize);

    int[] decompressResult;
    for (int i = 0; i < changesSize; i++) {
      decompressResult = deserializeCompressedInteger(content, offset);

      int pageOffset = decompressResult[0];
      offset = decompressResult[1];

      decompressResult = deserializeCompressedInteger(content, offset);
      int dataLength = decompressResult[0];
      offset = decompressResult[1];

      boolean newValuesIsPresent = content[offset++] > 0;

      byte[] newValues;
      if (newValuesIsPresent) {
        newValues = new byte[dataLength];
        System.arraycopy(content, offset, newValues, 0, dataLength);
        offset += dataLength;
      } else
        newValues = null;

      byte[] oldValues = new byte[dataLength];
      System.arraycopy(content, offset, oldValues, 0, dataLength);
      offset += dataLength;

      changeUnits.add(new ChangeUnit(pageOffset, oldValues, newValues));
    }

    return offset;
  }

  private int compressedIntegerSize(int value) {
    if (value <= 127)
      return 1;
    if (value <= 16383)
      return 2;
    if (value <= 2097151)
      return 3;

    throw new IllegalArgumentException("Values more than 2097151 are not supported.");
  }

  private int serializeCompressedInteger(byte[] content, int offset, int value) {
    if (value <= 127) {
      content[offset] = (byte) value;
      return offset + 1;
    }

    if (value <= 16383) {
      content[offset + 1] = (byte) (0xFF & value);

      value = value >>> 8;
      content[offset] = (byte) (0x80 | value);
      return offset + 2;
    }

    if (value <= 2097151) {
      content[offset + 2] = (byte) (0xFF & value);
      value = value >>> 8;

      content[offset + 1] = (byte) (0xFF & value);
      value = value >>> 8;

      content[offset] = (byte) (0xC0 | value);

      return offset + 3;
    }

    throw new IllegalArgumentException("Values more than 2097151 are not supported.");
  }

  private int[] deserializeCompressedInteger(byte[] content, int offset) {
    if ((content[offset] & 0x80) == 0)
      return new int[] { content[offset], offset + 1 };

    if ((content[offset] & 0xC0) == 0x80) {
      final int value = (0xFF & content[offset + 1]) | ((content[offset] & 0x3F) << 8);
      return new int[] { value, offset + 2 };
    }

    if ((content[offset] & 0xE0) == 0xC0) {
      final int value = (0xFF & content[offset + 2]) | ((0xFF & content[offset + 1]) << 8) | ((content[offset] & 0x1F) << 16);
      return new int[] { value, offset + 3 };
    }

    throw new IllegalArgumentException("Invalid integer format.");
  }

  private final static class ChangeUnit {
    private final int    pageOffset;
    private final byte[] oldValues;
    private final byte[] newValues;

    private ChangeUnit(int pageOffset, byte[] oldValues, byte[] newValues) {
      this.pageOffset = pageOffset;
      this.oldValues = oldValues;
      this.newValues = newValues;
    }
  }
}
