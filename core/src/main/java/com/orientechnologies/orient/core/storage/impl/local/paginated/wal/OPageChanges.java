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
import com.orientechnologies.orient.core.index.hashindex.local.ODirectoryPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

public class OPageChanges {
  private List<ChangeUnit> changeUnits    = new ArrayList<ChangeUnit>();
  private int              serializedSize = OIntegerSerializer.INT_SIZE;

  public void addChanges(int pageOffset, byte[] newValues, byte[] oldValues) {
    assert newValues == null || newValues.length == oldValues.length;

    changeUnits.add(new ChangeUnit(pageOffset, oldValues, newValues));

    serializedSize += 2 * OIntegerSerializer.INT_SIZE + (newValues == null ? 0 : newValues.length) + oldValues.length
        + OByteSerializer.BYTE_SIZE;
  }

  public boolean isEmpty() {
    return changeUnits.isEmpty();
  }

  public void applyChanges(ODirectMemoryPointer pointer) {
    for (ChangeUnit changeUnit : changeUnits) {
      // some components work in rollback only mode, so we do not have new values.
      if (changeUnit.newValues != null)
        pointer.set(changeUnit.pageOffset + ODurablePage.PAGE_PADDING, changeUnit.newValues, 0, changeUnit.newValues.length);
    }
  }

  public void revertChanges(ODirectMemoryPointer pointer) {
    ListIterator<ChangeUnit> iterator = changeUnits.listIterator(changeUnits.size());
    while (iterator.hasPrevious()) {
      ChangeUnit changeUnit = iterator.previous();
      pointer.set(changeUnit.pageOffset + ODurablePage.PAGE_PADDING, changeUnit.oldValues, 0, changeUnit.oldValues.length);
    }
  }

  public int serializedSize() {
    return serializedSize;
  }

  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(changeUnits.size(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (ChangeUnit changeUnit : changeUnits) {
      OIntegerSerializer.INSTANCE.serializeNative(changeUnit.pageOffset, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      OIntegerSerializer.INSTANCE.serializeNative(changeUnit.oldValues.length, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

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

    for (int i = 0; i < changesSize; i++) {
      int pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      int dataLength = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

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
