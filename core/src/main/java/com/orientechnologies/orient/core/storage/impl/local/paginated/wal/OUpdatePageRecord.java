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

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.PageSerializationType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 26.04.13
 */
public final class OUpdatePageRecord extends OAbstractPageWALRecord {
  private byte[]       page;
  private ODurablePage realPage;
  private int          pageSize;

  private PageSerializationType serializationType;

  @SuppressWarnings("WeakerAccess")
  public OUpdatePageRecord() {
  }

  public OUpdatePageRecord(final long pageIndex, final long fileId, final OOperationUnitId operationUnitId,
      final ODurablePage realPage, final PageSerializationType serializationType) {
    super(pageIndex, fileId, operationUnitId);

    this.realPage = realPage;
    this.pageSize = realPage.serializedSize();

    this.serializationType = serializationType;
  }

  public void clearRealPage() {
    this.realPage = null;
  }

  public byte[] getPage() {
    return page;
  }

  public PageSerializationType getSerializationType() {
    return serializationType;
  }

  @Override
  public int serializedSize() {
    int serializedSize = super.serializedSize();
    serializedSize += pageSize + OIntegerSerializer.INT_SIZE;

    serializedSize += OIntegerSerializer.INT_SIZE;

    return serializedSize;
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(pageSize, content, offset);

    final ByteBuffer buffer = ByteBuffer.allocate(pageSize).order(ByteOrder.nativeOrder());
    realPage.serializePage(buffer);

    System.arraycopy(buffer.array(), 0, content, offset, pageSize);
    offset += pageSize;

    OIntegerSerializer.INSTANCE.serializeNative(serializationType.ordinal(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(pageSize);
    final int oldPos = buffer.position();
    realPage.serializePage(buffer);
    buffer.position(oldPos + pageSize);

    buffer.putInt(serializationType.ordinal());
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    final int pageLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    page = new byte[pageLen];
    System.arraycopy(content, offset, page, 0, pageLen);

    final int serializationTypeId = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    serializationType = PageSerializationType.values()[serializationTypeId];

    return offset;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.UPDATE_PAGE_RECORD;
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
    return Arrays.equals(page, that.page) && serializationType == that.serializationType;
  }

  @Override
  public int hashCode() {

    int result = Objects.hash(super.hashCode(), serializationType);
    result = 31 * result + Arrays.hashCode(page);
    return result;
  }
}
