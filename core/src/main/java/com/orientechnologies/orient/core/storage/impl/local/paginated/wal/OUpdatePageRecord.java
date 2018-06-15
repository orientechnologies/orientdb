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
import com.orientechnologies.common.serialization.types.OLongSerializer;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 26.04.13
 */
public final class OUpdatePageRecord extends OAbstractPageWALRecord {
  private byte[] compressedPage;

  @SuppressWarnings("WeakerAccess")
  public OUpdatePageRecord() {
  }

  public OUpdatePageRecord(final long pageIndex, final long fileId, final OOperationUnitId operationUnitId,
      final byte[] compressedPage) {
    super(pageIndex, fileId, operationUnitId);
    this.compressedPage = compressedPage;
  }

  @Override
  public int serializedSize() {
    int serializedSize = super.serializedSize();
    serializedSize += compressedPage.length + OIntegerSerializer.INT_SIZE;

    serializedSize += 2 * OLongSerializer.LONG_SIZE;

    return serializedSize;
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);
    OIntegerSerializer.INSTANCE.serializeNative(compressedPage.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(compressedPage, 0, content, offset, compressedPage.length);
    offset += compressedPage.length;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(compressedPage.length);
    buffer.put(compressedPage);
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    final int pageLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    compressedPage = new byte[pageLen];
    System.arraycopy(content, offset, compressedPage, 0, pageLen);

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
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    final OUpdatePageRecord that = (OUpdatePageRecord) o;
    return Arrays.equals(compressedPage, that.compressedPage);
  }

  @Override
  public int hashCode() {

    int result = super.hashCode();
    result = 31 * result + Arrays.hashCode(compressedPage);
    return result;
  }
}
