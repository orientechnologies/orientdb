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

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 26.04.13
 */
public class OUpdatePageRecord extends OAbstractPageWALRecord {
  private byte[]             compressedPage;
  /**
   * Previous value of LSN for current page.
   * This value is used when we want to rollback changes of not completed transactions after restore.
   */
  private OLogSequenceNumber prevLsn;

  @SuppressWarnings("WeakerAccess")
  public OUpdatePageRecord() {
  }

  public OUpdatePageRecord(final long pageIndex, final long fileId, final OOperationUnitId operationUnitId, byte[] compressedPage,
      final OLogSequenceNumber prevLsn) {
    super(pageIndex, fileId, operationUnitId);
    this.compressedPage = compressedPage;
    this.prevLsn = prevLsn;
  }

  public byte[] getCompressedPage() {
    return compressedPage;
  }

  @Override
  public int serializedSize() {
    int serializedSize = super.serializedSize();
    serializedSize += compressedPage.length + OIntegerSerializer.INT_SIZE;

    serializedSize += 2 * OLongSerializer.LONG_SIZE;

    return serializedSize;
  }

  /**
   * Previous value of LSN for current page.
   * This value is used when we want to rollback changes of not completed transactions at the end of restore procedure which was
   * triggered at server start after its abnormal crash.
   */
  public OLogSequenceNumber getPrevLsn() {
    return prevLsn;
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);
    OIntegerSerializer.INSTANCE.serializeNative(compressedPage.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(compressedPage, 0, content, offset, compressedPage.length);
    offset += compressedPage.length;

    OLongSerializer.INSTANCE.serializeNative(prevLsn.getSegment(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(prevLsn.getPosition(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(compressedPage.length);
    buffer.put(compressedPage);

    buffer.putLong(prevLsn.getSegment());
    buffer.putLong(prevLsn.getPosition());
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    final int pageLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    compressedPage = new byte[pageLen];
    System.arraycopy(content, offset, compressedPage, 0, pageLen);

    final long segment = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    final long position = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    prevLsn = new OLogSequenceNumber(segment, position);

    return offset;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
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

    if (lsn == null && that.lsn == null)
      return true;

    if (lsn == null)
      return false;

    if (that.lsn == null)
      return false;

    if (!lsn.equals(that.lsn))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + lsn.hashCode();
    return result;
  }
}
