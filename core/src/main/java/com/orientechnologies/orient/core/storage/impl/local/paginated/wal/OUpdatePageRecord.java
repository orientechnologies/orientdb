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

import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 26.04.13
 */
public class OUpdatePageRecord extends OAbstractPageWALRecord {
  private OPageChanges       pageChanges;
  private OLogSequenceNumber prevLsn;

  public OUpdatePageRecord() {
  }

  public OUpdatePageRecord(long pageIndex, long fileId, OOperationUnitId operationUnitId, OPageChanges pageChanges,
      OLogSequenceNumber prevLsn) {
    super(pageIndex, fileId, operationUnitId);
    this.pageChanges = pageChanges;
    this.prevLsn = prevLsn;

    assert prevLsn != null;
  }

  public OPageChanges getChanges() {
    return pageChanges;
  }

  public OLogSequenceNumber getPrevLsn() {
    return prevLsn;
  }

  @Override
  public int serializedSize() {
    int serializedSize = super.serializedSize();

    serializedSize += 2 * OLongSerializer.LONG_SIZE;
    serializedSize += pageChanges.serializedSize();

    return serializedSize;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(prevLsn.getPosition(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(prevLsn.getSegment(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    offset = pageChanges.toStream(content, offset);

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    long position = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    long segment = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    prevLsn = new OLogSequenceNumber(segment, position);

    pageChanges = new OPageChanges();
    offset = pageChanges.fromStream(content, offset);

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

    if (!prevLsn.equals(that.prevLsn))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + prevLsn.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "OUpdatePageRecord{" + "pageChanges=" + pageChanges + ", prevLsn=" + prevLsn + '}';
  }
}
