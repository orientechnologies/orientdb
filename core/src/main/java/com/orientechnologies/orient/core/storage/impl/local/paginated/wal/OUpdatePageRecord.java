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
  private OPageChanges pageChanges;

  public OUpdatePageRecord() {
  }

  public OUpdatePageRecord(final long pageIndex, final long fileId, final OOperationUnitId operationUnitId,
      final OPageChanges pageChanges, final OLogSequenceNumber prevLsn) {
    super(pageIndex, fileId, operationUnitId);
    this.pageChanges = pageChanges;
    this.lsn = prevLsn;

    assert prevLsn != null;
  }

  public OPageChanges getChanges() {
    return pageChanges;
  }

  @Override
  public int serializedSize() {
    int serializedSize = super.serializedSize();

    serializedSize += 2 * OLongSerializer.LONG_SIZE;
    serializedSize += pageChanges.serializedSize();

    return serializedSize;
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(lsn.getPosition(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(lsn.getSegment(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    offset = pageChanges.toStream(content, offset);

    return offset;
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    final long position = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    final long segment = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    lsn = new OLogSequenceNumber(segment, position);

    pageChanges = new OPageChanges();
    offset = pageChanges.fromStream(content, offset);

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

  @Override
  public String toString() {
    return toString("pageChanges=" + pageChanges);
  }
}
