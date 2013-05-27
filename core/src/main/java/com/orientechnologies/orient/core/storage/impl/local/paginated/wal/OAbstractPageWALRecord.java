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

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
public abstract class OAbstractPageWALRecord implements OWALRecord {
  private OLogSequenceNumber lsn;

  private long               pageIndex;
  private int                clusterId;
  private OLogSequenceNumber prevUnitRecord;

  protected OAbstractPageWALRecord() {
  }

  protected OAbstractPageWALRecord(long pageIndex, int clusterId, OLogSequenceNumber prevUnitRecord) {
    this.pageIndex = pageIndex;
    this.clusterId = clusterId;
    this.prevUnitRecord = prevUnitRecord;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(pageIndex, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(clusterId, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    if (prevUnitRecord == null) {
      content[offset] = 0;
      offset++;
    } else {
      content[offset] = 1;
      offset++;

      OLongSerializer.INSTANCE.serializeNative(prevUnitRecord.getPosition(), content, offset);
      offset += OLongSerializer.LONG_SIZE;

      OIntegerSerializer.INSTANCE.serializeNative(prevUnitRecord.getSegment(), content, offset);
      offset += OIntegerSerializer.INT_SIZE;
    }

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    clusterId = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    if (content[offset] == 0) {
      offset++;
      return offset;
    }

    offset++;

    long position = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    int segment = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    prevUnitRecord = new OLogSequenceNumber(segment, position);

    return offset;
  }

  @Override
  public int serializedSize() {
    if (prevUnitRecord == null)
      return OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;

    return 2 * (OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE) + OByteSerializer.BYTE_SIZE;
  }

  public long getPageIndex() {
    return pageIndex;
  }

  public int getClusterId() {
    return clusterId;
  }

  @Override
  public OLogSequenceNumber getLsn() {
    return lsn;
  }

  @Override
  public void setLsn(OLogSequenceNumber lsn) {
    this.lsn = lsn;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OAbstractPageWALRecord that = (OAbstractPageWALRecord) o;

    if (clusterId != that.clusterId)
      return false;
    if (pageIndex != that.pageIndex)
      return false;
    if (prevUnitRecord != null ? !prevUnitRecord.equals(that.prevUnitRecord) : that.prevUnitRecord != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + clusterId;
    result = 31 * result + (prevUnitRecord != null ? prevUnitRecord.hashCode() : 0);
    return result;
  }
}
