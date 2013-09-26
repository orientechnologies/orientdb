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

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
public abstract class OAbstractPageWALRecord extends OOperationUnitRecord implements OClusterAwareWALRecord {
  private OLogSequenceNumber lsn;

  private long               pageIndex;
  private int                clusterId;

  protected OAbstractPageWALRecord() {
  }

  protected OAbstractPageWALRecord(long pageIndex, int clusterId, OOperationUnitId operationUnitId) {
    super(operationUnitId);
    this.pageIndex = pageIndex;
    this.clusterId = clusterId;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(pageIndex, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(clusterId, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    clusterId = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;
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
    if (!super.equals(o))
      return false;

    OAbstractPageWALRecord that = (OAbstractPageWALRecord) o;

    if (clusterId != that.clusterId)
      return false;
    if (pageIndex != that.pageIndex)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();

    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + clusterId;
    return result;
  }

  @Override
  public String toString() {
    return "OAbstractPageWALRecord{" + "lsn=" + lsn + ", pageIndex=" + pageIndex + ", clusterId=" + clusterId + "} "
        + super.toString();
  }
}
