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
 * @since 29.04.13
 */
public abstract class OAbstractPageWALRecord extends OOperationUnitRecord {
  private long pageIndex;
  private long fileId;

  protected OAbstractPageWALRecord() {
  }

  protected OAbstractPageWALRecord(long pageIndex, long fileId, OOperationUnitId operationUnitId) {
    super(operationUnitId);
    this.pageIndex = pageIndex;
    this.fileId = fileId;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(pageIndex, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(fileId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    fileId = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OLongSerializer.LONG_SIZE;
  }

  public long getPageIndex() {
    return pageIndex;
  }

  public long getFileId() {
    return fileId;
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

    if (fileId != that.fileId)
      return false;
    if (pageIndex != that.pageIndex)
      return false;
    if (lsn != null ? !lsn.equals(that.lsn) : that.lsn != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (lsn != null ? lsn.hashCode() : 0);
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + (int) (fileId ^ (fileId >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return toString("pageIndex=" + pageIndex + ", fileId=" + fileId);
  }
}
