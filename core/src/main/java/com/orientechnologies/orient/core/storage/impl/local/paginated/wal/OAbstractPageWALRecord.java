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

import com.orientechnologies.common.serialization.types.OLongSerializer;
import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 29.04.13
 */
public abstract class OAbstractPageWALRecord extends OOperationUnitBodyRecord {
  private long pageIndex;
  private long fileId;

  protected OAbstractPageWALRecord() {}

  protected OAbstractPageWALRecord(long pageIndex, long fileId, long operationUnitId) {
    super(operationUnitId);
    this.pageIndex = pageIndex;
    this.fileId = fileId;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    buffer.putLong(pageIndex);
    buffer.putLong(fileId);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    pageIndex = buffer.getLong();
    fileId = buffer.getLong();
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
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    OAbstractPageWALRecord that = (OAbstractPageWALRecord) o;

    if (pageIndex != that.pageIndex) return false;
    return fileId == that.fileId;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + (int) (fileId ^ (fileId >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return toString("pageIndex=" + pageIndex + ", fileId=" + fileId);
  }
}
