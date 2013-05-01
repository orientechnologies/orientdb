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
import com.orientechnologies.common.serialization.types.OStringSerializer;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
public abstract class OAbstractPageWALRecord implements OWALRecord {
  private OLogSequenceNumber lsn;

  private long               pageIndex;
  private String             fileName;

  protected OAbstractPageWALRecord() {
  }

  protected OAbstractPageWALRecord(long pageIndex, String fileName) {
    this.pageIndex = pageIndex;
    this.fileName = fileName;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(pageIndex, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OStringSerializer.INSTANCE.serializeNative(fileName, content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(fileName);

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    fileName = OStringSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(fileName);

    return offset;
  }

  @Override
  public int serializedSize() {
    return OLongSerializer.LONG_SIZE + OStringSerializer.INSTANCE.getObjectSize(fileName);
  }

  public long getPageIndex() {
    return pageIndex;
  }

  public String getFileName() {
    return fileName;
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

    if (pageIndex != that.pageIndex)
      return false;
    if (!fileName.equals(that.fileName))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + fileName.hashCode();
    return result;
  }
}
