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

import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;

/**
 * @author Andrey Lomakin
 * @since 26.04.13
 */
public class OSetPageDataRecord implements OWALRecord {
  private byte[] data;
  private int    pageOffset;
  private long   pageIndex;

  private String fileName;

  public OSetPageDataRecord() {
  }

  public OSetPageDataRecord(byte[] data, int pageOffset, long pageIndex, String fileName) {
    this.data = data;
    this.pageOffset = pageOffset;
    this.pageIndex = pageIndex;
    this.fileName = fileName;
  }

  @Override
  public int serializedSize() {
    return OIntegerSerializer.INT_SIZE + OStringSerializer.INSTANCE.getObjectSize(fileName)
        + OBinaryTypeSerializer.INSTANCE.getObjectSize(data) + OLongSerializer.LONG_SIZE;
  }

  @Override
  public void toStream(byte[] content, int offset) {
    OBinaryTypeSerializer.INSTANCE.serializeNative(data, content, offset);
    offset += OBinaryTypeSerializer.INSTANCE.getObjectSize(data);

    OIntegerSerializer.INSTANCE.serializeNative(pageOffset, content, offset);
    pageOffset += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(pageIndex, content, offset);
    pageOffset += OLongSerializer.LONG_SIZE;

    OStringSerializer.INSTANCE.serializeNative(fileName, content, offset);
  }

  @Override
  public void fromStream(byte[] content, int offset) {
    data = OBinaryTypeSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OBinaryTypeSerializer.INSTANCE.getObjectSize(data);

    pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    pageOffset += OIntegerSerializer.INT_SIZE;

    pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    fileName = OStringSerializer.INSTANCE.deserializeNative(content, offset);
  }

  @Override
  public long getPageIndex() {
    return pageIndex;
  }

  @Override
  public String getFileName() {
    return fileName;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }
}
