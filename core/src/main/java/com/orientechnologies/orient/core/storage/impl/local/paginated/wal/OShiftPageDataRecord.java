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
import com.orientechnologies.common.serialization.types.OStringSerializer;

/**
 * @author Andrey Lomakin
 * @since 26.04.13
 */
public class OShiftPageDataRecord implements OWALRecord {
  private int    from;
  private int    to;
  private int    len;

  private String fileName;
  private long   pageIndex;

  public OShiftPageDataRecord() {
  }

  public OShiftPageDataRecord(int from, int to, int len, String fileName, long pageIndex) {
    this.from = from;
    this.to = to;
    this.len = len;
    this.fileName = fileName;
    this.pageIndex = pageIndex;
  }

  @Override
  public void toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(from, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(to, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(len, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(pageIndex, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OStringSerializer.INSTANCE.serializeNative(fileName, content, offset);
  }

  @Override
  public void fromStream(byte[] content, int offset) {
    from = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    to = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    len = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    fileName = OStringSerializer.INSTANCE.deserializeNative(content, offset);
  }

  @Override
  public int serializedSize() {
    return 3 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE + OStringSerializer.INSTANCE.getObjectSize(fileName);
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public String getFileName() {
    return fileName;
  }

  @Override
  public long getPageIndex() {
    return pageIndex;
  }
}
