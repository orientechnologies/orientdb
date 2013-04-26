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
 * @since 26.04.13
 */
public class OStartAtomicPageUpdate implements OWALRecord {
  private long   pageIndex;
  private String fileName;

  public OStartAtomicPageUpdate() {
  }

  public OStartAtomicPageUpdate(long pageIndex, String fileName) {
    this.pageIndex = pageIndex;
    this.fileName = fileName;
  }

  @Override
  public void toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(pageIndex, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OStringSerializer.INSTANCE.serializeNative(fileName, content, offset);
  }

  @Override
  public void fromStream(byte[] content, int offset) {
    pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    fileName = OStringSerializer.INSTANCE.deserializeNative(content, offset);
  }

  @Override
  public int serializedSize() {
    return OLongSerializer.LONG_SIZE + OStringSerializer.INSTANCE.getObjectSize(fileName);
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public long getPageIndex() {
    return pageIndex;
  }

  @Override
  public String getFileName() {
    return fileName;
  }
}
