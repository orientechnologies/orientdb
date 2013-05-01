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

import java.util.Arrays;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPage;

/**
 * @author Andrey Lomakin
 * @since 5/1/13
 */
public class OWholePageRecord extends OAbstractPageWALRecord {

  private byte[] pageContent = new byte[OLocalPage.PAGE_SIZE];

  public OWholePageRecord() {
  }

  public OWholePageRecord(long pageIndex, String fileName, long pagePointer) {
    super(pageIndex, fileName);

    final ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();
    directMemory.get(pagePointer, pageContent, 0, pageContent.length);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLocalPage.PAGE_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);
    System.arraycopy(pageContent, 0, content, offset, OLocalPage.PAGE_SIZE);
    offset += OLocalPage.PAGE_SIZE;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);
    System.arraycopy(content, offset, pageContent, 0, OLocalPage.PAGE_SIZE);
    offset += OLocalPage.PAGE_SIZE;

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

    OWholePageRecord that = (OWholePageRecord) o;

    if (!Arrays.equals(pageContent, that.pageContent))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + Arrays.hashCode(pageContent);
    return result;
  }
}
