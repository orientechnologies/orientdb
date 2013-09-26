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

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;

/**
 * @author Andrey Lomakin
 * @since 27.05.13
 */
public class OIntPageDiff extends OPageDiff<Integer> {
  public OIntPageDiff() {
  }

  public OIntPageDiff(Integer newValue, int pageOffset) {
    super(newValue, pageOffset);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int toStream(byte[] stream, int offset) {
    offset = super.toStream(stream, offset);
    OIntegerSerializer.INSTANCE.serializeNative(newValue, stream, offset);

    return offset + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int fromStream(byte[] stream, int offset) {
    offset = super.fromStream(stream, offset);
    newValue = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
    return offset + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public void restorePageData(long pagePointer) {
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(newValue, directMemory, pagePointer + pageOffset);
  }
}
