/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.io.IOException;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 4/25/14
 */
public class ONullBucket<V> extends ODurablePage {
  private final OBinarySerializer<V> valueSerializer;

  public ONullBucket(ODirectMemoryPointer pagePointer, TrackMode trackMode, OBinarySerializer<V> valueSerializer, boolean isNew) {
    super(pagePointer, trackMode);
    this.valueSerializer = valueSerializer;

    if (isNew)
      setByteValue(NEXT_FREE_POSITION, (byte) 0);
  }

  public void setValue(V value) throws IOException {
    setByteValue(NEXT_FREE_POSITION, (byte) 1);

    final int valueSize = valueSerializer.getObjectSize(value);

    final byte[] serializedValue = new byte[valueSize];
    valueSerializer.serializeNative(value, serializedValue, 0);

    setBinaryValue(NEXT_FREE_POSITION + 1, serializedValue);
  }

  public V getValue() {
    if (getByteValue(NEXT_FREE_POSITION) == 0)
      return null;

    return valueSerializer.deserializeFromDirectMemory(pagePointer, NEXT_FREE_POSITION + 1);
  }

  public void removeValue() {
    setByteValue(NEXT_FREE_POSITION, (byte) 0);
  }
}