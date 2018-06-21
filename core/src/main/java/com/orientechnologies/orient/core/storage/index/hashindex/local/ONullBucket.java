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

package com.orientechnologies.orient.core.storage.index.hashindex.local;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.PageSerializationType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/25/14
 */
public final class ONullBucket<V> extends ODurablePage {
  private final OBinarySerializer<V> valueSerializer;

  public ONullBucket(OCacheEntry cacheEntry) {
    super(cacheEntry);

    valueSerializer = null;
  }

  public ONullBucket(final OCacheEntry cacheEntry, final OBinarySerializer<V> valueSerializer, final boolean isNew) {
    super(cacheEntry);
    this.valueSerializer = valueSerializer;

    if (isNew) {
      buffer.put(NEXT_FREE_POSITION, (byte) 0);
      cacheEntry.markDirty();
    }
  }

  public void setValue(final byte[] value) {
    buffer.position(NEXT_FREE_POSITION);
    buffer.put((byte) 1);
    buffer.put(value);

    cacheEntry.markDirty();
  }

  public V getValue() {
    if (buffer.get(NEXT_FREE_POSITION) == 0) {
      return null;
    }

    final ByteBuffer buffer = getBufferDuplicate();
    buffer.position(NEXT_FREE_POSITION + 1);
    return valueSerializer.deserializeFromByteBufferObject(buffer);
  }

  public byte[] getRawValue() {
    if (buffer.get(NEXT_FREE_POSITION) == 0) {
      return null;
    }

    buffer.position(NEXT_FREE_POSITION + 1);
    final int valueLen = valueSerializer.getObjectSizeInByteBuffer(buffer);

    final ByteBuffer buffer = getBufferDuplicate();

    final byte[] value = new byte[valueLen];
    buffer.position(NEXT_FREE_POSITION + 1);
    buffer.get(value);

    return value;
  }

  public void removeValue() {
    buffer.put(NEXT_FREE_POSITION, (byte) 0);
    cacheEntry.markDirty();
  }

  @Override
  public int serializedSize() {
    if (this.buffer.get(NEXT_FREE_POSITION) == 0) {
      return NEXT_FREE_POSITION + 1;
    }

    this.buffer.position(NEXT_FREE_POSITION + 1);
    final int valueLen = valueSerializer.getObjectSizeInByteBuffer(this.buffer);

    return valueLen + NEXT_FREE_POSITION + 1;
  }

  @Override
  public void serializePage(ByteBuffer recordBuffer) {
    assert buffer.limit() == buffer.capacity();

    if (this.buffer.get(NEXT_FREE_POSITION) == 0) {
      this.buffer.position(0);
      this.buffer.limit(NEXT_FREE_POSITION + 1);
      recordBuffer.put(this.buffer);
      this.buffer.limit(this.buffer.capacity());
      return;
    }

    this.buffer.position(NEXT_FREE_POSITION + 1);
    final int valueLen = valueSerializer.getObjectSizeInByteBuffer(this.buffer);

    this.buffer.position(0);
    this.buffer.limit(valueLen + NEXT_FREE_POSITION + 1);
    recordBuffer.put(this.buffer);
    this.buffer.limit(this.buffer.capacity());
  }

  @Override
  public void deserializePage(byte[] page) {
    assert buffer.limit() == buffer.capacity();

    buffer.position(0);
    buffer.put(page);

    cacheEntry.markDirty();
  }

  @Override
  protected PageSerializationType serializationType() {
    return PageSerializationType.HASH_NULL_BUCKET;
  }
}
