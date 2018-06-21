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
package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.PageSerializationType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.nio.ByteBuffer;

/**
 * Bucket which is intended to save values stored in sbtree under <code>null</code> key. Bucket has following layout:
 * <ol>
 * <li>First byte is flag which indicates presence of value in bucket</li>
 * <li>Second byte indicates whether value is presented by link to the "bucket list" where actual value is stored or real value
 * passed be user.</li>
 * <li>The rest is serialized value whether link or passed in value.</li>
 * </ol>
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/15/14
 */
public final class ONullBucket<V> extends ODurablePage {
  private final OBinarySerializer<V> valueSerializer;

  public ONullBucket(final OCacheEntry cacheEntry) {
    super(cacheEntry);

    valueSerializer = null;
  }

  ONullBucket(final OCacheEntry cacheEntry, final OBinarySerializer<V> valueSerializer, final boolean isNew) {
    super(cacheEntry);
    this.valueSerializer = valueSerializer;

    if (isNew) {
      buffer.put(NEXT_FREE_POSITION, (byte) 0);
      cacheEntry.markDirty();
    }
  }

  public void setValue(final OSBTreeValue<V> value) {
    buffer.put(NEXT_FREE_POSITION, (byte) 1);

    if (value.isLink()) {
      buffer.put(NEXT_FREE_POSITION + 1, (byte) 0);
      buffer.putLong(NEXT_FREE_POSITION + 2, value.getLink());
    } else {
      final int valueSize = valueSerializer.getObjectSize(value.getValue());

      final byte[] serializedValue = new byte[valueSize];
      valueSerializer.serializeNativeObject(value.getValue(), serializedValue, 0);

      buffer.put(NEXT_FREE_POSITION + 1, (byte) 1);

      buffer.position(NEXT_FREE_POSITION + 2);
      buffer.put(serializedValue);
    }

    cacheEntry.markDirty();
  }

  void setRawValue(final byte[] rawValue) {
    buffer.put(NEXT_FREE_POSITION, (byte) 1);

    buffer.put(NEXT_FREE_POSITION + 1, (byte) 1);

    buffer.position(NEXT_FREE_POSITION + 2);
    buffer.put(rawValue);

    cacheEntry.markDirty();
  }

  public OSBTreeValue<V> getValue() {
    if (buffer.get(NEXT_FREE_POSITION) == 0) {
      return null;
    }

    final boolean isLink = buffer.get(NEXT_FREE_POSITION + 1) == 0;
    if (isLink) {
      return new OSBTreeValue<>(true, buffer.getLong(NEXT_FREE_POSITION + 2), null);
    }

    final ByteBuffer buffer = getBufferDuplicate();
    buffer.position(NEXT_FREE_POSITION + 2);
    return new OSBTreeValue<>(false, -1, valueSerializer.deserializeFromByteBufferObject(buffer));
  }

  byte[] getRawValue() {
    if (buffer.get(NEXT_FREE_POSITION) == 0) {
      return null;
    }

    assert buffer.get(NEXT_FREE_POSITION + 1) > 0;

    final ByteBuffer buffer = getBufferDuplicate();
    buffer.position(NEXT_FREE_POSITION + 2);
    final int valueLen = valueSerializer.getObjectSizeInByteBuffer(buffer);

    buffer.position(NEXT_FREE_POSITION + 2);
    final byte[] value = new byte[valueLen];
    buffer.get(value);
    return value;
  }

  void removeValue() {
    buffer.put(NEXT_FREE_POSITION, (byte) 0);
    cacheEntry.markDirty();
  }

  @Override
  public int serializedSize() {
    if (this.buffer.get(NEXT_FREE_POSITION) == 0) {
      return NEXT_FREE_POSITION + 1;
    }

    this.buffer.position(NEXT_FREE_POSITION + 2);
    final int valueLen = valueSerializer.getObjectSizeInByteBuffer(this.buffer);
    final int size = NEXT_FREE_POSITION + 2 + valueLen;

    return size;
  }

  @Override
  public void serializePage(ByteBuffer recordBuffer) {
    if (this.buffer.get(NEXT_FREE_POSITION) == 0) {
      this.buffer.position(0);

      this.buffer.limit(NEXT_FREE_POSITION + 1);
      recordBuffer.put(this.buffer);
      this.buffer.limit(this.buffer.capacity());
      return;
    }

    this.buffer.position(NEXT_FREE_POSITION + 2);
    final int valueLen = valueSerializer.getObjectSizeInByteBuffer(this.buffer);

    this.buffer.position(0);
    this.buffer.limit(NEXT_FREE_POSITION + 2 + valueLen);
    recordBuffer.put(this.buffer);
    this.buffer.limit(this.buffer.capacity());
  }

  @Override
  public void deserializePage(final byte[] page) {
    buffer.position(0);
    buffer.put(page);

    cacheEntry.markDirty();
  }

  @Override
  protected PageSerializationType serializationType() {
    return PageSerializationType.SBTREE_NULL_BUCKET;
  }
}
