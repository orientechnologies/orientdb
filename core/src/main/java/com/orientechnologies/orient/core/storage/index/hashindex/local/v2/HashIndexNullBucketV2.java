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

package com.orientechnologies.orient.core.storage.index.hashindex.local.v2;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/25/14
 */
public final class HashIndexNullBucketV2<V> extends ODurablePage {
  public HashIndexNullBucketV2(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() {
    setByteValue(NEXT_FREE_POSITION, (byte) 0);
  }

  public void setValue(final byte[] value, final byte[] oldValue) {
    assert value != null;

    setByteValue(NEXT_FREE_POSITION, (byte) 1);
    setBinaryValue(NEXT_FREE_POSITION + 1, value);
  }

  public byte[] getRawValue(final OBinarySerializer<V> valueSerializer) {
    if (getByteValue(NEXT_FREE_POSITION) == 0) {
      return null;
    }

    final int valueSize = getObjectSizeInDirectMemory(valueSerializer, NEXT_FREE_POSITION + 1);
    return getBinaryValue(NEXT_FREE_POSITION + 1, valueSize);
  }

  public V getValue(final OBinarySerializer<V> valueSerializer) {
    if (getByteValue(NEXT_FREE_POSITION) == 0) {
      return null;
    }

    return deserializeFromDirectMemory(valueSerializer, NEXT_FREE_POSITION + 1);
  }

  public void removeValue(final byte[] prevValue) {
    if (getByteValue(NEXT_FREE_POSITION) == 0) {
      return;
    }

    setByteValue(NEXT_FREE_POSITION, (byte) 0);
  }
}
