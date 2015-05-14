/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.index.sbtree.local;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;

import java.io.IOException;

/**
 * 
 * Bucket which is intended to save values stored in sbtree under <code>null</code> key. Bucket has following layout:
 * <ol>
 * <li>First byte is flag which indicates presence of value in bucket</li>
 * <li>Second byte indicates whether value is presented by link to the "bucket list" where actual value is stored or real value
 * passed be user.</li>
 * <li>The rest is serialized value whether link or passed in value.</li>
 * </ol>
 * 
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 4/15/14
 */
public class ONullBucket<V> extends ODurablePage {
  private final OBinarySerializer<V> valueSerializer;

  public ONullBucket(OCacheEntry cacheEntry, OWALChangesTree changesTree, OBinarySerializer<V> valueSerializer, boolean isNew) {
    super(cacheEntry, changesTree);
    this.valueSerializer = valueSerializer;

    if (isNew)
      setByteValue(NEXT_FREE_POSITION, (byte) 0);
  }

  public void setValue(OSBTreeValue<V> value) throws IOException {
    setByteValue(NEXT_FREE_POSITION, (byte) 1);

    if (value.isLink()) {
      setByteValue(NEXT_FREE_POSITION + 1, (byte) 0);
      setLongValue(NEXT_FREE_POSITION + 2, value.getLink());
    } else {
      final int valueSize = valueSerializer.getObjectSize(value.getValue());

      final byte[] serializedValue = new byte[valueSize];
      valueSerializer.serializeNativeObject(value.getValue(), serializedValue, 0);

      setByteValue(NEXT_FREE_POSITION + 1, (byte) 1);
      setBinaryValue(NEXT_FREE_POSITION + 2, serializedValue);
    }
  }

  public OSBTreeValue<V> getValue() {
    if (getByteValue(NEXT_FREE_POSITION) == 0)
      return null;

    final boolean isLink = getByteValue(NEXT_FREE_POSITION + 1) == 0;
    if (isLink)
      return new OSBTreeValue<V>(true, getLongValue(NEXT_FREE_POSITION + 2), null);

    return new OSBTreeValue<V>(false, -1, deserializeFromDirectMemory(valueSerializer, NEXT_FREE_POSITION + 2));
  }

  public void removeValue() {
    setByteValue(NEXT_FREE_POSITION, (byte) 0);
  }
}
