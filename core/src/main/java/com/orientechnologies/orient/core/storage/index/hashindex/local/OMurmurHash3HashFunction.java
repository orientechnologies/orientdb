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

import com.orientechnologies.common.hash.OMurmurHash3;
import com.orientechnologies.common.serialization.types.OBinarySerializer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 12.03.13
 */
public class OMurmurHash3HashFunction<V> implements OHashFunction<V> {
  private static final int SEED = 362498820;

  private final OBinarySerializer<V> valueSerializer;

  public OMurmurHash3HashFunction(OBinarySerializer<V> valueSerializer) {
    this.valueSerializer = valueSerializer;
  }

  @Override
  public long hashCode(final V value) {
    final byte[] serializedValue = new byte[valueSerializer.getObjectSize(value)];
    valueSerializer.serializeNativeObject(value, serializedValue, 0);

    return OMurmurHash3.murmurHash3_x64_64(serializedValue, SEED);
  }
}
