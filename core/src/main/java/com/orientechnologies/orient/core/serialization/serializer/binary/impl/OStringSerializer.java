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

package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2int;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.int2bytes;

import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializer;

/**
 * Serializer for {@link com.orientechnologies.orient.core.metadata.schema.OType#STRING}
 * 
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OStringSerializer implements OBinarySerializer<String> {

  public static final OStringSerializer INSTANCE = new OStringSerializer();
  public static final byte              ID       = 13;

  public int getObjectSize(final String object) {
    return object.length() * 2 + OIntegerSerializer.INT_SIZE;
  }

  public void serialize(final String object, final byte[] stream, final int startPosition) {
    final OCharSerializer charSerializer = OCharSerializer.INSTANCE;
    final int length = object.length();
    int2bytes(length, stream, startPosition);
    for (int i = 0; i < length; i++) {
      charSerializer.serialize(object.charAt(i), stream, startPosition + OIntegerSerializer.INT_SIZE + i * 2);
    }
  }

  public String deserialize(final byte[] stream, final int startPosition) {
    final OCharSerializer charSerializer = OCharSerializer.INSTANCE;
    final int len = bytes2int(stream, startPosition);
    final StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < len; i++) {
      stringBuilder.append(charSerializer.deserialize(stream, startPosition + OIntegerSerializer.INT_SIZE + i * 2));
    }
    return stringBuilder.toString();
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return bytes2int(stream, startPosition) * 2 + OIntegerSerializer.INT_SIZE;
  }

  public byte getId() {
    return ID;
  }
}
