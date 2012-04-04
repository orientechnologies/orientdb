/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializer;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2long;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.long2bytes;

/**
 * Serializer for  {@link com.orientechnologies.orient.core.metadata.schema.OType#LONG}
 *
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OLongSerializer implements OBinarySerializer<Long> {

	public static OLongSerializer INSTANCE = new  OLongSerializer();
	public static final byte ID = 10;

	/**
	 * size of long value in bytes
	 */
	public static final int LONG_SIZE = 8;

	public int getObjectSize(Long object) {
		return LONG_SIZE;
	}

	public void serialize(Long object, byte[] stream, int startPosition) {
		long2bytes(object, stream, startPosition);
	}

	public Long deserialize(byte[] stream, int startPosition) {
		return bytes2long(stream, startPosition);
	}

	public int getObjectSize(byte[] stream, int startPosition) {
		return LONG_SIZE;
	}

	public byte getId() {
		return ID;
	}
}


