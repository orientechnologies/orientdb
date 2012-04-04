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

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2short;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.short2bytes;

/**
 * Serializer for  {@link com.orientechnologies.orient.core.metadata.schema.OType#SHORT}
 *
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OShortSerializer implements OBinarySerializer<Short> {

	public static OShortSerializer INSTANCE = new  OShortSerializer();
	public static final byte ID = 12;

	/**
	 * size of short value in bytes
	 */
	public static final int SHORT_SIZE = 2;

	public int getObjectSize(Short object) {
		return SHORT_SIZE;
	}

	public void serialize(Short object, byte[] stream, int startPosition) {
		short2bytes(object, stream, startPosition);
	}

	public Short deserialize(byte[] stream, int startPosition) {
		return bytes2short(stream, startPosition);
	}

	public int getObjectSize(byte[] stream, int startPosition) {
		return SHORT_SIZE;
	}

	public byte getId() {
		return ID;
	}
}


