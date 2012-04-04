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

/**
 * Serializer for  {@link com.orientechnologies.orient.core.metadata.schema.OType#BOOLEAN} .
 *
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OByteSerializer implements OBinarySerializer<Byte> {
	/**
	 * size of byte value in bytes
	 */
	public static final int BYTE_SIZE = 1;

	public static OByteSerializer INSTANCE = new OByteSerializer();
	public static final byte ID = 2;

	public int getObjectSize(Byte object) {
		return BYTE_SIZE;
	}

	public void serialize(Byte object, byte[] stream, int startPosition) {
		stream[startPosition] = object;
	}

	public Byte deserialize(byte[] stream, int startPosition) {
		return stream[startPosition];
	}

	public int getObjectSize(byte[] stream, int startPosition) {
		return BYTE_SIZE;
	}

	public byte getId() {
		return ID;
	}
}

