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

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2int;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.int2bytes;

/**
 * Serializer for  {@link com.orientechnologies.orient.core.metadata.schema.OType#INTEGER}
 *
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 17.01.12
 */
public class OIntegerSerializer implements OBinarySerializer<Integer> {
	public static  OIntegerSerializer INSTANCE = new  OIntegerSerializer();
	public static final byte ID = 8;

	/**
	 * size of int value in bytes
	 */
	public static final int INT_SIZE = 4;

	public int getObjectSize(Integer object) {
		return INT_SIZE;
	}

	public void serialize(Integer object, byte[] stream, int startPosition) {
		int2bytes(object, stream, startPosition);
	}

	public Integer deserialize(byte[] stream, int startPosition) {
		return bytes2int(stream, startPosition);
	}

	public int getObjectSize(byte[] stream, int startPosition) {
		return INT_SIZE;
	}

	public byte getId() {
		return ID;
	}

}


