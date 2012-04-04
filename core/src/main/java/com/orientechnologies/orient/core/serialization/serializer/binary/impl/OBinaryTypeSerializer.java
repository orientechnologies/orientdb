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

import java.util.Arrays;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2int;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.int2bytes;

/**
 * Serializer for  {@link com.orientechnologies.orient.core.metadata.schema.OType#BINARY} .
 *
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 20.01.12
 */
public class OBinaryTypeSerializer implements OBinarySerializer<byte[]> {

	public static final OBinaryTypeSerializer INSTANCE = new OBinaryTypeSerializer();
	public static final byte ID = 17;

	public int getObjectSize(int length) {
		return length + OIntegerSerializer.INT_SIZE;
	}

	public int getObjectSize(byte[] object) {
		return object.length + OIntegerSerializer.INT_SIZE;
	}

	public void serialize(byte[] object, byte[] stream, int startPosition) {
		int len = object.length;
		int2bytes(len, stream, startPosition);
		System.arraycopy(object, 0, stream, startPosition + OIntegerSerializer.INT_SIZE, len);
	}

	public byte[] deserialize(byte[] stream, int startPosition) {
		int len = bytes2int(stream, startPosition);
		return Arrays.copyOfRange(stream, startPosition + OIntegerSerializer.INT_SIZE, startPosition + OIntegerSerializer.INT_SIZE + len);
	}

	public int getObjectSize(byte[] stream, int startPosition) {
		return  bytes2int(stream, startPosition);
	}

	public byte getId() {
		return ID;
	}
}


