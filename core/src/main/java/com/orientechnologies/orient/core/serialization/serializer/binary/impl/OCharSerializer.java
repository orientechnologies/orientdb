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
 *
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OCharSerializer implements OBinarySerializer<Character> {
	/**
	 * size of char value in bytes
	 */
	public static final int CHAR_SIZE = 2;

	public static OCharSerializer INSTANCE = new OCharSerializer();
	public static final byte ID = 3;

	public int getObjectSize(Character object) {
		return CHAR_SIZE;
	}

	public void serialize(Character object, byte[] stream, int startPosition) {
		stream[startPosition] = (byte) (object >>> 8);
		stream[startPosition + 1] = (byte) (object.charValue());
	}

	public Character deserialize(byte[] stream, int startPosition) {
		return (char) (
						((stream[startPosition] & 0xFF) << 8) +
										(stream[startPosition + 1] & 0xFF)
		);
	}

	public int getObjectSize(byte[] stream, int startPosition) {
		return CHAR_SIZE;
	}

	public byte getId() {
		return ID;
	}

}
