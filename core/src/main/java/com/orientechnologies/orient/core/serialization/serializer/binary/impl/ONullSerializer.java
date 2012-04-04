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
 * Serialize and deserialize null values
 *
 * <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class ONullSerializer implements OBinarySerializer<Object> {

	public static ONullSerializer INSTANCE = new  ONullSerializer();
	public static final byte ID = 11;

	public int getObjectSize(final Object object) {
		return 0;
	}

	public void serialize(final Object object, final byte[] stream, final int startPosition) {
		//nothing to serialize
	}

	public Object deserialize(final byte[] stream, final int startPosition) {
		//nothing to deserialize
		return null;
	}

	public int getObjectSize(byte[] stream, int startPosition) {
		return 0;
	}

	public byte getId() {
		return ID;
	}
}

