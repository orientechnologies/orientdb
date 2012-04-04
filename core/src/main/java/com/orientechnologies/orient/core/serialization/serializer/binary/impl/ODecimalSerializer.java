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

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Serializer for  {@link com.orientechnologies.orient.core.metadata.schema.OType#DECIMAL}
 *
 * @author Andrey Lomakin
 * @since 03.04.12
 */
public class ODecimalSerializer implements OBinarySerializer<BigDecimal> {
	public static final ODecimalSerializer INSTANCE = new ODecimalSerializer();
	public static final byte ID = 18;

	public int getObjectSize(BigDecimal object) {
		return OIntegerSerializer.INT_SIZE +
						OBinaryTypeSerializer.INSTANCE.getObjectSize(object.unscaledValue().toByteArray());
	}

	public int getObjectSize(byte[] stream, int startPosition) {
		final int size = OIntegerSerializer.INT_SIZE +
						OBinaryTypeSerializer.INSTANCE.getObjectSize(stream, startPosition + OIntegerSerializer.INT_SIZE);
		return size;
	}

	public void serialize(BigDecimal object, byte[] stream, int startPosition) {
		OIntegerSerializer.INSTANCE.serialize(object.scale(), stream, startPosition);
		startPosition += OIntegerSerializer.INT_SIZE;
		OBinaryTypeSerializer.INSTANCE.serialize(object.unscaledValue().toByteArray(), stream, startPosition);

	}

	public BigDecimal deserialize(byte[] stream, int startPosition) {
		final int scale  = OIntegerSerializer.INSTANCE.deserialize(stream, startPosition);
		startPosition += OIntegerSerializer.INT_SIZE;

		final byte[] unscaledValue = OBinaryTypeSerializer.INSTANCE.deserialize(stream, startPosition);

		return new BigDecimal(new BigInteger(unscaledValue), scale);
	}

	public byte getId() {
		return ID;
	}
}
