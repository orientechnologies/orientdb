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

package com.orientechnologies.orient.core.serialization.serializer.binary.impl.index;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;

/**
 * Serializer that is used for serialization of
 * non {@link com.orientechnologies.common.collection.OCompositeKey} keys in index.
 *
 * @author Andrey Lomakin
 * @since 31.03.12
 */
public class OSimpleKeySerializer implements OBinarySerializer<Comparable> {

	public static OSimpleKeySerializer INSTANCE = new OSimpleKeySerializer();

	public static final byte ID = 15;
	public static final String									NAME			= "bsks";

	public int getObjectSize(Comparable key) {
		final OType type = OType.getTypeByClass(key.getClass());
		if(type == OType.LINK)
			key = ((OIdentifiable)key).getIdentity();

		return OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE +
						OBinarySerializerFactory.INSTANCE.getObjectSerializer(type).getObjectSize(key);
	}

	public void serialize(Comparable key, byte[] stream, int startPosition) {
		final OType type = OType.getTypeByClass(key.getClass());
		OBinarySerializer binarySerializer = OBinarySerializerFactory.INSTANCE.getObjectSerializer(type);

		stream[startPosition] = binarySerializer.getId();
		startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

		if(type == OType.LINK)
			key = ((OIdentifiable)key).getIdentity();

		binarySerializer.serialize(key, stream, startPosition);
		startPosition += binarySerializer.getObjectSize(key);
	}

	public Comparable deserialize(byte[] stream, int startPosition) {
		final byte typeId = stream[startPosition];
		startPosition += OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

		OBinarySerializer binarySerializer = OBinarySerializerFactory.INSTANCE.getObjectSerializer(typeId);
		return (Comparable)binarySerializer.deserialize(stream, startPosition);
	}

	public int getObjectSize(byte[] stream, int startPosition) {
		final byte serializerId = stream[startPosition];

		return OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE +
						OBinarySerializerFactory.INSTANCE.getObjectSerializer(serializerId).getObjectSize(stream, startPosition +
										OBinarySerializerFactory.TYPE_IDENTIFIER_SIZE);
	}

	public byte getId() {
		return ID;
	}
}
