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

package com.orientechnologies.orient.core.serialization.serializer.binary;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.*;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OSimpleKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerRID;

/**
 * This class is responsible for obtaining OBinarySerializer realization,
 * by it's id of type of object that should be serialized.
 *
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OBinarySerializerFactory {

	private final Map<Byte, OBinarySerializer<?>> serializerIdMap = new HashMap<Byte, OBinarySerializer<?>>();
	private final Map<OType, OBinarySerializer<?>> serializerTypeMap = new HashMap<OType, OBinarySerializer<?>>();

	/**
	 * Instance of the factory
	 */
	public static final OBinarySerializerFactory INSTANCE = new OBinarySerializerFactory();
	/**
	 * Size of the type identifier block size
	 */
	public static final int TYPE_IDENTIFIER_SIZE = 1;


	private OBinarySerializerFactory() {

		serializerIdMap.put(ONullSerializer.ID, new ONullSerializer());

		serializerIdMap.put(OBooleanSerializer.ID, OBooleanSerializer.INSTANCE);
		serializerIdMap.put(OIntegerSerializer.ID, OIntegerSerializer.INSTANCE);
		serializerIdMap.put(OShortSerializer.ID, OShortSerializer.INSTANCE);
		serializerIdMap.put(OLongSerializer.ID, OLongSerializer.INSTANCE);
		serializerIdMap.put(OFloatSerializer.ID, OFloatSerializer.INSTANCE);
		serializerIdMap.put(ODoubleSerializer.ID, ODoubleSerializer.INSTANCE);
		serializerIdMap.put(ODateTimeSerializer.ID, ODateTimeSerializer.INSTANCE);
		serializerIdMap.put(OCharSerializer.ID, OCharSerializer.INSTANCE);
		serializerIdMap.put(OStringSerializer.ID, OStringSerializer.INSTANCE);
		serializerIdMap.put(OByteSerializer.ID, OByteSerializer.INSTANCE);
		serializerIdMap.put(ODateSerializer.ID, ODateSerializer.INSTANCE);
		serializerIdMap.put(OLinkSerializer.ID, OLinkSerializer.INSTANCE);
		serializerIdMap.put(OCompositeKeySerializer.ID, OCompositeKeySerializer.INSTANCE);
		serializerIdMap.put(OSimpleKeySerializer.ID, OSimpleKeySerializer.INSTANCE);
		serializerIdMap.put(OStreamSerializerRID.ID, OStreamSerializerRID.INSTANCE);
		serializerIdMap.put(OBinaryTypeSerializer.ID, OBinaryTypeSerializer.INSTANCE);
		serializerIdMap.put(ODecimalSerializer.ID, ODecimalSerializer.INSTANCE);

		serializerTypeMap.put(OType.BOOLEAN, OBooleanSerializer.INSTANCE);
		serializerTypeMap.put(OType.INTEGER, OIntegerSerializer.INSTANCE);
		serializerTypeMap.put(OType.SHORT, OShortSerializer.INSTANCE);
		serializerTypeMap.put(OType.LONG, OLongSerializer.INSTANCE);
		serializerTypeMap.put(OType.FLOAT, OFloatSerializer.INSTANCE);
		serializerTypeMap.put(OType.DOUBLE, ODoubleSerializer.INSTANCE);
		serializerTypeMap.put(OType.DATETIME, ODateTimeSerializer.INSTANCE);
		serializerTypeMap.put(OType.STRING, OStringSerializer.INSTANCE);
		serializerTypeMap.put(OType.BYTE, OByteSerializer.INSTANCE);
		serializerTypeMap.put(OType.DATE, ODateSerializer.INSTANCE);
		serializerTypeMap.put(OType.LINK, OLinkSerializer.INSTANCE);
		serializerTypeMap.put(OType.BINARY, OBinaryTypeSerializer.INSTANCE);
		serializerTypeMap.put(OType.DECIMAL, ODecimalSerializer.INSTANCE);
	}

	/**
	 * Obtain OBinarySerializer instance by it's id.
	 *
	 * @param identifier is serializes identifier.
	 * @return OBinarySerializer instance.
	 */
	public OBinarySerializer getObjectSerializer(final byte identifier) {
		return serializerIdMap.get(identifier);
	}

	/**
	 * Obtain OBinarySerializer realization for the OType
	 *
	 * @param type is the OType to obtain serializer algorithm for
	 * @return OBinarySerializer instance
	 */
	public OBinarySerializer getObjectSerializer(final OType type) {
		return serializerTypeMap.get(type);
	}
}
