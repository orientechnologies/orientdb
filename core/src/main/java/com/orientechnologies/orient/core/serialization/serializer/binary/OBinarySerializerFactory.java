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
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OBinaryTypeSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OBooleanSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OByteSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OCharSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ODateSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ODateTimeSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ODecimalSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ODoubleSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OFloatSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLongSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ONullSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OShortSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OStringSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OSimpleKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerRID;

/**
 * This class is responsible for obtaining OBinarySerializer realization, by it's id of type of object that should be serialized.
 * 
 * 
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OBinarySerializerFactory {

	private final Map<Byte, OBinarySerializer<?>>		serializerIdMap				= new HashMap<Byte, OBinarySerializer<?>>();
	private final Map<OType, OBinarySerializer<?>>	serializerTypeMap			= new HashMap<OType, OBinarySerializer<?>>();

	/**
	 * Instance of the factory
	 */
	public static final OBinarySerializerFactory		INSTANCE							= new OBinarySerializerFactory();
	/**
	 * Size of the type identifier block size
	 */
	public static final int													TYPE_IDENTIFIER_SIZE	= 1;

	private OBinarySerializerFactory() {

		registerSerializer(ONullSerializer.ID, new ONullSerializer(), null);

		registerSerializer(OBooleanSerializer.ID, OBooleanSerializer.INSTANCE, OType.BOOLEAN);
		registerSerializer(OIntegerSerializer.ID, OIntegerSerializer.INSTANCE, OType.INTEGER);
		registerSerializer(OShortSerializer.ID, OShortSerializer.INSTANCE, OType.SHORT);
		registerSerializer(OLongSerializer.ID, OLongSerializer.INSTANCE, OType.LONG);
		registerSerializer(OFloatSerializer.ID, OFloatSerializer.INSTANCE, OType.FLOAT);
		registerSerializer(ODoubleSerializer.ID, ODoubleSerializer.INSTANCE, OType.DOUBLE);
		registerSerializer(ODateTimeSerializer.ID, ODateTimeSerializer.INSTANCE, OType.DATETIME);
		registerSerializer(OCharSerializer.ID, OCharSerializer.INSTANCE, null);
		registerSerializer(OStringSerializer.ID, OStringSerializer.INSTANCE, OType.STRING);
		registerSerializer(OByteSerializer.ID, OByteSerializer.INSTANCE, OType.BYTE);
		registerSerializer(ODateSerializer.ID, ODateSerializer.INSTANCE, OType.DATE);
		registerSerializer(OLinkSerializer.ID, OLinkSerializer.INSTANCE, OType.LINK);
		registerSerializer(OCompositeKeySerializer.ID, OCompositeKeySerializer.INSTANCE, null);
		registerSerializer(OSimpleKeySerializer.ID, OSimpleKeySerializer.INSTANCE, null);
		registerSerializer(OStreamSerializerRID.ID, OStreamSerializerRID.INSTANCE, null);
		registerSerializer(OBinaryTypeSerializer.ID, OBinaryTypeSerializer.INSTANCE, OType.BINARY);
		registerSerializer(ODecimalSerializer.ID, ODecimalSerializer.INSTANCE, OType.DECIMAL);
	}

	public void registerSerializer(final byte iId, final OBinarySerializer<?> iInstance, final OType iType) {
		serializerIdMap.put(iId, iInstance);
		if (iType != null)
			serializerTypeMap.put(iType, iInstance);
	}

	/**
	 * Obtain OBinarySerializer instance by it's id.
	 * 
	 * @param identifier
	 *          is serializes identifier.
	 * @return OBinarySerializer instance.
	 */
	public OBinarySerializer getObjectSerializer(final byte identifier) {
		return serializerIdMap.get(identifier);
	}

	/**
	 * Obtain OBinarySerializer realization for the OType
	 * 
	 * @param type
	 *          is the OType to obtain serializer algorithm for
	 * @return OBinarySerializer instance
	 */
	public OBinarySerializer getObjectSerializer(final OType type) {
		return serializerTypeMap.get(type);
	}
}
