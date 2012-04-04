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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializer;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.*;

/**
 * Serializer for  {@link com.orientechnologies.orient.core.metadata.schema.OType#LINK}
 *
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 07.02.12
 */
public class OLinkSerializer implements OBinarySerializer<ORID> {

	public static  OLinkSerializer INSTANCE = new  OLinkSerializer();
	public static final byte ID = 9;

	public int getObjectSize(ORID rid) {
		return OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
	}

	public void serialize(ORID rid, byte[] stream, int startPosition) {
		short2bytes((short)rid.getClusterId(), stream, startPosition);
		long2bytes(rid.getClusterPosition(), stream, startPosition + OShortSerializer.SHORT_SIZE);
	}

	public ORecordId deserialize(byte[] stream, int startPosition) {
		return new ORecordId(bytes2short(stream, startPosition), bytes2long(stream, startPosition + OShortSerializer.SHORT_SIZE));
	}

	public int getObjectSize(byte[] stream, int startPosition) {
		return OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
	}

	public byte getId() {
		return ID;
	}
}
