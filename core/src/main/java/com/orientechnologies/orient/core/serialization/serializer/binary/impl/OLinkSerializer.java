/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2long;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2short;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.long2bytes;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.short2bytes;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializer;

/**
 * Serializer for {@link com.orientechnologies.orient.core.metadata.schema.OType#LINK}
 * 
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 07.02.12
 */
public class OLinkSerializer implements OBinarySerializer<OIdentifiable> {

	public static OLinkSerializer	INSTANCE	= new OLinkSerializer();
	public static final byte			ID				= 9;

	public int getObjectSize(final OIdentifiable rid) {
		return OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
	}

	public void serialize(final OIdentifiable rid, final byte[] stream, final int startPosition) {
		ORID r = rid.getIdentity();
		short2bytes((short) r.getClusterId(), stream, startPosition);
		long2bytes(r.getClusterPosition(), stream, startPosition + OShortSerializer.SHORT_SIZE);
	}

	public ORecordId deserialize(final byte[] stream, final int startPosition) {
		return new ORecordId(bytes2short(stream, startPosition), bytes2long(stream, startPosition + OShortSerializer.SHORT_SIZE));
	}

	public int getObjectSize(final byte[] stream, final int startPosition) {
		return OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
	}

	public byte getId() {
		return ID;
	}
}
