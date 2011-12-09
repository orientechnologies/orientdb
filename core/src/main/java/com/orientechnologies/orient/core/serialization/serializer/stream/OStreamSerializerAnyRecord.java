/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.serialization.serializer.stream;

import java.io.IOException;
import java.lang.reflect.Constructor;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

/**
 * Allow short and long form. Examples: <br/>
 * Short form: @[type][RID] where type = 1 byte<br/>
 * Long form: org.myapp.Myrecord|[RID]<br/>
 * 
 * @author Luca Garulli
 * 
 */
public class OStreamSerializerAnyRecord implements OStreamSerializer {
	public static final String											NAME			= "ar";
	public static final OStreamSerializerAnyRecord	INSTANCE	= new OStreamSerializerAnyRecord();

	/**
	 * Re-Create any object if the class has a public constructor that accepts a String as unique parameter.
	 */
	public Object fromStream(byte[] iStream) throws IOException {
		if (iStream == null || iStream.length == 0)
			// NULL VALUE
			return null;

		final String stream = OBinaryProtocol.bytes2string(iStream);

		Class<?> cls = null;

		try {
			final StringBuilder content = new StringBuilder();
			cls = OStreamSerializerHelper.readRecordType(stream, content);

			// TRY WITH THE DATABASE CONSTRUCTOR
			for (Constructor<?> c : cls.getDeclaredConstructors()) {
				Class<?>[] params = c.getParameterTypes();

				if (params.length == 2 && params[1].equals(ORID.class)) {
					ORecord<?> rec = (ORecord<?>) c.newInstance(new ORecordId(content.toString()));
					// rec.load();
					return rec;
				}
			}
		} catch (Exception e) {
			OLogManager.instance().exception("Error on unmarshalling content. Class %s", e, OSerializationException.class, cls.getName());
		}

		OLogManager
				.instance()
				.exception(
						"Cannot unmarshall the record since the serialized object of class %s has no constructor with suitable parameters: %s(ORID)",
						null, OSerializationException.class, cls.getSimpleName(), cls.getSimpleName());

		return null;
	}

	public byte[] toStream(Object iObject) throws IOException {
		if (iObject == null)
			return null;

		if (((ORecord<?>) iObject).getIdentity() == null)
			throw new OSerializationException("Cannot serialize record without identity. Store it before serialization.");

		final StringBuilder buffer = OStreamSerializerHelper.writeRecordType(iObject.getClass(), new StringBuilder());
		buffer.append(((ORecord<?>) iObject).getIdentity().toString());

		return OBinaryProtocol.string2bytes(buffer.toString());
	}

	public String getName() {
		return NAME;
	}
}
