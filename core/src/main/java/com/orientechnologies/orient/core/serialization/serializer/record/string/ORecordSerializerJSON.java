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
package com.orientechnologies.orient.core.serialization.serializer.record.string;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.ORecordStringable;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;

public class ORecordSerializerJSON extends ORecordSerializerStringAbstract {

	public static final String								NAME							= "json";
	public static final ORecordSerializerJSON	INSTANCE					= new ORecordSerializerJSON();

	private static final String								ATTRIBUTE_ID			= "_id";
	private static final String								ATTRIBUTE_VERSION	= "_ver";
	private static final String								ATTRIBUTE_CLASS		= "_class";

	@Override
	public ORecordInternal<?> fromString(final ODatabaseRecord<?> iDatabase, final String iSource, final ORecordInternal<?> iRecord) {
		try {
			StringWriter buffer = new StringWriter();
			OJSONWriter json = new OJSONWriter(buffer);

			json.beginObject();

			json.endObject();

		} catch (IOException e) {
			throw new OSerializationException("Error on unmarshalling of record from JSON", e);
		}
		return iRecord;
	}

	@Override
	public String toString(final ORecordInternal<?> iRecord, final String iFormat, final OUserObject2RecordHandler iObjHandler,
			final Map<ORecordInternal<?>, ORecordId> iMarshalledRecords) {
		try {
			final StringWriter buffer = new StringWriter();
			final OJSONWriter json = new OJSONWriter(buffer);

			boolean includeVer;
			boolean includeId;
			boolean includeClazz;

			if (iFormat == null) {
				includeVer = true;
				includeId = true;
				includeClazz = true;
			} else {
				includeVer = false;
				includeId = false;
				includeClazz = false;

				String[] format = iFormat.split(",");
				for (String f : format)
					if (f.equals("id"))
						includeId = true;
					else if (f.equals("ver"))
						includeVer = true;
					else if (f.equals("class"))
						includeClazz = true;
			}

			json.beginObject();

			if (includeId)
				json.writeAttribute(1, true, ATTRIBUTE_ID, iRecord.getIdentity());
			if (includeVer)
				json.writeAttribute(1, true, ATTRIBUTE_VERSION, iRecord.getVersion());
			if (includeClazz && iRecord instanceof ORecordSchemaAware<?>)
				json.writeAttribute(1, true, ATTRIBUTE_CLASS, ((ORecordSchemaAware<?>) iRecord).getClassName());

			if (iRecord instanceof ORecordSchemaAware<?>) {
				// SCHEMA AWARE
				final ORecordSchemaAware<?> record = (ORecordSchemaAware<?>) iRecord;
				for (String fieldName : record.fieldNames()) {
					json.writeAttribute(1, true, fieldName, record.field(fieldName));
				}
			} else if (iRecord instanceof ORecordStringable) {

				// STRINGABLE
				final ORecordStringable record = (ORecordStringable) iRecord;
				json.writeAttribute(1, true, "value", record.value());
			} else
				throw new OSerializationException("Error on marshalling record of type '" + iRecord.getClass()
						+ "' to JSON. The record type can't be exported to JSON");

			json.endObject();

			return buffer.toString();
		} catch (IOException e) {
			throw new OSerializationException("Error on marshalling of record to JSON", e);
		}
	}

	@Override
	public String toString() {
		return NAME;
	}
}
