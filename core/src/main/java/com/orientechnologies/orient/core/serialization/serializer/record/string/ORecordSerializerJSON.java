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
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;

public class ORecordSerializerJSON extends ORecordSerializerStringAbstract {

	public static final String								NAME			= "json";
	public static final ORecordSerializerJSON	INSTANCE	= new ORecordSerializerJSON();

	@Override
	public ORecordInternal<?> fromString(ODatabaseRecord<?> iDatabase, String iSource, ORecordInternal<?> iRecord) {
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
	public String toString(ORecordInternal<?> iRecord, OUserObject2RecordHandler iObjHandler,
			Map<ORecordInternal<?>, ORecordId> iMarshalledRecords) {
		if (!(iRecord instanceof ORecordSchemaAware<?>))
			throw new OSerializationException("Can't marshall a record of type " + iRecord.getClass().getSimpleName() + " to JSON");

		ORecordSchemaAware<?> record = (ORecordSchemaAware<?>) iRecord;

		try {
			StringWriter buffer = new StringWriter();
			OJSONWriter json = new OJSONWriter(buffer);

			json.beginObject();
			for (String fieldName : record.fieldNames()) {
				json.writeAttribute(1, true, fieldName, record.field(fieldName));
			}
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
