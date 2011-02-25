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
package com.orientechnologies.orient.core.serialization.serializer.record;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;

public class ORecordSerializerFactory {
	private static final ORecordSerializerFactory	instance				= new ORecordSerializerFactory();

	private Map<String, ORecordSerializer>				implementations	= new HashMap<String, ORecordSerializer>();
	private ORecordSerializer											defaultRecordFormat;

	public ORecordSerializerFactory() {
		defaultRecordFormat = new ORecordSerializerRaw();

		implementations.put(ORecordSerializerSchemaAware2CSV.NAME, new ORecordSerializerSchemaAware2CSV());
		implementations.put(ORecordSerializerJSON.NAME, new ORecordSerializerJSON());
		implementations.put(ORecordSerializerRaw.NAME, defaultRecordFormat);
	}

	public Collection<ORecordSerializer> getFormats() {
		return implementations.values();
	}

	public ORecordSerializer getFormat(final String iFormatName) {
		if (iFormatName == null)
			return null;

		return implementations.get(iFormatName);
	}

	public ORecordSerializer getFormatForObject(final Object iObject, final String iFormatName) {
		if (iObject == null)
			return null;

		ORecordSerializer recordFormat = null;
		if (iFormatName != null)
			recordFormat = implementations.get(iObject.getClass().getSimpleName() + "2" + iFormatName);

		if (recordFormat == null)
			recordFormat = defaultRecordFormat;

		return recordFormat;
	}

	public ORecordSerializer getDefaultRecordFormat() {
		return defaultRecordFormat;
	}

	public void setDefaultRecordFormat(final ORecordSerializer iDefaultFormat) {
		this.defaultRecordFormat = iDefaultFormat;
	}

	public static ORecordSerializerFactory instance() {
		return instance;
	}
}
