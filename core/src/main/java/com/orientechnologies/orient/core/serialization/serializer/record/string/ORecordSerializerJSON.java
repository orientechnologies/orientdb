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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.ORecordStringable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.record.impl.ORecordColumn;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

public class ORecordSerializerJSON extends ORecordSerializerStringAbstract {

	public static final String								NAME							= "json";
	public static final ORecordSerializerJSON	INSTANCE					= new ORecordSerializerJSON();

	public static final String								ATTRIBUTE_ID			= "@rid";
	public static final String								ATTRIBUTE_VERSION	= "@version";
	public static final String								ATTRIBUTE_TYPE		= "@type";
	public static final String								ATTRIBUTE_CLASS		= "@class";

	@Override
	public ORecordInternal<?> fromString(final ODatabaseRecord<?> iDatabase, final String iSource) {
		return fromString(iDatabase, iSource, null);
	}

	@Override
	public ORecordInternal<?> fromString(final ODatabaseRecord<?> iDatabase, String iSource, ORecordInternal<?> iRecord) {
		iSource = iSource.trim();
		if (!iSource.startsWith("{") || !iSource.endsWith("}"))
			throw new OSerializationException("Error on unmarshalling JSON content: content must be embraced by { }");

		if (iRecord != null)
			iRecord.setDirty();

		iSource = iSource.substring(1, iSource.length() - 1).trim();

		String[] fields = OStringParser.getWords(iSource, ":,");

		try {
			if (fields != null && fields.length > 0) {
				String fieldName;
				String fieldValue;

				for (int i = 0; i < fields.length; i += 2) {
					fieldName = fields[i];
					fieldValue = fields[i + 1];

					// RECORD ATTRIBUTES
					if (fieldName.equals(ATTRIBUTE_ID))
						iRecord.setIdentity(new ORecordId(fieldValue));

					else if (fieldName.equals(ATTRIBUTE_VERSION))
						iRecord.setVersion(Integer.parseInt(fieldValue));

					else if (fieldName.equals(ATTRIBUTE_TYPE)) {
						if (iRecord == null || iRecord.getRecordType() != fieldValue.charAt(0)) {
							// CREATE THE RIGHT RECORD INSTANCE
							iRecord = ORecordFactory.newInstance((byte) fieldValue.charAt(0));
							iRecord.setDatabase(iDatabase);
						}

					} else if (fieldName.equals(ATTRIBUTE_CLASS) && iRecord instanceof ODocument)
						((ODocument) iRecord).setClassName("null".equals(fieldValue) ? null : fieldValue);

					// RECORD VALUE(S)
					else if (fieldName.equals("value")) {
						if (iRecord instanceof ORecordColumn) {
							fieldValue = fieldValue.trim();
							if (!fieldValue.startsWith("[") || !fieldValue.endsWith("]"))
								throw new OSerializationException("Error on unmarshalling JSON content: value must be embraced by [ ]");

							fieldValue = fieldValue.substring(1, fieldValue.length() - 1).trim();

							String[] items = OStringParser.getWords(fieldValue, ",");
							for (String item : items) {
								((ORecordColumn) iRecord).add(item);
							}

						} else if (iRecord instanceof ORecordBytes) {
							// BYTES
							iRecord.fromStream(OBase64Utils.decode(OStringSerializerHelper.decode(fieldValue)));
						} else if (iRecord instanceof ORecordStringable) {
							((ORecordStringable) iRecord).value(fieldValue);
						}
					} else {
						if (iRecord instanceof ODocument)
							((ODocument) iRecord).field(fieldName, extractFieldValue((ODocument) iRecord, fieldName, fieldValue));
					}
				}
			}

		} catch (Exception e) {
			throw new OSerializationException("Error on unmarshalling JSON content", e);
		}
		return iRecord;
	}

	private Object extractFieldValue(final ODocument iRecord, String iFieldName, String iFieldValue) {
		OType type = null;
		OType linkedType = null;

		if (iRecord.getSchemaClass() != null) {
			final OProperty p = iRecord.getSchemaClass().getProperty(iFieldName);
			if (p != null) {
				type = p.getType();
				linkedType = p.getLinkedType();
			}
		}

		if (iFieldValue.equals("null"))
			return null;

		else if (iFieldValue.startsWith("{") && iFieldValue.endsWith("}")) {
			// MAP
			return ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedMapFromStream(iRecord.getDatabase(), OType.EMBEDDED, iFieldValue);

		} else if (iFieldValue.startsWith("[") && iFieldValue.endsWith("]")) {

			// EMBEDDED VALUES
			Collection<Object> embeddedCollection;
			if (type == OType.LINKSET || type == OType.EMBEDDEDSET)
				embeddedCollection = new HashSet<Object>();
			else
				embeddedCollection = new ArrayList<Object>();

			iFieldValue = iFieldValue.substring(1, iFieldValue.length() - 1);

			// EMBEDDED VALUES
			List<String> items = OStringSerializerHelper.smartSplit(iFieldValue, ',');

			Object itemToAdd;
			for (String item : items) {
				item = item.trim();
				itemToAdd = item;

				if (linkedType == null)
					// TRY TO DETERMINE THE CONTAINED TYPE from THE FIRST VALUE
					if (item.length() >= 4 && item.charAt(0) == '"' && item.charAt(1) == '#')
						linkedType = OType.LINK;
					else if (iFieldValue.startsWith("{") && iFieldValue.endsWith("}"))
						linkedType = OType.EMBEDDED;

				if (linkedType != null)
					if (linkedType == OType.LINK)
						itemToAdd = new ORecordId(item.substring(2, item.length() - 1));
					else if (linkedType == OType.EMBEDDED)
						itemToAdd = fromString(iRecord.getDatabase(), item);

				embeddedCollection.add(itemToAdd);
			}

			return embeddedCollection;
		} else {
			// OTHER
			if (linkedType == null)
				// TRY TO DETERMINE THE CONTAINED TYPE from THE FIRST VALUE
				if (iFieldValue.length() >= 4 && iFieldValue.charAt(0) == '#')
					linkedType = OType.LINK;
				else if (iFieldValue.startsWith("{") && iFieldValue.endsWith("}"))
					linkedType = OType.EMBEDDED;

			if (linkedType != null)
				if (linkedType == OType.LINK)
					return new ORecordId(iFieldValue.substring(1));
				else if (linkedType == OType.EMBEDDED)
					return fromString(iRecord.getDatabase(), iFieldValue);
		}

		return iFieldValue;
	}

	@Override
	public String toString(final ORecordInternal<?> iRecord, final String iFormat, final OUserObject2RecordHandler iObjHandler,
			final Map<ORecordInternal<?>, ORecordId> iMarshalledRecords) {
		try {
			final StringWriter buffer = new StringWriter();
			final OJSONWriter json = new OJSONWriter(buffer);

			boolean includeVer;
			boolean includeType;
			boolean includeId;
			boolean includeClazz;
			int identLevel;

			if (iFormat == null) {
				includeVer = true;
				includeType = true;
				includeId = true;
				includeClazz = true;
				identLevel = 0;
			} else {
				includeVer = false;
				includeType = true;
				includeId = false;
				includeClazz = false;
				identLevel = 0;

				String[] format = iFormat.split(",");
				for (String f : format)
					if (f.equals("id"))
						includeId = true;
					else if (f.equals("type"))
						includeType = true;
					else if (f.equals("ver"))
						includeVer = true;
					else if (f.equals("class"))
						includeClazz = true;
					else if (f.startsWith("ident"))
						identLevel = Integer.parseInt(f.substring(f.indexOf(":") + 1));
			}

			json.beginObject(identLevel);

			if (includeType)
				json.writeAttribute(identLevel + 1, true, ATTRIBUTE_TYPE, "" + (char) iRecord.getRecordType());
			if (includeId)
				json.writeAttribute(identLevel + 1, true, ATTRIBUTE_ID, iRecord.getIdentity().toString());
			if (includeVer)
				json.writeAttribute(identLevel + 1, true, ATTRIBUTE_VERSION, iRecord.getVersion());
			if (includeClazz && iRecord instanceof ORecordSchemaAware<?>)
				json.writeAttribute(identLevel + 1, true, ATTRIBUTE_CLASS, ((ORecordSchemaAware<?>) iRecord).getClassName());

			if (iRecord instanceof ORecordSchemaAware<?>) {
				// SCHEMA AWARE
				final ORecordSchemaAware<?> record = (ORecordSchemaAware<?>) iRecord;
				for (String fieldName : record.fieldNames()) {
					json.writeAttribute(identLevel + 1, true, fieldName, encode(record.field(fieldName)));
				}
			} else if (iRecord instanceof ORecordStringable) {

				// STRINGABLE
				final ORecordStringable record = (ORecordStringable) iRecord;
				json.writeAttribute(identLevel + 1, true, "value", record.value());

			} else if (iRecord instanceof ORecordBytes) {
				// BYTES
				final ORecordBytes record = (ORecordBytes) iRecord;
				json.writeAttribute(identLevel + 1, true, "value",
						OStringSerializerHelper.encode(OBase64Utils.encodeBytes(record.toStream())));
			} else

				throw new OSerializationException("Error on marshalling record of type '" + iRecord.getClass()
						+ "' to JSON. The record type can't be exported to JSON");

			json.endObject(identLevel);

			return buffer.toString();
		} catch (IOException e) {
			throw new OSerializationException("Error on marshalling of record to JSON", e);
		}
	}

	private Object encode(final Object iValue) {
		if (iValue instanceof String) {
			final String encoded = ((String) iValue).replace('"', '\'');
			return encoded;
		} else
			return iValue;
	}

	@Override
	public String toString() {
		return NAME;
	}
}
