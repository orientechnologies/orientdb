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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordTrackedList;
import com.orientechnologies.orient.core.db.record.ORecordTrackedSet;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
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
	public static final String								DEF_DATE_FORMAT		= "yyyy-MM-dd hh:mm:ss";

	private SimpleDateFormat									dateFormat				= new SimpleDateFormat(DEF_DATE_FORMAT);

	@Override
	public ORecordInternal<?> fromString(final ODatabaseRecord<?> iDatabase, final String iSource) {
		return fromString(iDatabase, iSource, null);
	}

	@Override
	public ORecordInternal<?> fromString(final ODatabaseRecord<?> iDatabase, String iSource, ORecordInternal<?> iRecord) {
		if (iSource == null)
			throw new OSerializationException("Error on unmarshalling JSON content: content is null");

		iSource = iSource.trim();
		if (!iSource.startsWith("{") || !iSource.endsWith("}"))
			throw new OSerializationException("Error on unmarshalling JSON content: content must be embraced by { }");

		if (iRecord != null)
			// RESET ALL THE FIELDS
			iRecord.reset();

		iSource = iSource.substring(1, iSource.length() - 1).trim();

		String[] fields = OStringParser.getWords(iSource, ":,", true);

		try {
			if (fields != null && fields.length > 0) {
				String fieldName;
				String fieldValue;
				String fieldValueAsString;

				for (int i = 0; i < fields.length; i += 2) {
					fieldName = fields[i];
					fieldName = fieldName.substring(1, fieldName.length() - 1);
					fieldValue = fields[i + 1];
					fieldValueAsString = fieldValue.length() >= 2 ? fieldValue.substring(1, fieldValue.length() - 1) : fieldValue;

					// RECORD ATTRIBUTES
					if (fieldName.equals(ATTRIBUTE_ID))
						iRecord.setIdentity(new ORecordId(fieldValueAsString));

					else if (fieldName.equals(ATTRIBUTE_VERSION))
						iRecord.setVersion(Integer.parseInt(fieldValue));

					else if (fieldName.equals(ATTRIBUTE_TYPE)) {
						if (iRecord == null || iRecord.getRecordType() != fieldValueAsString.charAt(0)) {
							// CREATE THE RIGHT RECORD INSTANCE
							iRecord = ORecordFactory.newInstance((byte) fieldValueAsString.charAt(0));
							iRecord.setDatabase(iDatabase);
						}

					} else if (fieldName.equals(ATTRIBUTE_CLASS) && iRecord instanceof ODocument)
						((ODocument) iRecord).setClassNameIfExists("null".equals(fieldValueAsString) ? null : fieldValueAsString);

					// RECORD VALUE(S)
					else if (fieldName.equals("value") && !(iRecord instanceof ODocument)) {
						if (iRecord instanceof ORecordColumn) {
							fieldValueAsString = fieldValueAsString.trim();

							final String[] items = OStringParser.getWords(fieldValueAsString, ",");
							for (String item : items) {
								((ORecordColumn) iRecord).add(item);
							}

						} else if (iRecord instanceof ORecordBytes) {
							// BYTES
							iRecord.fromStream(OBase64Utils.decode(fieldValueAsString));
						} else if (iRecord instanceof ORecordStringable) {
							((ORecordStringable) iRecord).value(fieldValueAsString);
						}
					} else {
						if (iRecord instanceof ODocument) {
							final Object v = getValue((ODocument) iRecord, fieldName, fieldValue, fieldValueAsString, null, null);

							if (v != null)
								if (v instanceof Collection<?> && ((Collection<?>) v).size() > 0) {
									// CHECK IF THE COLLECTION IS EMBEDDED
									Object first = ((Collection<?>) v).iterator().next();
									if (first != null && first instanceof ORecord<?> && !((ORecord<?>) first).getIdentity().isValid()) {
										((ODocument) iRecord).field(fieldName, v, v instanceof Set<?> ? OType.EMBEDDEDSET : OType.EMBEDDEDLIST);
										continue;
									}
								} else if (v instanceof Map<?, ?> && ((Map<?, ?>) v).size() > 0) {
									// CHECK IF THE MAP IS EMBEDDED
									Object first = ((Map<?, ?>) v).values().iterator().next();
									if (first != null && first instanceof ORecord<?> && !((ORecord<?>) first).getIdentity().isValid()) {
										((ODocument) iRecord).field(fieldName, v, OType.EMBEDDEDMAP);
										continue;
									}
								}

							((ODocument) iRecord).field(fieldName, v);
						}
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new OSerializationException("Error on unmarshalling JSON content", e);
		}
		return iRecord;
	}

	private Object getValue(final ODocument iRecord, String iFieldName, String iFieldValue, String iFieldValueAsString, OType iType,
			OType iLinkedType) {
		if (iFieldValue.equals("null"))
			return null;

		if (iFieldName != null)
			if (iRecord.getSchemaClass() != null) {
				final OProperty p = iRecord.getSchemaClass().getProperty(iFieldName);
				if (p != null) {
					iType = p.getType();
					iLinkedType = p.getLinkedType();
				}
			}

		if (iFieldValue.startsWith("{") && iFieldValue.endsWith("}")) {
			// OBJECT OR MAP. CHECK THE TYPE ATTRIBUTE TO KNOW IT
			String[] fields = OStringParser.getWords(iFieldValueAsString, ":,", true);
			if (fields == null || fields.length == 0)
				// EMPTY, RETURN an EMPTY HASHMAP
				return new HashMap<String, Object>();

			if (fields[0].equals("\"@type\""))
				// OBJECT
				return fromString(iRecord.getDatabase(), iFieldValue, null);
			else {
				// MAP
				final Map<String, Object> embeddedMap = new LinkedHashMap<String, Object>();

				for (int i = 0; i < fields.length; i += 2) {
					iFieldName = fields[i];
					if (iFieldName.length() >= 2)
						iFieldName = iFieldName.substring(1, iFieldName.length() - 1);
					iFieldValue = fields[i + 1];
					iFieldValueAsString = iFieldValue.length() >= 2 ? iFieldValue.substring(1, iFieldValue.length() - 1) : iFieldValue;

					embeddedMap.put(iFieldName, getValue(iRecord, null, iFieldValue, iFieldValueAsString, iLinkedType, null));
				}
				return embeddedMap;
			}
		} else if (iFieldValue.startsWith("[") && iFieldValue.endsWith("]")) {

			// EMBEDDED VALUES
			final Collection<Object> embeddedCollection;
			if (iType == OType.LINKSET || iType == OType.EMBEDDEDSET)
				embeddedCollection = new ORecordTrackedSet(iRecord);
			else
				embeddedCollection = new ORecordTrackedList(iRecord);

			iFieldValue = iFieldValue.substring(1, iFieldValue.length() - 1);

			if (iFieldValue.length() > 0) {
				// EMBEDDED VALUES
				List<String> items = OStringSerializerHelper.smartSplit(iFieldValue, ',');

				Object collectionItem;
				for (String item : items) {
					iFieldValue = item.trim();
					iFieldValueAsString = iFieldValue.length() >= 2 ? iFieldValue.substring(1, iFieldValue.length() - 1) : iFieldValue;

					collectionItem = getValue(iRecord, null, iFieldValue, iFieldValueAsString, iLinkedType, null);

					if (collectionItem instanceof ODocument && iRecord instanceof ODocument)
						// SET THE OWNER
						((ODocument) collectionItem).setOwner(iRecord);

					embeddedCollection.add(collectionItem);
				}
			}

			return embeddedCollection;
		}

		if (iType == null)
			// TRY TO DETERMINE THE CONTAINED TYPE from THE FIRST VALUE
			if (iFieldValue.charAt(0) != '\"') {
				if (iFieldValue.equalsIgnoreCase("false") || iFieldValue.equalsIgnoreCase("true"))
					iType = OType.BOOLEAN;
				else if (OStringSerializerHelper.contains(iFieldValue, '.'))
					iType = OType.DOUBLE;
				else
					iType = OType.LONG;

			} else if (iFieldValueAsString.length() >= 4 && iFieldValueAsString.charAt(0) == '#')
				iType = OType.LINK;
			else if (iFieldValueAsString.startsWith("{") && iFieldValueAsString.endsWith("}"))
				iType = OType.EMBEDDED;
			else {
				if (iFieldValueAsString.length() == DEF_DATE_FORMAT.length())
					// TRY TO PARSE AS DATE
					try {
						return dateFormat.parseObject(iFieldValueAsString);
					} catch (Exception e) {
					}

				iType = OType.STRING;
			}

		if (iType != null)
			switch (iType) {
			case STRING:
				return OStringSerializerHelper.unicode2java(iFieldValueAsString);

			case LINK:
				final int pos = iFieldValueAsString.indexOf("@");
				if (pos > -1)
					// CREATE DOCUMENT
					return new ODocument(iRecord.getDatabase(), iFieldValueAsString.substring(1, pos), new ORecordId(
							iFieldValueAsString.substring(pos + 1)));
				else
					// CREATE SIMPLE RID
					return new ORecordId(iFieldValueAsString.substring(1));

			case EMBEDDED:
				return fromString(iRecord.getDatabase(), iFieldValueAsString);

			case DATE:
				// TRY TO PARSE AS DATE
				try {
					return dateFormat.parseObject(iFieldValueAsString);
				} catch (ParseException e) {
					throw new OSerializationException("Unable to unmarshall date: " + iFieldValueAsString, e);
				}

			default:
				return OStringSerializerHelper.fieldTypeFromStream(iRecord, iType, iFieldValue);
			}

		return iFieldValueAsString;
	}

	@Override
	public String toString(final ORecordInternal<?> iRecord, final String iFormat, final OUserObject2RecordHandler iObjHandler,
			final Set<Integer> iMarshalledRecords) {
		try {
			final StringWriter buffer = new StringWriter();
			final OJSONWriter json = new OJSONWriter(buffer, iFormat);

			boolean includeVer;
			boolean includeType;
			boolean includeId;
			boolean includeClazz;
			boolean attribSameRow;
			int indentLevel;

			if (iFormat == null) {
				includeType = true;
				includeVer = true;
				includeId = true;
				includeClazz = true;
				attribSameRow = true;
				indentLevel = 0;
			} else {
				includeType = false;
				includeVer = false;
				includeId = false;
				includeClazz = false;
				attribSameRow = false;
				indentLevel = 0;

				String[] format = iFormat.split(",");
				for (String f : format)
					if (f.equals("type"))
						includeType = true;
					else if (f.equals("rid"))
						includeId = true;
					else if (f.equals("version"))
						includeVer = true;
					else if (f.equals("class"))
						includeClazz = true;
					else if (f.equals("attribSameRow"))
						attribSameRow = true;
					else if (f.startsWith("indent"))
						indentLevel = Integer.parseInt(f.substring(f.indexOf(":") + 1));
			}

			json.beginObject(indentLevel);

			boolean firstAttribute = true;

			if (includeType) {
				json.writeAttribute(firstAttribute ? indentLevel + 1 : 0, firstAttribute, ATTRIBUTE_TYPE,
						"" + (char) iRecord.getRecordType());
				if (attribSameRow)
					firstAttribute = false;
			}
			if (includeId && iRecord.getIdentity() != null && iRecord.getIdentity().isValid()) {
				json.writeAttribute(firstAttribute ? indentLevel + 1 : 0, firstAttribute, ATTRIBUTE_ID, iRecord.getIdentity().toString());
				if (attribSameRow)
					firstAttribute = false;
			}
			if (includeVer && iRecord.getVersion() > 0) {
				json.writeAttribute(firstAttribute ? indentLevel + 1 : 0, firstAttribute, ATTRIBUTE_VERSION, iRecord.getVersion());
				if (attribSameRow)
					firstAttribute = false;
			}
			if (includeClazz && iRecord instanceof ORecordSchemaAware<?> && ((ORecordSchemaAware<?>) iRecord).getClassName() != null) {
				json.writeAttribute(firstAttribute ? indentLevel + 1 : 0, firstAttribute, ATTRIBUTE_CLASS,
						((ORecordSchemaAware<?>) iRecord).getClassName());
				if (attribSameRow)
					firstAttribute = false;
			}

			if (iRecord instanceof ORecordSchemaAware<?>) {
				// SCHEMA AWARE
				final ORecordSchemaAware<?> record = (ORecordSchemaAware<?>) iRecord;
				for (String fieldName : record.fieldNames()) {
					json.writeAttribute(indentLevel + 1, true, fieldName, encode(record.field(fieldName)));
				}
			} else if (iRecord instanceof ORecordStringable) {

				// STRINGABLE
				final ORecordStringable record = (ORecordStringable) iRecord;
				json.writeAttribute(indentLevel + 1, true, "value", record.value());

			} else if (iRecord instanceof ORecordBytes) {
				// BYTES
				final ORecordBytes record = (ORecordBytes) iRecord;
				json.writeAttribute(indentLevel + 1, true, "value", OBase64Utils.encodeBytes(record.toStream()));
			} else

				throw new OSerializationException("Error on marshalling record of type '" + iRecord.getClass()
						+ "' to JSON. The record type can't be exported to JSON");

			json.endObject(indentLevel);

			return buffer.toString();
		} catch (IOException e) {
			throw new OSerializationException("Error on marshalling of record to JSON", e);
		}
	}

	private Object encode(final Object iValue) {
		if (iValue instanceof String) {
			return OStringSerializerHelper.java2unicode(((String) iValue).replace('"', '\''));
		} else
			return iValue;
	}

	@Override
	public String toString() {
		return NAME;
	}
}
