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
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.fetch.json.OJSONFetchContext;
import com.orientechnologies.orient.core.fetch.json.OJSONFetchListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.ORecordStringable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

@SuppressWarnings("serial")
public class ORecordSerializerJSON extends ORecordSerializerStringAbstract {

	public static final String								NAME									= "json";
	public static final ORecordSerializerJSON	INSTANCE							= new ORecordSerializerJSON();
	public static final String								ATTRIBUTE_FIELD_TYPES	= "@fieldTypes";
	public static final String								DEF_DATE_FORMAT				= "yyyy-MM-dd HH:mm:ss:SSS";
	public static final char[]								PARAMETER_SEPARATOR		= new char[] { ':', ',' };

	private SimpleDateFormat									dateFormat						= new SimpleDateFormat(DEF_DATE_FORMAT);

	@Override
	public ORecordInternal<?> fromString(String iSource, ORecordInternal<?> iRecord) {
		if (iSource == null)
			throw new OSerializationException("Error on unmarshalling JSON content: content is null");

		iSource = iSource.trim();
		if (!iSource.startsWith("{") || !iSource.endsWith("}"))
			throw new OSerializationException("Error on unmarshalling JSON content: content must be between { }");

		if (iRecord != null)
			// RESET ALL THE FIELDS
			iRecord.reset();

		iSource = iSource.substring(1, iSource.length() - 1).trim();

		final List<String> fields = OStringSerializerHelper
				.smartSplit(iSource, PARAMETER_SEPARATOR, 0, -1, true, ' ', '\n', '\r', '\t');

		if (fields.size() % 2 != 0)
			throw new OSerializationException("Error on unmarshalling JSON content: wrong format. Use <field> : <value>");

		Map<String, Character> fieldTypes = null;

		if (fields != null && fields.size() > 0) {
			// SEARCH FOR FIELD TYPES IF ANY
			for (int i = 0; i < fields.size(); i += 2) {
				final String fieldName = OStringSerializerHelper.getStringContent(fields.get(i));
				final String fieldValue = fields.get(i + 1);
				final String fieldValueAsString = OStringSerializerHelper.getStringContent(fieldValue);

				if (fieldName.equals(ATTRIBUTE_FIELD_TYPES) && iRecord instanceof ODocument) {
					// LOAD THE FIELD TYPE MAP
					final String[] fieldTypesParts = fieldValueAsString.split(",");
					if (fieldTypesParts.length > 0) {
						fieldTypes = new HashMap<String, Character>();
						String[] part;
						for (String f : fieldTypesParts) {
							part = f.split("=");
							if (part.length == 2)
								fieldTypes.put(part[0], part[1].charAt(0));
						}
					}
				} else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_TYPE)) {
					if (iRecord == null || iRecord.getRecordType() != fieldValueAsString.charAt(0)) {
						// CREATE THE RIGHT RECORD INSTANCE
						iRecord = Orient.instance().getRecordFactoryManager().newInstance((byte) fieldValueAsString.charAt(0));
					}
				}
			}

			try {
				for (int i = 0; i < fields.size(); i += 2) {
					final String fieldName = OStringSerializerHelper.getStringContent(fields.get(i));
					final String fieldValue = fields.get(i + 1);
					final String fieldValueAsString = OStringSerializerHelper.getStringContent(fieldValue);

					// RECORD ATTRIBUTES
					if (fieldName.equals(ODocumentHelper.ATTRIBUTE_RID))
						iRecord.setIdentity(new ORecordId(fieldValueAsString));

					else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_VERSION))
						iRecord.setVersion(Integer.parseInt(fieldValue));

					else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_TYPE)) {
						continue;
					} else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_CLASS) && iRecord instanceof ODocument)
						((ODocument) iRecord).setClassNameIfExists("null".equals(fieldValueAsString) ? null : fieldValueAsString);
					else if (fieldName.equals(ATTRIBUTE_FIELD_TYPES) && iRecord instanceof ODocument)
						// JUMP IT
						continue;

					// RECORD VALUE(S)
					else if (fieldName.equals("value") && !(iRecord instanceof ODocument)) {
						if ("null".equals(fieldValue))
							iRecord.fromStream(new byte[] {});
						else if (iRecord instanceof ORecordBytes) {
							// BYTES
							iRecord.fromStream(OBase64Utils.decode(fieldValueAsString));
						} else if (iRecord instanceof ORecordStringable) {
							((ORecordStringable) iRecord).value(fieldValueAsString);
						}
					} else {
						if (iRecord instanceof ODocument) {
							final Object v = getValue((ODocument) iRecord, fieldName, fieldValue, fieldValueAsString, null, null, fieldTypes);

							if (v != null)
								if (v instanceof Collection<?> && !((Collection<?>) v).isEmpty()) {
									if (v instanceof ORecordLazyList)
										((ORecordLazyList) v).setAutoConvertToRecord(false);
									else if (v instanceof OMVRBTreeRIDSet)
										((OMVRBTreeRIDSet) v).setAutoConvert(false);

									// CHECK IF THE COLLECTION IS EMBEDDED
									Object first = ((Collection<?>) v).iterator().next();
									if (first != null && first instanceof ORecord<?> && !((ORecord<?>) first).getIdentity().isValid()) {
										((ODocument) iRecord).field(fieldName, v, v instanceof Set<?> ? OType.EMBEDDEDSET : OType.EMBEDDEDLIST);
										continue;
									}
								} else if (v instanceof Map<?, ?> && !((Map<?, ?>) v).isEmpty()) {
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

			} catch (Exception e) {
				e.printStackTrace();
				throw new OSerializationException("Error on unmarshalling JSON content for record " + iRecord.getIdentity(), e);
			}
		}
		return iRecord;
	}

	@SuppressWarnings("unchecked")
	private Object getValue(final ODocument iRecord, String iFieldName, String iFieldValue, String iFieldValueAsString, OType iType,
			OType iLinkedType, final Map<String, Character> iFieldTypes) {
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
			iFieldValueAsString = iFieldValue.substring(1, iFieldValue.length() - 1);
			final String[] fields = OStringParser.getWords(iFieldValueAsString, ":,", true);
			if (fields == null || fields.length == 0)
				// EMPTY, RETURN an EMPTY HASHMAP
				return new HashMap<String, Object>();

			if (hasTypeField(fields)) {
				// OBJECT
				final ORecordInternal<?> recordInternal = fromString(iFieldValue, null);
				if (recordInternal instanceof ODocument)
					((ODocument) recordInternal).addOwner(iRecord);
				return recordInternal;
			} else {
				if (fields.length % 2 == 1)
					throw new OSerializationException("Bad JSON format on map. Expected pairs of field:value but received '"
							+ iFieldValueAsString + "'");

				// MAP
				final Map<String, Object> embeddedMap = new LinkedHashMap<String, Object>();

				for (int i = 0; i < fields.length; i += 2) {
					iFieldName = fields[i];
					if (iFieldName.length() >= 2)
						iFieldName = iFieldName.substring(1, iFieldName.length() - 1);
					iFieldValue = fields[i + 1];
					iFieldValueAsString = OStringSerializerHelper.getStringContent(iFieldValue);

					embeddedMap.put(iFieldName, getValue(iRecord, null, iFieldValue, iFieldValueAsString, iLinkedType, null, iFieldTypes));
				}
				return embeddedMap;
			}
		} else if (iFieldValue.startsWith("[") && iFieldValue.endsWith("]")) {

			// EMBEDDED VALUES
			final Collection<?> embeddedCollection;
			if (iType == OType.LINKSET)
				embeddedCollection = new OMVRBTreeRIDSet(iRecord);
			else if (iType == OType.EMBEDDEDSET)
				embeddedCollection = new OTrackedSet<Object>(iRecord);
			else if (iType == OType.LINKLIST)
				embeddedCollection = new ORecordLazyList(iRecord);
			else
				embeddedCollection = new OTrackedList<Object>(iRecord);

			iFieldValue = iFieldValue.substring(1, iFieldValue.length() - 1);

			if (!iFieldValue.isEmpty()) {
				// EMBEDDED VALUES
				List<String> items = OStringSerializerHelper.smartSplit(iFieldValue, ',');

				Object collectionItem;
				for (String item : items) {
					iFieldValue = item.trim();
					iFieldValueAsString = iFieldValue.length() >= 2 ? iFieldValue.substring(1, iFieldValue.length() - 1) : iFieldValue;

					collectionItem = getValue(iRecord, null, iFieldValue, iFieldValueAsString, iLinkedType, null, iFieldTypes);

					if (collectionItem instanceof ODocument && iRecord instanceof ODocument)
						// SET THE OWNER
						((ODocument) collectionItem).addOwner(iRecord);

					((Collection<Object>) embeddedCollection).add(collectionItem);
				}
			}

			return embeddedCollection;
		}

		if (iType == null)
			// TRY TO DETERMINE THE CONTAINED TYPE from THE FIRST VALUE
			if (iFieldValue.charAt(0) != '\"' && iFieldValue.charAt(0) != '\'') {
				if (iFieldValue.equalsIgnoreCase("false") || iFieldValue.equalsIgnoreCase("true"))
					iType = OType.BOOLEAN;
				else {
					Character c = null;
					if (iFieldTypes != null) {
						c = iFieldTypes.get(iFieldName);
						if (c != null)
							iType = ORecordSerializerStringAbstract.getType(iFieldValue + c);
					}

					if (c == null && !iFieldValue.isEmpty()) {
						// TRY TO AUTODETERMINE THE BEST TYPE
						if (iFieldValue.charAt(0) == ORID.PREFIX && iFieldValue.contains(":"))
							iType = OType.LINK;
						else if (OStringSerializerHelper.contains(iFieldValue, '.'))
							iType = OType.FLOAT;
						else
							iType = OType.INTEGER;
					}
				}
			} else if (iFieldValueAsString.startsWith("{") && iFieldValueAsString.endsWith("}"))
				iType = OType.EMBEDDED;
			else {
				if (iFieldValueAsString.length() >= 4 && iFieldValueAsString.charAt(0) == ORID.PREFIX && iFieldValueAsString.contains(":")) {
					// IS IT A LINK?
					final List<String> parts = OStringSerializerHelper.split(iFieldValueAsString, 1, -1, ':');
					if (parts.size() == 2)
						try {
							Short.parseShort(parts.get(0));
							Long.parseLong(parts.get(1));
							// YES, IT'S A LINK
							iType = OType.LINK;
						} catch (Exception e) {
						}
				}

				if (iFieldTypes != null) {
					Character c = null;
					c = iFieldTypes.get(iFieldName);
					if (c != null)
						iType = ORecordSerializerStringAbstract.getType(iFieldValueAsString, c);
				}

				if (iType == null) {
					if (iFieldValueAsString.length() == DEF_DATE_FORMAT.length())
						// TRY TO PARSE AS DATE
						try {
							synchronized (dateFormat) {
								return dateFormat.parseObject(iFieldValueAsString);
							}
						} catch (Exception e) {
						}

					iType = OType.STRING;
				}
			}

		if (iType != null)
			switch (iType) {
			case STRING:
				return decodeJSON(iFieldValueAsString);

			case LINK:
				final int pos = iFieldValueAsString.indexOf('@');
				if (pos > -1)
					// CREATE DOCUMENT
					return new ODocument(iFieldValueAsString.substring(1, pos), new ORecordId(iFieldValueAsString.substring(pos + 1)));
				else {
					// CREATE SIMPLE RID
					return new ORecordId(iFieldValueAsString);
				}

			case EMBEDDED:
				return fromString(iFieldValueAsString);

			case DATE:
			case DATETIME:
				if (iFieldValueAsString == null || iFieldValueAsString.equals(""))
					return null;
				try {
					// TRY TO PARSE AS LONG
					return Long.parseLong(iFieldValueAsString);
				} catch (NumberFormatException e) {
					try {
						// TRY TO PARSE AS DATE
						return dateFormat.parseObject(iFieldValueAsString);
					} catch (ParseException ex) {
						throw new OSerializationException("Unable to unmarshall date: " + iFieldValueAsString, e);
					}
				}
			case BINARY:
				return OStringSerializerHelper.fieldTypeFromStream(iRecord, iType, iFieldValueAsString);
			default:
				return OStringSerializerHelper.fieldTypeFromStream(iRecord, iType, iFieldValue);
			}

		return iFieldValueAsString;
	}

	private String decodeJSON(String iFieldValueAsString) {
		iFieldValueAsString = OStringParser.replaceAll(iFieldValueAsString, "\\\\", "\\");
		iFieldValueAsString = OStringParser.replaceAll(iFieldValueAsString, "\\\"", "\"");
		iFieldValueAsString = OStringParser.replaceAll(iFieldValueAsString, "\\/", "/");
		return iFieldValueAsString;
	}

	@Override
	public StringBuilder toString(final ORecordInternal<?> iRecord, final StringBuilder iOutput, final String iFormat,
			final OUserObject2RecordHandler iObjHandler, final Set<Integer> iMarshalledRecords, boolean iOnlyDelta) {
		try {
			final StringWriter buffer = new StringWriter();
			final OJSONWriter json = new OJSONWriter(buffer, iFormat);

			boolean includeVer;
			boolean includeType;
			boolean includeId;
			boolean includeClazz;
			boolean attribSameRow;
			boolean alwaysFetchEmbeddedDocuments;
			int indentLevel;
			String fetchPlan = null;
			boolean keepTypes;

			if (iFormat == null) {
				includeType = true;
				includeVer = true;
				includeId = true;
				includeClazz = true;
				attribSameRow = true;
				indentLevel = 0;
				fetchPlan = "";
				keepTypes = true;
				alwaysFetchEmbeddedDocuments = true;
			} else {
				includeType = false;
				includeVer = false;
				includeId = false;
				includeClazz = false;
				attribSameRow = false;
				alwaysFetchEmbeddedDocuments = false;
				indentLevel = 0;
				keepTypes = true;

				final String[] format = iFormat.split(",");
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
						indentLevel = Integer.parseInt(f.substring(f.indexOf(':') + 1));
					else if (f.startsWith("fetchPlan"))
						fetchPlan = f.substring(f.indexOf(':') + 1);
					else if (f.startsWith("keepTypes"))
						keepTypes = true;
					else if (f.startsWith("alwaysFetchEmbedded"))
						alwaysFetchEmbeddedDocuments = true;
			}

			json.beginObject(indentLevel);
			OJSONFetchContext context = new OJSONFetchContext(json, includeType, includeId, includeVer, includeClazz, attribSameRow,
					keepTypes, alwaysFetchEmbeddedDocuments);

			context.writeSignature(json, indentLevel, includeType, includeId, includeVer, includeClazz, attribSameRow, iRecord);

			if (iRecord instanceof ORecordSchemaAware<?>) {

				OFetchHelper.fetch(iRecord, null, OFetchHelper.buildFetchPlan(fetchPlan), new OJSONFetchListener(), context);
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
						+ "' to JSON. The record type cannot be exported to JSON");

			json.endObject(indentLevel);

			iOutput.append(buffer);
			return iOutput;
		} catch (IOException e) {
			throw new OSerializationException("Error on marshalling of record to JSON", e);
		}
	}

	private boolean hasTypeField(String[] fields) {
		for (int i = 0; i < fields.length; i = i + 2) {
			if (fields[i].equals("\"@type\"") || fields[i].equals("'@type'")) {
				return true;
			}
		}
		return false;
	}

	@Override
	public String toString() {
		return NAME;
	}
}
