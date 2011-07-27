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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.ORecordStringable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

public class ORecordSerializerJSON extends ORecordSerializerStringAbstract {

	public static final String								NAME									= "json";
	public static final ORecordSerializerJSON	INSTANCE							= new ORecordSerializerJSON();
	public static final String								ATTRIBUTE_ID					= "@rid";
	public static final String								ATTRIBUTE_VERSION			= "@version";
	public static final String								ATTRIBUTE_TYPE				= "@type";
	public static final String								ATTRIBUTE_FIELD_TYPES	= "@fieldTypes";
	public static final String								ATTRIBUTE_CLASS				= "@class";
	public static final String								DEF_DATE_FORMAT				= "yyyy-MM-dd HH:mm:ss:SSS";

	private SimpleDateFormat									dateFormat						= new SimpleDateFormat(DEF_DATE_FORMAT);

	@Override
	public ORecordInternal<?> fromString(final ODatabaseRecord iDatabase, final String iSource) {
		return fromString(iDatabase, iSource, null);
	}

	@Override
	public ORecordInternal<?> fromString(final ODatabaseRecord iDatabase, String iSource, ORecordInternal<?> iRecord) {
		if (iSource == null)
			throw new OSerializationException("Error on unmarshalling JSON content: content is null");

		iSource = iSource.trim();
		if (!iSource.startsWith("{") || !iSource.endsWith("}"))
			throw new OSerializationException("Error on unmarshalling JSON content: content must be between { }");

		if (iRecord != null)
			// RESET ALL THE FIELDS
			iRecord.reset();

		iSource = iSource.substring(1, iSource.length() - 1).trim();

		final String[] fields = OStringParser.getWords(iSource, ":,", true);

		String fieldName;
		String fieldValue;
		String fieldValueAsString;
		Map<String, Character> fieldTypes = null;

		// SEARCH FOR FIELD TYPES IF ANY
		if (fields != null && fields.length > 0) {
			for (int i = 0; i < fields.length; i += 2) {
				fieldName = fields[i];
				fieldName = fieldName.substring(1, fieldName.length() - 1);
				fieldValue = fields[i + 1];
				fieldValueAsString = fieldValue.length() >= 2 ? fieldValue.substring(1, fieldValue.length() - 1) : fieldValue;

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
				} else if (fieldName.equals(ATTRIBUTE_TYPE)) {
					if (iRecord == null || iRecord.getRecordType() != fieldValueAsString.charAt(0)) {
						// CREATE THE RIGHT RECORD INSTANCE
						iRecord = Orient.instance().getRecordFactoryManager().newInstance(iDatabase, (byte) fieldValueAsString.charAt(0));
					}
				}
			}

			try {
				for (int i = 0; i < fields.length; i += 2) {
					fieldName = fields[i];
					fieldName = fieldName.substring(1, fieldName.length() - 1);
					fieldValue = fields[i + 1].trim();
					fieldValueAsString = fieldValue.length() >= 2 ? fieldValue.substring(1, fieldValue.length() - 1) : fieldValue;

					// RECORD ATTRIBUTES
					if (fieldName.equals(ATTRIBUTE_ID))
						iRecord.setIdentity(new ORecordId(fieldValueAsString));

					else if (fieldName.equals(ATTRIBUTE_VERSION))
						iRecord.setVersion(Integer.parseInt(fieldValue));

					else if (fieldName.equals(ATTRIBUTE_TYPE)) {
						continue;
					} else if (fieldName.equals(ATTRIBUTE_CLASS) && iRecord instanceof ODocument)
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
								if (v instanceof Collection<?> && ((Collection<?>) v).size() > 0) {
									if (v instanceof ORecordLazySet)
										((ORecordLazySet) v).setAutoConvertToRecord(false);
									else if (v instanceof ORecordLazyList)
										((ORecordLazyList) v).setAutoConvertToRecord(false);

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
			String[] fields = OStringParser.getWords(iFieldValueAsString, ":,", true);
			if (fields == null || fields.length == 0)
				// EMPTY, RETURN an EMPTY HASHMAP
				return new HashMap<String, Object>();

			if (hasTypeField(fields))
				// OBJECT
				return fromString(iRecord.getDatabase(), iFieldValue, null);
			else {
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
					iFieldValueAsString = iFieldValue.length() >= 2 ? iFieldValue.substring(1, iFieldValue.length() - 1) : iFieldValue;

					embeddedMap.put(iFieldName, getValue(iRecord, null, iFieldValue, iFieldValueAsString, iLinkedType, null, iFieldTypes));
				}
				return embeddedMap;
			}
		} else if (iFieldValue.startsWith("[") && iFieldValue.endsWith("]")) {

			// EMBEDDED VALUES
			final Collection<?> embeddedCollection;
			if (iType == OType.LINKSET)
				embeddedCollection = new ORecordLazySet(iRecord);
			else if (iType == OType.EMBEDDEDSET)
				embeddedCollection = new OTrackedSet<Object>(iRecord);
			else if (iType == OType.LINKLIST)
				embeddedCollection = new ORecordLazyList(iRecord);
			else
				embeddedCollection = new OTrackedList<Object>(iRecord);

			iFieldValue = iFieldValue.substring(1, iFieldValue.length() - 1);

			if (iFieldValue.length() > 0) {
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

					if (c == null && iFieldValue.length() > 0) {
						// TRY TO AUTODETERMINE THE BEST TYPE
						if (iFieldValue.charAt(0) == ORID.PREFIX && iFieldValue.contains(":"))
							iType = OType.LINK;
						else if (OStringSerializerHelper.contains(iFieldValue, '.'))
							iType = OType.FLOAT;
						else
							iType = OType.INTEGER;
					}
				}
			} else if (iFieldValueAsString.length() >= 4 && iFieldValueAsString.charAt(0) == ORID.PREFIX
					&& iFieldValueAsString.contains(":"))
				iType = OType.LINK;
			else if (iFieldValueAsString.startsWith("{") && iFieldValueAsString.endsWith("}"))
				iType = OType.EMBEDDED;
			else {
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

		if (iType != null)
			switch (iType) {
			case STRING:
				return iFieldValueAsString;

			case LINK:
				final int pos = iFieldValueAsString.indexOf('@');
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

			default:
				return OStringSerializerHelper.fieldTypeFromStream(iRecord, iType, iFieldValue);
			}

		return iFieldValueAsString;
	}

	@Override
	public StringBuilder toString(final ORecordInternal<?> iRecord, final StringBuilder iOutput, final String iFormat,
			final OUserObject2RecordHandler iObjHandler, final Set<Integer> iMarshalledRecords, boolean iOnlyDelta) {
		try {
			final StringWriter buffer = new StringWriter();
			final OJSONWriter json = new OJSONWriter(buffer, iFormat);
			final Set<ORID> parsedRecords = new HashSet<ORID>();

			boolean includeVer;
			boolean includeType;
			boolean includeId;
			boolean includeClazz;
			boolean attribSameRow;
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
				keepTypes = false;
			} else {
				includeType = false;
				includeVer = false;
				includeId = false;
				includeClazz = false;
				attribSameRow = false;
				indentLevel = 0;
				keepTypes = false;

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
			}

			json.beginObject(indentLevel);

			writeSignature(json, indentLevel, includeType, includeId, includeVer, includeClazz, attribSameRow, iRecord);

			if (iRecord instanceof ORecordSchemaAware<?>) {
				// SCHEMA AWARE
				final ORecordSchemaAware<?> record = (ORecordSchemaAware<?>) iRecord;
				parsedRecords.add(iRecord.getIdentity());

				Map<String, Integer> fetchPlanMap = null;
				if (fetchPlan != null && fetchPlan.length() > 0)
					fetchPlanMap = OFetchHelper.buildFetchPlan(fetchPlan);

				processRecord(json, indentLevel, includeType, includeId, includeVer, includeClazz, attribSameRow, record, fetchPlanMap,
						keepTypes, 0, -1, parsedRecords);
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
			parsedRecords.clear();

			iOutput.append(buffer);
			return iOutput;
		} catch (IOException e) {
			throw new OSerializationException("Error on marshalling of record to JSON", e);
		}
	}

	private void processRecord(final OJSONWriter json, int indentLevel, boolean includeType, boolean includeId, boolean includeVer,
			boolean includeClazz, boolean attribSameRow, final ORecordSchemaAware<?> record, Map<String, Integer> iFetchPlan,
			boolean keepTypes, final int iCurrentLevel, final int iMaxFetch, final Set<ORID> parsedRecords) throws IOException {
		Object fieldValue;

		final StringBuilder types = new StringBuilder();

		for (String fieldName : record.fieldNames()) {
			if (iFetchPlan == null) {
				final Object v = record.field(fieldName);
				if (keepTypes) {
					if (v instanceof Long)
						appendType(types, fieldName, 'l');
					else if (v instanceof Float)
						appendType(types, fieldName, 'f');
					else if (v instanceof Short)
						appendType(types, fieldName, 's');
					else if (v instanceof Double)
						appendType(types, fieldName, 'd');
					else if (v instanceof Date)
						appendType(types, fieldName, 't');
					else if (v instanceof Byte)
						appendType(types, fieldName, 'b');
				}
				json.writeAttribute(indentLevel + 1, true, fieldName, OJSONWriter.encode(v));
			} else {
				final Integer depthLevel = getDepthLevel(record, iFetchPlan, fieldName);
				if (depthLevel != null) {
					if (depthLevel == 0) {
						// NO FETCH THIS FIELD PLEASE
						continue;
					}
					if (depthLevel > -1 && depthLevel >= iCurrentLevel) {
						// MAX DEPTH REACHED: STOP TO FETCH THIS FIELD
						continue;
					}
				}
				fieldValue = record.field(fieldName);
				if (fieldValue == null
						|| !(fieldValue instanceof OIdentifiable)
						&& (!(fieldValue instanceof Collection<?>) || ((Collection<?>) fieldValue).size() == 0 || !(((Collection<?>) fieldValue)
								.iterator().next() instanceof OIdentifiable))
						&& (!(fieldValue instanceof Map<?, ?>) || ((Map<?, ?>) fieldValue).size() == 0 || !(((Map<?, ?>) fieldValue).values()
								.iterator().next() instanceof OIdentifiable))) {
					json.writeAttribute(indentLevel + 1, true, fieldName, OJSONWriter.encode(fieldValue));
				} else {
					try {
						fetch(record, iFetchPlan, fieldValue, fieldName, iCurrentLevel + 1, iMaxFetch, json, indentLevel, includeType,
								includeId, includeVer, includeClazz, attribSameRow, keepTypes, parsedRecords);
					} catch (Exception e) {
						e.printStackTrace();
						OLogManager.instance().error(null, "Fetching error on record %s", e, record.getIdentity());
					}
				}
			}
		}

		if (keepTypes && types.length() > 0)
			json.writeAttribute(indentLevel + 1, true, ATTRIBUTE_FIELD_TYPES, types.toString());
	}

	private void appendType(final StringBuilder iBuffer, final String iFieldName, final char iType) {
		if (iBuffer.length() > 0)
			iBuffer.append(',');
		iBuffer.append(iFieldName);
		iBuffer.append('=');
		iBuffer.append(iType);
	}

	private void writeSignature(final OJSONWriter json, int indentLevel, boolean includeType, boolean includeId, boolean includeVer,
			boolean includeClazz, boolean attribSameRow, final ORecordInternal<?> record) throws IOException {
		boolean firstAttribute = true;
		if (includeType) {
			json.writeAttribute(firstAttribute ? indentLevel + 1 : 0, firstAttribute, ATTRIBUTE_TYPE, "" + (char) record.getRecordType());
			if (attribSameRow)
				firstAttribute = false;
		}
		if (includeId && record.getIdentity() != null && record.getIdentity().isValid()) {
			json.writeAttribute(!firstAttribute ? indentLevel + 1 : 0, firstAttribute, ATTRIBUTE_ID, record.getIdentity().toString());
			if (attribSameRow)
				firstAttribute = false;
		}
		if (includeVer) {
			json.writeAttribute(firstAttribute ? indentLevel + 1 : 0, firstAttribute, ATTRIBUTE_VERSION, record.getVersion());
			if (attribSameRow)
				firstAttribute = false;
		}
		if (includeClazz && record instanceof ORecordSchemaAware<?> && ((ORecordSchemaAware<?>) record).getClassName() != null) {
			json.writeAttribute(firstAttribute ? indentLevel + 1 : 0, firstAttribute, ATTRIBUTE_CLASS,
					((ORecordSchemaAware<?>) record).getClassName());
			if (attribSameRow)
				firstAttribute = false;
		}
	}

	private void fetch(final ORecordSchemaAware<?> iRootRecord, final Map<String, Integer> iFetchPlan, final Object fieldValue,
			final String fieldName, final int iCurrentLevel, final int iMaxFetch, final OJSONWriter json, int indentLevel,
			boolean includeType, final boolean includeId, final boolean includeVer, final boolean includeClazz,
			final boolean attribSameRow, final boolean keepTypes, final Set<ORID> parsedRecords) throws IOException {
		Integer depthLevel;
		final Integer anyFieldDepthLevel = iFetchPlan != null ? iFetchPlan.get("*") : -1;
		depthLevel = getDepthLevel(iRootRecord, iFetchPlan, fieldName);

		if (depthLevel == null)
			// NO SPECIFIED: ASSIGN DEFAULT LEVEL TAKEN FROM * WILDCARD IF ANY
			depthLevel = anyFieldDepthLevel;

		if (depthLevel == 0)
			// NO FETCH THIS FIELD PLEASE
			return;

		if (depthLevel > -1 && iCurrentLevel >= depthLevel)
			// MAX DEPTH REACHED: STOP TO FETCH THIS FIELD
			return;

		if (fieldValue == null) {
			json.writeAttribute(indentLevel + 1, true, fieldName, null);
		} else if (fieldValue instanceof ODocument) {
			fetchDocument(iFetchPlan, fieldValue, fieldName, iCurrentLevel, iMaxFetch, json, indentLevel, includeType, includeId,
					includeVer, includeClazz, attribSameRow, keepTypes, parsedRecords);
		} else if (fieldValue instanceof Collection<?>) {
			fetchCollection(iRootRecord.getDatabase(), iFetchPlan, fieldValue, fieldName, iCurrentLevel, iMaxFetch, json, indentLevel,
					includeType, includeId, includeVer, includeClazz, attribSameRow, keepTypes, parsedRecords);
		} else if (fieldValue.getClass().isArray()) {
			fetchArray(iFetchPlan, fieldValue, fieldName, iCurrentLevel, iMaxFetch, json, indentLevel, includeType, includeId,
					includeVer, includeClazz, attribSameRow, keepTypes, parsedRecords);
		} else if (fieldValue instanceof Map<?, ?>) {
			fetchMap(iFetchPlan, fieldValue, fieldName, iCurrentLevel, iMaxFetch, json, indentLevel, includeType, includeId, includeVer,
					includeClazz, attribSameRow, keepTypes, parsedRecords);
		}
		if (iMaxFetch > -1 && iCurrentLevel >= iMaxFetch) {
			// MAX FETCH SIZE REACHED: STOP TO FETCH AT ALL
			return;
		}

	}

	@SuppressWarnings("unchecked")
	private void fetchMap(Map<String, Integer> iFetchPlan, Object fieldValue, String fieldName, final int iCurrentLevel,
			final int iMaxFetch, final OJSONWriter json, final int indentLevel, final boolean includeType, final boolean includeId,
			final boolean includeVer, final boolean includeClazz, final boolean attribSameRow, final boolean keepTypes,
			final Set<ORID> parsedRecords) throws IOException {
		final Map<String, ODocument> linked = (Map<String, ODocument>) fieldValue;
		json.beginObject(indentLevel + 1, true, fieldValue);
		for (ODocument d : (linked).values()) {
			// GO RECURSIVELY
			if (!parsedRecords.contains((d).getIdentity())) {
				parsedRecords.add((d).getIdentity());
				json.beginObject(indentLevel + 1, true, null);
				writeSignature(json, indentLevel, includeType, includeId, includeVer, includeClazz, attribSameRow, d);
				processRecord(json, indentLevel, includeType, includeId, includeVer, includeClazz, attribSameRow, d, iFetchPlan, keepTypes,
						iCurrentLevel + 1, iMaxFetch, parsedRecords);
				json.endObject(indentLevel + 1, true);
			} else {
				json.writeValue(indentLevel + 1, false, OJSONWriter.encode(d));
			}
		}
		json.endObject(indentLevel + 1, true);
	}

	private void fetchArray(final Map<String, Integer> iFetchPlan, final Object fieldValue, final String fieldName,
			final int iCurrentLevel, final int iMaxFetch, final OJSONWriter json, final int indentLevel, final boolean includeType,
			final boolean includeId, final boolean includeVer, final boolean includeClazz, final boolean attribSameRow,
			final boolean keepTypes, final Set<ORID> parsedRecords) throws IOException {
		if (fieldValue instanceof ODocument[]) {
			final ODocument[] linked = (ODocument[]) fieldValue;
			json.beginCollection(indentLevel + 1, true, fieldName);
			for (ODocument d : linked) {
				// GO RECURSIVELY
				if (!parsedRecords.contains((d).getIdentity())) {
					parsedRecords.add((d).getIdentity());
					json.beginObject(indentLevel + 1, true, null);
					writeSignature(json, indentLevel, includeType, includeId, includeVer, includeClazz, attribSameRow, d);
					processRecord(json, indentLevel, includeType, includeId, includeVer, includeClazz, attribSameRow, d, iFetchPlan,
							keepTypes, iCurrentLevel + 1, iMaxFetch, parsedRecords);
					json.endObject(indentLevel + 1, true);
				} else {
					json.writeValue(indentLevel + 1, false, OJSONWriter.encode(d));
				}
			}
			json.endCollection(indentLevel + 1, false);
		} else {
			json.writeAttribute(indentLevel + 1, true, fieldName, null);
		}
	}

	@SuppressWarnings("unchecked")
	private void fetchCollection(final ODatabaseRecord iDatabase, final Map<String, Integer> iFetchPlan, final Object fieldValue,
			final String fieldName, final int iCurrentLevel, final int iMaxFetch, final OJSONWriter json, final int indentLevel,
			final boolean includeType, final boolean includeId, final boolean includeVer, final boolean includeClazz,
			final boolean attribSameRow, final boolean keepTypes, final Set<ORID> parsedRecords) throws IOException {
		final Collection<ODocument> linked = (Collection<ODocument>) fieldValue;
		json.beginCollection(indentLevel + 1, true, fieldName);
		for (OIdentifiable d : linked) {
			// GO RECURSIVELY
			if (!parsedRecords.contains((d).getIdentity())) {
				parsedRecords.add((d).getIdentity());
				if (d instanceof ORecordId) {
					d = iDatabase.load((ORecordId) d);
				}
				if (!(d instanceof ODocument)) {
					json.writeValue(indentLevel + 1, false, OJSONWriter.encode(d));
				} else {
					json.beginObject(indentLevel + 1, true, null);
					writeSignature(json, indentLevel, includeType, includeId, includeVer, includeClazz, attribSameRow, (ODocument) d);
					processRecord(json, indentLevel, includeType, includeId, includeVer, includeClazz, attribSameRow, (ODocument) d,
							iFetchPlan, keepTypes, iCurrentLevel + 1, iMaxFetch, parsedRecords);
					json.endObject(indentLevel + 1, true);
				}
			} else {
				json.writeValue(indentLevel + 1, false, OJSONWriter.encode(d));
			}
		}
		json.endCollection(indentLevel + 1, false);
	}

	private void fetchDocument(Map<String, Integer> iFetchPlan, Object fieldValue, String fieldName, final int iCurrentLevel,
			final int iMaxFetch, final OJSONWriter json, int indentLevel, boolean includeType, boolean includeId, boolean includeVer,
			boolean includeClazz, boolean attribSameRow, boolean keepTypes, final Set<ORID> parsedRecords) throws IOException {
		if (!parsedRecords.contains(((ODocument) fieldValue).getIdentity())) {
			parsedRecords.add(((ODocument) fieldValue).getIdentity());
			final ODocument linked = (ODocument) fieldValue;
			json.beginObject(indentLevel + 1, true, fieldName);
			writeSignature(json, indentLevel, includeType, includeId, includeVer, includeClazz, attribSameRow, linked);
			processRecord(json, indentLevel, includeType, includeId, includeVer, includeClazz, attribSameRow, linked, iFetchPlan,
					keepTypes, iCurrentLevel + 1, iMaxFetch, parsedRecords);
			json.endObject(indentLevel + 1, true);
		} else {
			json.writeAttribute(indentLevel + 1, true, fieldName, OJSONWriter.encode(fieldValue));
		}
	}

	private Integer getDepthLevel(final ORecordSchemaAware<?> record, final Map<String, Integer> iFetchPlan, final String iFieldName) {
		Integer depthLevel;

		if (iFetchPlan != null) {
			// GET THE FETCH PLAN FOR THE GENERIC FIELD IF SPECIFIED
			depthLevel = iFetchPlan.get(iFieldName);

			if (depthLevel == null) {
				OClass cls = record.getSchemaClass();
				while (cls != null && depthLevel == null) {
					depthLevel = iFetchPlan.get(cls.getName() + "." + iFieldName);

					if (depthLevel == null)
						cls = cls.getSuperClass();
				}
			}
		} else
			// INFINITE
			depthLevel = -1;

		return depthLevel;
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
