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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.entity.OEntityManagerInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializerHelper;

@SuppressWarnings("unchecked")
public abstract class ORecordSerializerCSVAbstract extends ORecordSerializerStringAbstract {
	private static final char		DECIMAL_SEPARATOR			= '.';
	public static final String	FIELD_VALUE_SEPARATOR	= ":";

	protected abstract ORecordSchemaAware<?> newObject(ODatabaseRecord<?> iDatabase, String iClassName);

	public Object fieldFromStream(final ODatabaseRecord<?> iDatabase, final OType iType, OClass iLinkedClass, OType iLinkedType,
			final String iName, final String iValue) {

		if (iValue == null)
			return null;

		switch (iType) {
		case EMBEDDEDLIST:
		case EMBEDDEDSET:
			return embeddedCollectionFromStream(iDatabase, iType, iLinkedClass, iLinkedType, iValue);

		case LINKLIST:
		case LINKSET: {
			if (iValue.length() == 0)
				return null;

			// REMOVE BEGIN & END COLLECTIONS CHARACTERS IF IT'S A COLLECTION
			String value = iValue.startsWith("[") ? iValue.substring(1, iValue.length() - 1) : iValue;

			Collection<Object> coll = iType == OType.LINKLIST ? new ArrayList<Object>() : new HashSet<Object>();

			if (value.length() == 0)
				return coll;

			String[] items = OStringSerializerHelper.split(value, OStringSerializerHelper.RECORD_SEPARATOR_AS_CHAR);

			for (String item : items) {
				// GET THE CLASS NAME IF ANY
				int classSeparatorPos = value.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
				if (classSeparatorPos > -1) {
					String className = value.substring(1, classSeparatorPos);
					if (className != null) {
						iLinkedClass = iDatabase.getMetadata().getSchema().getClass(className);
						item = item.substring(classSeparatorPos + 1);
					}
				} else
					item = item.substring(1);

				coll.add(new ODocument(iDatabase, iLinkedClass != null ? iLinkedClass.getName() : null, new ORecordId(item)));
			}

			return coll;
		}
		case EMBEDDEDMAP: {
			if (iValue.length() == 0)
				return null;

			// REMOVE BEGIN & END MAP CHARACTERS
			String value = iValue.substring(1, iValue.length() - 1);

			Map<String, Object> map = new HashMap<String, Object>();

			if (value.length() == 0)
				return map;

			String[] items = OStringSerializerHelper.split(value, OStringSerializerHelper.RECORD_SEPARATOR_AS_CHAR);

			// EMBEDDED LITERALS
			String[] entry;
			String mapValue;
			for (String item : items) {
				if (item != null && item.length() > 0) {
					entry = item.split(OStringSerializerHelper.ENTRY_SEPARATOR);
					if (entry.length > 0) {
						mapValue = entry[1];

						if (iLinkedType == null) {
							if (mapValue.length() > 0) {
								if (mapValue.startsWith(OStringSerializerHelper.LINK)) {
									iLinkedType = OType.LINK;

									// GET THE CLASS NAME IF ANY
									int classSeparatorPos = value.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
									if (classSeparatorPos > -1) {
										String className = value.substring(1, classSeparatorPos);
										if (className != null)
											iLinkedClass = iDatabase.getMetadata().getSchema().getClass(className);
									}
								} else if (mapValue.charAt(0) == OStringSerializerHelper.EMBEDDED) {
									iLinkedType = OType.EMBEDDED;
								} else if (Character.isDigit(mapValue.charAt(0)) || mapValue.charAt(0) == '+' || mapValue.charAt(0) == '-') {
									iLinkedType = getNumber(mapValue);
								} else if (mapValue.charAt(0) == '\'' || mapValue.charAt(0) == '"')
									iLinkedType = OType.STRING;
							} else
								iLinkedType = OType.EMBEDDED;
						}

						map.put((String) OStringSerializerHelper.fieldTypeFromStream(OType.STRING, entry[0]),
								OStringSerializerHelper.fieldTypeFromStream(iLinkedType, mapValue));
					}

				}
			}

			return map;
		}
		case LINK:
			if (iValue.length() > 1) {
				int pos = iValue.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
				if (pos > -1)
					iLinkedClass = iDatabase.getMetadata().getSchema().getClass(iValue.substring(OStringSerializerHelper.LINK.length(), pos));
				else
					pos = 0;

				// if (iLinkedClass == null)
				// throw new IllegalArgumentException("Linked class not specified in ORID field: " + iValue);

				ORecordId recId = new ORecordId(iValue.substring(pos + 1));
				return new ODocument(iDatabase, iLinkedClass != null ? iLinkedClass.getName() : null, recId);
			} else
				return null;

		default:
			return OStringSerializerHelper.fieldTypeFromStream(iType, iValue);
		}
	}

	public String fieldToStream(final ODocument iRecord, final ODatabaseComplex<?> iDatabase,
			final OUserObject2RecordHandler iObjHandler, final OType iType, final OClass iLinkedClass, final OType iLinkedType,
			final String iName, final Object iValue, final Map<ORecordInternal<?>, ORecordId> iMarshalledRecords) {
		StringBuilder buffer = new StringBuilder();

		switch (iType) {
		case EMBEDDED:
			if (iValue instanceof ODocument)
				buffer.append(toString((ODocument) iValue, null, iObjHandler, iMarshalledRecords));
			else if (iValue != null)
				buffer.append(iValue.toString());
			break;

		case LINK: {
			linkToStream(buffer, iRecord, iValue);
			break;
		}

		case EMBEDDEDLIST:
		case EMBEDDEDSET: {
			embeddedCollectionToStream(iDatabase, iObjHandler, iLinkedClass, iLinkedType, iValue, iMarshalledRecords, buffer);
			break;
		}

		case EMBEDDEDMAP: {
			buffer.append(OStringSerializerHelper.MAP_BEGIN);

			int items = 0;
			if (iLinkedClass != null) {
				// EMBEDDED OBJECTS
				ODocument record;
				for (Entry<String, Object> o : ((Map<String, Object>) iValue).entrySet()) {
					if (items > 0)
						buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

					if (o != null) {
						if (o instanceof ODocument)
							record = (ODocument) o;
						else
							record = OObjectSerializerHelper.toStream(o, new ODocument((ODatabaseRecord<?>) iDatabase, o.getClass()
									.getSimpleName()), iDatabase instanceof ODatabaseObjectTx ? ((ODatabaseObjectTx) iDatabase).getEntityManager()
									: OEntityManagerInternal.INSTANCE, iLinkedClass, iObjHandler != null ? iObjHandler
									: new OUserObject2RecordHandler() {

										public Object getUserObjectByRecord(ORecordInternal<?> iRecord, final String iFetchPlan) {
											return iRecord;
										}

										public ORecordInternal<?> getRecordByUserObject(Object iPojo, boolean iIsMandatory) {
											return new ODocument(iLinkedClass);
										}

										public boolean existsUserObjectByRecord(ORecordInternal<?> iRecord) {
											return false;
										}
									});

						buffer.append(OStringSerializerHelper.EMBEDDED);
						buffer.append(toString(record, null, iObjHandler, iMarshalledRecords));
						buffer.append(OStringSerializerHelper.EMBEDDED);
					}

					items++;
				}
			} else
				// EMBEDDED LITERALS
				for (Entry<String, Object> o : ((Map<String, Object>) iValue).entrySet()) {
					if (items > 0)
						buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

					buffer.append(OStringSerializerHelper.fieldTypeToString(OType.STRING, o.getKey()));
					buffer.append(OStringSerializerHelper.ENTRY_SEPARATOR);
					buffer.append(OStringSerializerHelper.fieldTypeToString(iLinkedType, o.getValue()));
					items++;
				}

			buffer.append(OStringSerializerHelper.MAP_END);
			break;
		}

		case LINKLIST:
		case LINKSET: {
			buffer.append(OStringSerializerHelper.COLLECTION_BEGIN);

			int items = 0;
			// LINKED OBJECTS
			for (Object link : (Collection<Object>) iValue) {
				if (items++ > 0)
					buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

				linkToStream(buffer, iRecord, link);
			}

			buffer.append(OStringSerializerHelper.COLLECTION_END);
			break;
		}

		default:
			return OStringSerializerHelper.fieldTypeToString(iType, iValue);
		}

		return buffer.toString();
	}

	public Object embeddedCollectionFromStream(final ODatabaseRecord<?> iDatabase, final OType iType, OClass iLinkedClass,
			OType iLinkedType, final String iValue) {
		if (iValue.length() == 0)
			return null;

		// REMOVE BEGIN & END COLLECTIONS CHARACTERS IF IT'S A COLLECTION
		String value = iValue.startsWith("[") ? iValue.substring(1, iValue.length() - 1) : iValue;

		Collection<Object> coll = iType == OType.EMBEDDEDLIST ? new ArrayList<Object>() : new HashSet<Object>();

		if (value.length() == 0)
			return coll;

		String[] items = OStringSerializerHelper.split(value, OStringSerializerHelper.RECORD_SEPARATOR_AS_CHAR);

		for (String item : items) {
			if (iLinkedClass != null) {
				// EMBEDDED OBJECT
				if (item.length() > 2) {
					item = item.substring(1, item.length() - 1);
					coll.add(fromString(iDatabase, item, new ODocument(iDatabase, iLinkedClass.getName())));
				}

			} else if (item.length() > 0 && item.charAt(0) == OStringSerializerHelper.EMBEDDED) {
				// EMBEDDED OBJECT
				coll.add(OStringSerializerHelper.fieldTypeFromStream(iLinkedType, item.substring(1, item.length() - 1)));

			} else {
				// EMBEDDED LITERAL
				if (iLinkedType == null)
					throw new IllegalArgumentException(
							"Linked type can't be null. Probably the serialized type has not stored the type along with data");
				coll.add(OStringSerializerHelper.fieldTypeFromStream(iLinkedType, item));
			}
		}

		return coll;
	}

	public void embeddedCollectionToStream(final ODatabaseComplex<?> iDatabase, final OUserObject2RecordHandler iObjHandler,
			final OClass iLinkedClass, final OType iLinkedType, final Object iValue,
			final Map<ORecordInternal<?>, ORecordId> iMarshalledRecords, StringBuilder buffer) {
		buffer.append(OStringSerializerHelper.COLLECTION_BEGIN);

		int size = iValue instanceof Collection<?> ? ((Collection<Object>) iValue).size() : Array.getLength(iValue);
		Iterator<Object> iterator = iValue instanceof Collection<?> ? ((Collection<Object>) iValue).iterator() : null;
		Object o;

		for (int i = 0; i < size; ++i) {
			if (iValue instanceof Collection<?>)
				o = iterator.next();
			else
				o = Array.get(iValue, i);

			if (i > 0)
				buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

			if (o instanceof ORecord<?>)
				buffer.append(OStringSerializerHelper.EMBEDDED);

			if (iLinkedClass != null) {
				// EMBEDDED OBJECTS
				ODocument record;

				if (o != null) {
					if (o instanceof ODocument)
						record = (ODocument) o;
					else
						record = OObjectSerializerHelper.toStream(o,
								new ODocument((ODatabaseRecord<?>) iDatabase, o.getClass().getSimpleName()),
								iDatabase instanceof ODatabaseObjectTx ? ((ODatabaseObjectTx) iDatabase).getEntityManager()
										: OEntityManagerInternal.INSTANCE, iLinkedClass, iObjHandler != null ? iObjHandler
										: new OUserObject2RecordHandler() {
											public Object getUserObjectByRecord(ORecordInternal<?> iRecord, final String iFetchPlan) {
												return iRecord;
											}

											public ORecordInternal<?> getRecordByUserObject(Object iPojo, boolean iIsMandatory) {
												return new ODocument(iLinkedClass);
											}

											public boolean existsUserObjectByRecord(ORecordInternal<?> iRecord) {
												return false;
											}
										});

					buffer.append(toString(record, null, iObjHandler, iMarshalledRecords));
				}
			} else {
				// EMBEDDED LITERALS
				buffer.append(OStringSerializerHelper.fieldTypeToString(iLinkedType, o));
			}

			if (o instanceof ORecord<?>)
				buffer.append(OStringSerializerHelper.EMBEDDED);
		}

		buffer.append(OStringSerializerHelper.COLLECTION_END);
	}

	/**
	 * Parse a string returning the closer type. To avoid limit exceptions, for integer numbers are returned always LONG and for
	 * decimal ones always DOUBLE.
	 * 
	 * @param iUnusualSymbols
	 *          Localized decimal number separators
	 * @param iValue
	 *          Value to parse
	 * @return OType.LONG for integers, OType.DOUBLE for decimals and OType.STRING for other
	 */
	public static OType getNumber(final String iValue) {
		boolean integer = true;
		char c;

		for (int index = 0; index < iValue.length(); ++index) {
			c = iValue.charAt(index);
			if (c < '0' || c > '9')
				if ((index == 0 && (c == '+' || c == '-')))
					continue;
				else if (c == DECIMAL_SEPARATOR)
					integer = false;
				else {
					return OType.STRING;
				}
		}

		return integer ? OType.LONG : OType.DOUBLE;
	}

	/**
	 * Serialize the link.
	 * 
	 * @param buffer
	 * @param iParentRecord
	 * @param iLinked
	 *          Can be an instance of ORID or a Record<?>
	 */
	private void linkToStream(final StringBuilder buffer, final ORecordSchemaAware<?> iParentRecord, Object iLinked) {
		if (iLinked == null)
			// NULL REFERENCE
			return;

		ORID rid;

		buffer.append(OStringSerializerHelper.LINK);

		if (iLinked instanceof ORID) {
			// JUST THE REFERENCE
			rid = (ORID) iLinked;
		} else {
			if (!(iLinked instanceof ORecordInternal<?>)) {
				// NOT RECORD: TRY TO EXTRACT THE DOCUMENT IF ANY
				final String boundDocumentField = OObjectSerializerHelper.getDocumentBoundField(iLinked.getClass());
				if (boundDocumentField != null)
					iLinked = OObjectSerializerHelper.getFieldValue(iLinked, boundDocumentField);
			}

			if (!(iLinked instanceof ORecordInternal<?>))
				throw new IllegalArgumentException("Invalid object received. Expected a record but received: " + iLinked);

			// RECORD
			ORecordInternal<?> iLinkedRecord = (ORecordInternal<?>) iLinked;
			rid = iLinkedRecord.getIdentity();
			if (!rid.isValid() || iLinkedRecord.isDirty()) {
				// OVERWRITE THE DATABASE TO THE SAME OF PARENT ONE
				iLinkedRecord.setDatabase(iParentRecord.getDatabase());

				// STORE THE TRAVERSED OBJECT TO KNOW THE RECORD ID. CALL THIS VERSION TO AVOID CLEAR OF STACK IN THREAD-LOCAL
				((ODatabaseRecord<ORecordInternal<?>>) iLinkedRecord.getDatabase()).save((ORecordInternal<?>) iLinkedRecord);
			}

			if (iLinkedRecord instanceof ORecordSchemaAware<?>) {
				final ORecordSchemaAware<?> schemaAwareRecord = (ORecordSchemaAware<?>) iLinkedRecord;

				if (schemaAwareRecord.getClassName() != null) {
					buffer.append(schemaAwareRecord.getClassName());
					buffer.append(OStringSerializerHelper.CLASS_SEPARATOR);
				}
			}
		}

		if (rid.isValid())
			buffer.append(rid.toString());
	}
}
