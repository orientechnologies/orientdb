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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.orient.core.annotation.OAfterSerialization;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.document.OLazyRecordList;
import com.orientechnologies.orient.core.db.document.OLazyRecordMap;
import com.orientechnologies.orient.core.db.document.OLazyRecordSet;
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

			Collection<Object> coll = iType == OType.LINKLIST ? new OLazyRecordList(iDatabase, ODocument.RECORD_TYPE)
					: new OLazyRecordSet(iDatabase, ODocument.RECORD_TYPE);

			if (value.length() == 0)
				return coll;

			final List<String> items = OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR);

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

				if (item.length() == 0)
					continue;

				coll.add(new ORecordId(item));
				// coll.add(new ODocument(iDatabase, iLinkedClass != null ? iLinkedClass.getName() : null, new ORecordId(item)));
			}

			return coll;
		}

		case LINKMAP: {
			if (iValue.length() == 0)
				return null;

			// REMOVE BEGIN & END MAP CHARACTERS
			String value = iValue.substring(1, iValue.length() - 1);

			@SuppressWarnings("rawtypes")
			final Map map = new OLazyRecordMap(iDatabase, ODocument.RECORD_TYPE);

			if (value.length() == 0)
				return map;

			final List<String> items = OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR);

			// EMBEDDED LITERALS
			List<String> entry;
			String mapValue;

			for (String item : items) {
				if (item != null && item.length() > 0) {
					entry = OStringSerializerHelper.smartSplit(item, OStringSerializerHelper.ENTRY_SEPARATOR);
					if (entry.size() > 0) {
						mapValue = getLinkedRecord(iDatabase, value, entry.get(1));
						map.put((String) OStringSerializerHelper.fieldTypeFromStream(OType.STRING, entry.get(0)), new ORecordId(mapValue));
					}

				}
			}
			return map;
		}

		case EMBEDDEDMAP:
			return embeddedMapFromStream(iDatabase, iLinkedType, iValue);

		case LINK:
			if (iValue.length() > 1) {
				int pos = iValue.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
				if (pos > -1)
					iLinkedClass = iDatabase.getMetadata().getSchema().getClass(iValue.substring(1, pos));
				else
					pos = 0;

				return new ORecordId(iValue.substring(pos + 1));
			} else
				return null;

		default:
			return OStringSerializerHelper.fieldTypeFromStream(iType, iValue);
		}
	}

	public Map<String, Object> embeddedMapFromStream(final ODatabaseRecord<?> iDatabase, OType iLinkedType, final String iValue) {
		if (iValue.length() == 0)
			return null;

		// REMOVE BEGIN & END MAP CHARACTERS
		String value = iValue.substring(1, iValue.length() - 1);

		@SuppressWarnings("rawtypes")
		final Map map = new OLazyRecordMap(iDatabase, ODocument.RECORD_TYPE);

		if (value.length() == 0)
			return map;

		final List<String> items = OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR);

		// EMBEDDED LITERALS
		List<String> entry;
		String mapValue;
		for (String item : items) {
			if (item != null && item.length() > 0) {
				entry = OStringSerializerHelper.smartSplit(item, OStringSerializerHelper.ENTRY_SEPARATOR);
				if (entry.size() > 0) {
					mapValue = entry.get(1);

					if (iLinkedType == null) {
						if (mapValue.length() > 0) {
							if (mapValue.charAt(0) == OStringSerializerHelper.EMBEDDED) {
								iLinkedType = OType.EMBEDDED;
								mapValue = mapValue.substring(1, mapValue.length() - 1);
							} else if (Character.isDigit(mapValue.charAt(0)) || mapValue.charAt(0) == '+' || mapValue.charAt(0) == '-') {
								iLinkedType = getNumber(mapValue);
							} else if (mapValue.charAt(0) == '\'' || mapValue.charAt(0) == '"')
								iLinkedType = OType.STRING;
						} else
							iLinkedType = OType.EMBEDDED;
					}

					map.put((String) OStringSerializerHelper.fieldTypeFromStream(OType.STRING, entry.get(0)),
							OStringSerializerHelper.fieldTypeFromStream(iLinkedType, mapValue));
				}

			}
		}
		return map;
	}

	public String fieldToStream(final ODocument iRecord, final ODatabaseComplex<?> iDatabase,
			final OUserObject2RecordHandler iObjHandler, final OType iType, final OClass iLinkedClass, final OType iLinkedType,
			final String iName, final Object iValue, final Set<Integer> iMarshalledRecords) {
		StringBuilder buffer = new StringBuilder();

		switch (iType) {

		case LINK: {
			final ORID rid = linkToStream(buffer, iRecord, iValue);
			if (rid != null)
				// OVERWRITE CONTENT
				iRecord.field(iName, rid);
			break;
		}

		case LINKLIST: {
			buffer.append(OStringSerializerHelper.COLLECTION_BEGIN);

			ORID rid;
			int items = 0;
			List<Object> coll = (List<Object>) iValue;
			// LINKED LIST
			for (int i = 0; i < coll.size(); ++i) {
				if (items++ > 0)
					buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

				rid = linkToStream(buffer, iRecord, coll.get(i));

				if (rid != null)
					coll.set(i, rid);
			}

			buffer.append(OStringSerializerHelper.COLLECTION_END);
			break;
		}

		case LINKSET: {
			buffer.append(OStringSerializerHelper.COLLECTION_BEGIN);

			ORID rid;
			int items = 0;
			Set<Object> coll = (Set<Object>) iValue;
			Map<Object, Object> objToReplace = null;

			// LINKED SET
			for (Object item : coll) {
				if (items++ > 0)
					buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

				rid = linkToStream(buffer, iRecord, item);

				if (rid != null) {
					// REMEMBER TO REPLACE THIS ITEM AFTER ALL
					if (objToReplace == null)
						objToReplace = new HashMap<Object, Object>();

					objToReplace.put(item, rid);
				}
			}

			if (objToReplace != null)
				// REPLACE ALL CHANGED ITEMS
				for (Map.Entry<Object, Object> entry : objToReplace.entrySet()) {
					coll.remove(entry.getKey());
					coll.add(entry.getValue());
				}

			buffer.append(OStringSerializerHelper.COLLECTION_END);
			break;
		}

		case LINKMAP: {
			buffer.append(OStringSerializerHelper.MAP_BEGIN);

			ORID rid;
			int items = 0;
			Map<String, Object> map = (Map<String, Object>) iValue;
			Map<String, Object> objToReplace = null;

			// LINKED MAP
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				if (items++ > 0)
					buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

				buffer.append(OStringSerializerHelper.fieldTypeToString(OType.STRING, entry.getKey()));
				buffer.append(OStringSerializerHelper.ENTRY_SEPARATOR);
				rid = linkToStream(buffer, iRecord, entry.getValue());

				if (rid != null) {
					// REMEMBER TO REPLACE THIS ITEM AFTER ALL
					if (objToReplace == null)
						objToReplace = new HashMap<String, Object>();

					objToReplace.put(entry.getKey(), rid);
				}
			}

			if (objToReplace != null)
				// REPLACE ALL CHANGED ITEMS
				for (Map.Entry<String, Object> entry : objToReplace.entrySet()) {
					map.put(entry.getKey(), entry.getValue());
				}

			buffer.append(OStringSerializerHelper.MAP_END);
			break;
		}

		case EMBEDDED:
			if (iValue instanceof ODocument)
				buffer.append(toString((ODocument) iValue, null, iObjHandler, iMarshalledRecords));
			else if (iValue != null)
				buffer.append(iValue.toString());
			break;

		case EMBEDDEDLIST:
		case EMBEDDEDSET: {
			embeddedCollectionToStream(iDatabase, iObjHandler, iLinkedClass, iLinkedType, iValue, iMarshalledRecords, buffer);
			break;
		}

		case EMBEDDEDMAP: {
			buffer.append(OStringSerializerHelper.MAP_BEGIN);

			if (iValue != null) {
				int items = 0;
				// EMBEDDED OBJECTS
				ODocument record;
				for (Entry<String, Object> o : ((Map<String, Object>) iValue).entrySet()) {
					if (items > 0)
						buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

					if (o != null) {
						buffer.append(OStringSerializerHelper.fieldTypeToString(OType.STRING, o.getKey()));
						buffer.append(OStringSerializerHelper.ENTRY_SEPARATOR);

						if (o.getValue() instanceof ORecord<?>) {
							if (o.getValue() instanceof ODocument)
								record = (ODocument) o.getValue();
							else
								record = OObjectSerializerHelper.toStream(o.getValue(), new ODocument((ODatabaseRecord<?>) iDatabase, o.getValue()
										.getClass().getSimpleName()),
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

							buffer.append(OStringSerializerHelper.EMBEDDED);
							buffer.append(OStringSerializerHelper.fieldTypeToString(iLinkedType, record));
							// buffer.append(toString(record, null, iObjHandler, iMarshalledRecords));
							buffer.append(OStringSerializerHelper.EMBEDDED);
						} else
							// EMBEDDED LITERALS
							buffer.append(OStringSerializerHelper.fieldTypeToString(iLinkedType, o.getValue()));
					}

					items++;
				}
			}

			buffer.append(OStringSerializerHelper.MAP_END);
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
		final String value = iValue.startsWith("[") ? iValue.substring(1, iValue.length() - 1) : iValue;

		final Collection<Object> coll = iType == OType.EMBEDDEDLIST ? new ArrayList<Object>() : new HashSet<Object>();

		if (value.length() == 0)
			return coll;

		final List<String> items = OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR);

		for (String item : items) {
			if (iLinkedClass != null) {
				// EMBEDDED RECORD
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
			final OClass iLinkedClass, final OType iLinkedType, final Object iValue, final Set<Integer> iMarshalledRecords,
			StringBuilder buffer) {
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

			if (o == null)
				continue;

			ODocument document = null;
			final OClass linkedClass;
			if (!(o instanceof ORecord<?>)) {
				final String fieldBound = OObjectSerializerHelper.getDocumentBoundField(o.getClass());
				if (fieldBound != null) {
					OObjectSerializerHelper.invokeCallback(o, null, OBeforeSerialization.class);
					document = (ODocument) OObjectSerializerHelper.getFieldValue(o, fieldBound);
					OObjectSerializerHelper.invokeCallback(o, document, OAfterSerialization.class);
				}
				linkedClass = iLinkedClass;
			} else {
				document = (ODocument) o;
				linkedClass = document.getSchemaClass();
			}

			if (document != null)
				buffer.append(OStringSerializerHelper.EMBEDDED);

			if (linkedClass != null || document != null) {
				if (document == null)
					// EMBEDDED OBJECTS
					document = OObjectSerializerHelper.toStream(o,
							new ODocument((ODatabaseRecord<?>) iDatabase, o.getClass().getSimpleName()),
							iDatabase instanceof ODatabaseObjectTx ? ((ODatabaseObjectTx) iDatabase).getEntityManager()
									: OEntityManagerInternal.INSTANCE, iLinkedClass, iObjHandler != null ? iObjHandler
									: new OUserObject2RecordHandler() {
										public Object getUserObjectByRecord(ORecordInternal<?> iRecord, final String iFetchPlan) {
											return iRecord;
										}

										public ORecordInternal<?> getRecordByUserObject(Object iPojo, boolean iIsMandatory) {
											return new ODocument(linkedClass);
										}

										public boolean existsUserObjectByRecord(ORecordInternal<?> iRecord) {
											return false;
										}
									});

				buffer.append(toString(document, null, iObjHandler, iMarshalledRecords));
			} else {
				// EMBEDDED LITERALS
				buffer.append(OStringSerializerHelper.fieldTypeToString(iLinkedType, o));
			}

			if (document != null)
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
	 * @param iFieldName
	 *          TODO
	 * @param iLinked
	 *          Can be an instance of ORID or a Record<?>
	 * @return
	 */
	private ORID linkToStream(final StringBuilder buffer, final ORecordSchemaAware<?> iParentRecord, Object iLinked) {
		if (iLinked == null)
			// NULL REFERENCE
			return null;

		ORID resultRid = null;
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
				throw new IllegalArgumentException("Invalid object received. Expected a record but received type="
						+ iLinked.getClass().getName() + " and value=" + iLinked);

			// RECORD
			ORecordInternal<?> iLinkedRecord = (ORecordInternal<?>) iLinked;
			rid = iLinkedRecord.getIdentity();
			if (rid.isNew() || iLinkedRecord.isDirty()) {
				// OVERWRITE THE DATABASE TO THE SAME OF THE PARENT ONE
				iLinkedRecord.setDatabase(iParentRecord.getDatabase());

				// STORE THE TRAVERSED OBJECT TO KNOW THE RECORD ID. CALL THIS VERSION TO AVOID CLEAR OF STACK IN THREAD-LOCAL
				((ODatabaseRecord<ORecordInternal<?>>) iLinkedRecord.getDatabase()).save((ORecordInternal<?>) iLinkedRecord);
			}

			if (iParentRecord.getDatabase() instanceof ODatabaseRecord<?>) {
				final ODatabaseRecord<?> db = (ODatabaseRecord<?>) iParentRecord.getDatabase();
				if (!db.isRetainRecords())
					// REPLACE CURRENT RECORD WITH ITS ID: THIS SAVES A LOT OF MEMORY
					resultRid = iLinkedRecord.getIdentity();
			}
		}

		if (rid.isValid())
			buffer.append(rid.toString());

		return resultRid;
	}

	private String getLinkedRecord(final ODatabaseRecord<?> iDatabase, String value, String item) {
		// OClass iLinkedClass;
		// GET THE CLASS NAME IF ANY
		int classSeparatorPos = value.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
		if (classSeparatorPos > -1) {
			String className = value.substring(1, classSeparatorPos);
			if (className != null) {
				// iLinkedClass = iDatabase.getMetadata().getSchema().getClass(className);
				item = item.substring(classSeparatorPos + 1);
			}
		} else
			item = item.substring(1);
		return item;
	}
}
