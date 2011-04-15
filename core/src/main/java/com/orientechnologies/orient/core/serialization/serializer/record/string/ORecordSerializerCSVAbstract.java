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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.annotation.OAfterSerialization;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.db.object.OLazyObjectList;
import com.orientechnologies.orient.core.db.object.OLazyObjectMap;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
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
	public static final char	FIELD_VALUE_SEPARATOR	= ':';

	protected abstract ORecordSchemaAware<?> newObject(ODatabaseRecord iDatabase, String iClassName);

	public Object fieldFromStream(final ORecordInternal<?> iSourceRecord, final OType iType, OClass iLinkedClass, OType iLinkedType,
			final String iName, final String iValue) {

		if (iValue == null)
			return null;

		final ODatabaseRecord database = iSourceRecord.getDatabase();

		switch (iType) {
		case EMBEDDEDLIST:
		case EMBEDDEDSET:
			return embeddedCollectionFromStream(database, (ODocument) iSourceRecord, iType, iLinkedClass, iLinkedType, iValue);

		case LINKLIST:
		case LINKSET: {
			if (iValue.length() == 0)
				return null;

			// REMOVE BEGIN & END COLLECTIONS CHARACTERS IF IT'S A COLLECTION
			String value = iValue.startsWith("[") ? iValue.substring(1, iValue.length() - 1) : iValue;

			Collection<?> coll = iType == OType.LINKLIST ? new ORecordLazyList(iSourceRecord) : new ORecordLazySet(iSourceRecord);

			if (value.length() == 0)
				return coll;

			final List<String> items = OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR);

			for (String item : items) {
				// GET THE CLASS NAME IF ANY
				if (item != null && item.length() > 0)
					item = item.substring(1);

				if (item.length() == 0)
					continue;

				((Collection<Object>) coll).add(new ORecordId(item));
			}

			return coll;
		}

		case LINKMAP: {
			if (iValue.length() == 0)
				return null;

			// REMOVE BEGIN & END MAP CHARACTERS
			String value = iValue.substring(1, iValue.length() - 1);

			@SuppressWarnings("rawtypes")
			final Map map = new ORecordLazyMap(iSourceRecord, ODocument.RECORD_TYPE);

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
						if (mapValue != null && mapValue.length() > 0)
							mapValue.substring(1);
						map.put(fieldTypeFromStream((ODocument) iSourceRecord, OType.STRING, entry.get(0)), new ORecordId(mapValue));
					}

				}
			}
			return map;
		}

		case EMBEDDEDMAP:
			return embeddedMapFromStream((ODocument) iSourceRecord, iLinkedType, iValue);

		case LINK:
			if (iValue.length() > 1) {
				int pos = iValue.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
				if (pos > -1)
					iLinkedClass = database.getMetadata().getSchema().getClass(iValue.substring(1, pos));
				else
					pos = 0;

				return new ORecordId(iValue.substring(pos + 1));
			} else
				return null;

		case EMBEDDED:
			if (iValue.length() > 2) {
				// REMOVE BEGIN & END EMBEDDED CHARACTERS
				final String value = iValue.substring(1, iValue.length() - 1);
				return fieldTypeFromStream((ODocument) iSourceRecord, iType, value);
			} else
				return null;

		default:
			return fieldTypeFromStream((ODocument) iSourceRecord, iType, iValue);
		}
	}

	public Map<String, Object> embeddedMapFromStream(final ODocument iSourceDocument, OType iLinkedType, final String iValue) {
		if (iValue.length() == 0)
			return null;

		// REMOVE BEGIN & END MAP CHARACTERS
		String value = iValue.substring(1, iValue.length() - 1);

		@SuppressWarnings("rawtypes")
		final Map map;
		if (iLinkedType == OType.LINK || iLinkedType == OType.EMBEDDED)
			map = new ORecordLazyMap(iSourceDocument, ODocument.RECORD_TYPE);
		else
			map = new OTrackedMap<Object>(iSourceDocument);

		if (value.length() == 0)
			return map;

		final List<String> items = OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR);

		// EMBEDDED LITERALS
		List<String> entry;
		String mapValue;
		Object mapValueObject;

		for (String item : items) {
			if (item != null && item.length() > 0) {
				entry = OStringSerializerHelper.smartSplit(item, OStringSerializerHelper.ENTRY_SEPARATOR);
				if (entry.size() > 0) {
					mapValue = entry.get(1);

					if (iLinkedType == null) {
						if (mapValue.length() > 0) {
							final char begin = mapValue.charAt(0);

							if (begin == OStringSerializerHelper.PARENTHESIS_BEGIN)
								iLinkedType = OType.EMBEDDED;
							else if (begin == OStringSerializerHelper.LINK)
								iLinkedType = OType.LINK;
							else if (Character.isDigit(begin) || begin == '+' || begin == '-')
								iLinkedType = getType(mapValue);
							else if (begin == '\'' || begin == '"')
								iLinkedType = OType.STRING;
							else if (begin == OStringSerializerHelper.MAP_BEGIN)
								iLinkedType = OType.EMBEDDEDMAP;
						} else
							iLinkedType = OType.EMBEDDED;
					}

					if (iLinkedType == OType.EMBEDDED)
						mapValue = mapValue.substring(1, mapValue.length() - 1);

					mapValueObject = fieldTypeFromStream(iSourceDocument, iLinkedType, mapValue);

					if (mapValueObject != null && mapValueObject instanceof ODocument)
						((ODocument) mapValueObject).addOwner(iSourceDocument);

					map.put(fieldTypeFromStream(iSourceDocument, OType.STRING, entry.get(0)), mapValueObject);
				}

			}
		}
		return map;
	}

	public void fieldToStream(final ODocument iRecord, final ODatabaseComplex<?> iDatabase, final StringBuilder iOutput,
			final OUserObject2RecordHandler iObjHandler, final OType iType, final OClass iLinkedClass, final OType iLinkedType,
			final String iName, final Object iValue, final Set<Integer> iMarshalledRecords, final boolean iSaveOnlyDirty) {
		if (iValue == null)
			return;

		final long timer = OProfiler.getInstance().startChrono();

		switch (iType) {

		case LINK: {
			final Object link = linkToStream(iOutput, iRecord, iValue);
			if (link != null)
				// OVERWRITE CONTENT
				iRecord.field(iName, link);
			OProfiler.getInstance().stopChrono("serializer.rec.str.link2string", timer);
			break;
		}

		case LINKLIST: {
			iOutput.append(OStringSerializerHelper.COLLECTION_BEGIN);

			if (iValue != null) {
				Object link;
				int items = 0;
				List<Object> coll = (List<Object>) iValue;
				if (coll instanceof OLazyObjectList<?>) {
					((OLazyObjectList<?>) coll).setConvertToRecord(false);
				}

				boolean autoConvert = false;
				if (coll instanceof ORecordLazyMultiValue) {
					autoConvert = ((ORecordLazyMultiValue) coll).isAutoConvertToRecord();
					((ORecordLazyMultiValue) coll).convertRecords2Links();
					((ORecordLazyMultiValue) coll).setAutoConvertToRecord(false);
				}

				try {
					// LINKED LIST
					for (int i = 0; i < coll.size(); ++i) {
						if (items++ > 0)
							iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

						link = linkToStream(iOutput, iRecord, coll.get(i));

						if (link != null)
							coll.set(i, link);
					}
				} finally {
					if (coll instanceof OLazyObjectList<?>) {
						((OLazyObjectList<?>) coll).setConvertToRecord(true);
					}
				}

				if (coll instanceof ORecordLazyMultiValue)
					((ORecordLazyMultiValue) coll).setAutoConvertToRecord(autoConvert);
			}

			iOutput.append(OStringSerializerHelper.COLLECTION_END);
			OProfiler.getInstance().stopChrono("serializer.rec.str.linkList2string", timer);
			break;
		}

		case LINKSET: {
			iOutput.append(OStringSerializerHelper.COLLECTION_BEGIN);

			int items = 0;

			ORecordLazySet coll;
			if (!(iValue instanceof ORecordLazySet)) {
				// FIRST TIME: CONVERT THE ENTIRE COLLECTION
				coll = new ORecordLazySet(iRecord);
				coll.addAll((Collection<? extends OIdentifiable>) iValue);
				((Collection<? extends OIdentifiable>) iValue).clear();

				iRecord.field(iName, coll);
			} else
				coll = (ORecordLazySet) iValue;

			// LINKED SET
			boolean containsNew = false;
			for (Iterator<OIdentifiable> it = coll.rawIterator(); it.hasNext();) {
				if (items++ > 0)
					iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

				final OIdentifiable item = it.next();

				linkToStream(iOutput, iRecord, item);
			}

			coll.savedAllNewItems();

			iOutput.append(OStringSerializerHelper.COLLECTION_END);
			OProfiler.getInstance().stopChrono("serializer.rec.str.linkSet2string", timer);
			break;
		}

		case LINKMAP: {
			iOutput.append(OStringSerializerHelper.MAP_BEGIN);

			Object link;
			int items = 0;
			Map<String, Object> map = (Map<String, Object>) iValue;
			boolean invalidMap = false;

			// LINKED MAP
			if (map instanceof OLazyObjectMap<?>) {
				((OLazyObjectMap<?>) map).setConvertToRecord(false);
			}
			try {
				for (Map.Entry<String, Object> entry : map.entrySet()) {
					if (items++ > 0)
						iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

					fieldTypeToString(iOutput, iDatabase, OType.STRING, entry.getKey());
					iOutput.append(OStringSerializerHelper.ENTRY_SEPARATOR);
					link = linkToStream(iOutput, iRecord, entry.getValue());

					if (link != null && !invalidMap)
						// IDENTITY IS CHANGED, RE-SET INTO THE COLLECTION TO RECOMPUTE THE HASH
						invalidMap = true;
				}
			} finally {
				if (map instanceof OLazyObjectMap<?>) {
					((OLazyObjectMap<?>) map).setConvertToRecord(true);
				}
			}

			if (invalidMap) {
				final ORecordLazyMap newMap = new ORecordLazyMap(iRecord, ODocument.RECORD_TYPE);

				// REPLACE ALL CHANGED ITEMS
				for (Map.Entry<String, Object> entry : map.entrySet()) {
					newMap.put(entry.getKey(), (OIdentifiable) entry.getValue());
				}
				map.clear();
				iRecord.field(iName, newMap);
			}

			iOutput.append(OStringSerializerHelper.MAP_END);
			OProfiler.getInstance().stopChrono("serializer.rec.str.linkMap2string", timer);
			break;
		}

		case EMBEDDED:
			if (iValue instanceof ODocument) {
				iOutput.append(OStringSerializerHelper.PARENTHESIS_BEGIN);
				toString((ODocument) iValue, iOutput, null, iObjHandler, iMarshalledRecords);
				iOutput.append(OStringSerializerHelper.PARENTHESIS_END);
			} else if (iValue != null)
				iOutput.append(iValue.toString());
			OProfiler.getInstance().stopChrono("serializer.rec.str.embed2string", timer);
			break;

		case EMBEDDEDLIST:
			embeddedCollectionToStream(iDatabase, iObjHandler, iOutput, iLinkedClass, iLinkedType, iValue, iMarshalledRecords,
					iSaveOnlyDirty);
			OProfiler.getInstance().stopChrono("serializer.rec.str.embedList2string", timer);
			break;

		case EMBEDDEDSET:
			embeddedCollectionToStream(iDatabase, iObjHandler, iOutput, iLinkedClass, iLinkedType, iValue, iMarshalledRecords,
					iSaveOnlyDirty);
			OProfiler.getInstance().stopChrono("serializer.rec.str.embedSet2string", timer);
			break;

		case EMBEDDEDMAP: {
			embeddedMapToStream(iDatabase, iObjHandler, iOutput, iLinkedClass, iLinkedType, iValue, iMarshalledRecords, iSaveOnlyDirty);
			OProfiler.getInstance().stopChrono("serializer.rec.str.embedMap2string", timer);
			break;
		}

		default:
			fieldTypeToString(iOutput, iDatabase, iType, iValue);
		}
	}

	public void embeddedMapToStream(final ODatabaseComplex<?> iDatabase, final OUserObject2RecordHandler iObjHandler,
			final StringBuilder iOutput, final OClass iLinkedClass, OType iLinkedType, final Object iValue,
			final Set<Integer> iMarshalledRecords, final boolean iSaveOnlyDirty) {
		iOutput.append(OStringSerializerHelper.MAP_BEGIN);

		if (iValue != null) {
			int items = 0;
			// EMBEDDED OBJECTS
			ODocument record;
			for (Entry<String, Object> o : ((Map<String, Object>) iValue).entrySet()) {
				if (items > 0)
					iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

				if (o != null) {
					fieldTypeToString(iOutput, iDatabase, OType.STRING, o.getKey());
					iOutput.append(OStringSerializerHelper.ENTRY_SEPARATOR);

					if (o.getValue() instanceof ORecord<?>) {
						if (o.getValue() instanceof ODocument)
							record = (ODocument) o.getValue();
						else
							record = OObjectSerializerHelper.toStream(o.getValue(), new ODocument((ODatabaseRecord) iDatabase, o.getValue()
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

												public boolean existsUserObjectByRID(ORID iRID) {
													return false;
												}

												public void registerPojo(Object iObject, ORecordInternal<?> iRecord) {
												}
											}, null, iSaveOnlyDirty);

						iOutput.append(OStringSerializerHelper.PARENTHESIS_BEGIN);
						toString(record, iOutput, null, iObjHandler, iMarshalledRecords);
						iOutput.append(OStringSerializerHelper.PARENTHESIS_END);
					} else if (o.getValue() instanceof Map<?, ?>) {
						// SUB MAP
						fieldTypeToString(iOutput, iDatabase, OType.EMBEDDEDMAP, o.getValue());
					} else {
						// EMBEDDED LITERALS
						if (iLinkedType == null)
							iLinkedType = OType.getTypeByClass(o.getValue().getClass());
						fieldTypeToString(iOutput, iDatabase, iLinkedType, o.getValue());
					}
				}

				items++;
			}
		}

		iOutput.append(OStringSerializerHelper.MAP_END);
	}

	public Object embeddedCollectionFromStream(final ODatabaseRecord iDatabase, final ODocument iDocument, final OType iType,
			OClass iLinkedClass, final OType iLinkedType, final String iValue) {
		if (iValue.length() == 0)
			return null;

		// REMOVE BEGIN & END COLLECTIONS CHARACTERS IF IT'S A COLLECTION
		final String value = iValue.startsWith("[") ? iValue.substring(1, iValue.length() - 1) : iValue;

		final Collection<?> coll;
		if (iLinkedType == OType.LINK) {
			if (iDocument != null)
				coll = iType == OType.EMBEDDEDLIST ? new ORecordLazyList(iDocument) : new ORecordLazySet(iDocument);
			else
				coll = iType == OType.EMBEDDEDLIST ? new ORecordLazyList(iDatabase) : new ORecordLazySet(iDatabase);
		} else
			coll = iType == OType.EMBEDDEDLIST ? new OTrackedList<Object>(iDocument) : new OTrackedSet<Object>(iDocument);

		if (value.length() == 0)
			return coll;

		final List<String> items = OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR);

		Object objectToAdd;
		for (String item : items) {
			objectToAdd = null;

			if (item.length() > 2 && item.charAt(0) == OStringSerializerHelper.PARENTHESIS_BEGIN) {
				// REMOVE EMBEDDED BEGIN/END CHARS
				item = item.substring(1, item.length() - 1);

				if (item.length() > 0) {
					// EMBEDDED RECORD, EXTRACT THE CLASS NAME IF DIFFERENT BY THE PASSED (SUB-CLASS OR IT WAS PASSED NULL)
					iLinkedClass = OStringSerializerHelper.getRecordClassName(iDatabase, item, iLinkedClass);

					if (iLinkedClass != null) {
						objectToAdd = fromString(iDocument.getDatabase(), item, new ODocument(iDatabase, iLinkedClass.getName()));
					} else
						// EMBEDDED OBJECT
						objectToAdd = fieldTypeFromStream(iDocument, iLinkedType, item);
				}
			} else {
				// EMBEDDED LITERAL
				if (iLinkedType == null)
					throw new IllegalArgumentException(
							"Linked type can't be null. Probably the serialized type has not stored the type along with data");
				objectToAdd = fieldTypeFromStream(iDocument, iLinkedType, item);
			}

			if (objectToAdd != null) {
				if (objectToAdd instanceof ODocument && coll instanceof ORecordElement)
					((ODocument) objectToAdd).addOwner((ORecordElement) coll);
				((Collection<Object>) coll).add(objectToAdd);
			}
		}

		return coll;
	}

	public StringBuilder embeddedCollectionToStream(final ODatabaseComplex<?> iDatabase, final OUserObject2RecordHandler iObjHandler,
			final StringBuilder iOutput, final OClass iLinkedClass, final OType iLinkedType, final Object iValue,
			final Set<Integer> iMarshalledRecords, final boolean iSaveOnlyDirty) {
		iOutput.append(OStringSerializerHelper.COLLECTION_BEGIN);

		final int size = iValue instanceof Collection<?> ? ((Collection<Object>) iValue).size() : Array.getLength(iValue);
		final Iterator<Object> iterator = iValue instanceof Collection<?> ? ((Collection<Object>) iValue).iterator() : null;
		Object o;

		for (int i = 0; i < size; ++i) {
			if (iValue instanceof Collection<?>)
				o = iterator.next();
			else
				o = Array.get(iValue, i);

			if (i > 0)
				iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

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

				if (document.getIdentity().isTemporary())
					document.save();

				linkedClass = document.getSchemaClass();
			}

			if (document != null && iLinkedType != OType.LINK)
				iOutput.append(OStringSerializerHelper.PARENTHESIS_BEGIN);

			if (iLinkedType != OType.LINK && (linkedClass != null || document != null)) {
				if (document == null)
					// EMBEDDED OBJECTS
					document = OObjectSerializerHelper.toStream(o, new ODocument((ODatabaseRecord) iDatabase, o.getClass().getSimpleName()),
							iDatabase instanceof ODatabaseObjectTx ? ((ODatabaseObjectTx) iDatabase).getEntityManager()
									: OEntityManagerInternal.INSTANCE, iLinkedClass, iObjHandler != null ? iObjHandler
									: new OUserObject2RecordHandler() {
										public Object getUserObjectByRecord(ORecordInternal<?> iRecord, final String iFetchPlan) {
											return iRecord;
										}

										public ORecordInternal<?> getRecordByUserObject(Object iPojo, boolean iIsMandatory) {
											return new ODocument(linkedClass);
										}

										public boolean existsUserObjectByRID(ORID iRID) {
											return false;
										}

										public void registerPojo(Object iObject, ORecordInternal<?> iRecord) {
										}
									}, null, iSaveOnlyDirty);

				toString(document, iOutput, null, iObjHandler, iMarshalledRecords);
			} else {
				// EMBEDDED LITERALS
				fieldTypeToString(iOutput, iDatabase, iLinkedType, o);
			}

			if (document != null && iLinkedType != OType.LINK)
				iOutput.append(OStringSerializerHelper.PARENTHESIS_END);
		}

		iOutput.append(OStringSerializerHelper.COLLECTION_END);
		return iOutput;
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
	private OIdentifiable linkToStream(final StringBuilder buffer, final ORecordSchemaAware<?> iParentRecord, Object iLinked) {
		if (iLinked == null)
			// NULL REFERENCE
			return null;

		OIdentifiable resultRid = null;
		ORID rid;

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
				if (iLinkedRecord.getDatabase() == null)
					// OVERWRITE THE DATABASE TO THE SAME OF THE PARENT ONE
					iLinkedRecord.setDatabase(iParentRecord.getDatabase());

				if (iLinkedRecord instanceof ODocument) {
					final OClass schemaClass = ((ODocument) iLinkedRecord).getSchemaClass();
					iLinkedRecord.getDatabase().save(iLinkedRecord,
							schemaClass != null ? iLinkedRecord.getDatabase().getClusterNameById(schemaClass.getDefaultClusterId()) : null);
				} else
					// STORE THE TRAVERSED OBJECT TO KNOW THE RECORD ID. CALL THIS VERSION TO AVOID CLEAR OF STACK IN THREAD-LOCAL
					iLinkedRecord.getDatabase().save(iLinkedRecord);

				iLinkedRecord.getDatabase().registerPojo(iLinkedRecord.getDatabase().getUserObjectByRecord(iLinkedRecord, null),
						iLinkedRecord);

				resultRid = iLinkedRecord;
			}

			if (iParentRecord.getDatabase() instanceof ODatabaseRecord) {
				final ODatabaseRecord db = iParentRecord.getDatabase();
				if (!db.isRetainRecords())
					// REPLACE CURRENT RECORD WITH ITS ID: THIS SAVES A LOT OF MEMORY
					resultRid = iLinkedRecord.getIdentity();
			}
		}

		if (rid.isValid()) {
			buffer.append(OStringSerializerHelper.LINK);
			rid.toString(buffer);
		}

		return resultRid;
	}
}
