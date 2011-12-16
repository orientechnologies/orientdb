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

import com.orientechnologies.common.collection.OLazyIterator;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.annotation.OAfterSerialization;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.db.object.OLazyObjectMap;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordElement.STATUS;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
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
import com.orientechnologies.orient.core.serialization.serializer.string.OStringSerializerAnyStreamable;
import com.orientechnologies.orient.core.tx.OTransactionRecordEntry;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

@SuppressWarnings({ "unchecked", "serial" })
public abstract class ORecordSerializerCSVAbstract extends ORecordSerializerStringAbstract {
	public static final char	FIELD_VALUE_SEPARATOR	= ':';

	protected abstract ORecordSchemaAware<?> newObject(final String iClassName);

	public Object fieldFromStream(final ORecordInternal<?> iSourceRecord, final OType iType, OClass iLinkedClass, OType iLinkedType,
			final String iName, final String iValue) {

		if (iValue == null)
			return null;

		switch (iType) {
		case EMBEDDEDLIST:
		case EMBEDDEDSET:
			return embeddedCollectionFromStream((ODocument) iSourceRecord, iType, iLinkedClass, iLinkedType, iValue);

		case LINKLIST:
		case LINKSET: {
			if (iValue.length() == 0)
				return null;

			// REMOVE BEGIN & END COLLECTIONS CHARACTERS IF IT'S A COLLECTION
			final String value = iValue.startsWith("[") ? iValue.substring(1, iValue.length() - 1) : iValue;

			return iType == OType.LINKLIST ? new ORecordLazyList(iSourceRecord).setStreamedContent(new StringBuilder(value))
					: new OMVRBTreeRIDSet(iSourceRecord).fromStream(new StringBuilder(iValue));
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
			for (String item : items) {
				if (item != null && !item.isEmpty()) {
					final List<String> entry = OStringSerializerHelper.smartSplit(item, OStringSerializerHelper.ENTRY_SEPARATOR);
					if (!entry.isEmpty()) {
						String mapValue = entry.get(1);
						if (mapValue != null && !mapValue.isEmpty())
							mapValue = mapValue.substring(1);
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
					iLinkedClass = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema().getClass(iValue.substring(1, pos));
				else
					pos = 0;

				return new ORecordId(iValue.substring(pos + 1));
			} else
				return null;

		case EMBEDDED:
			if (iValue.length() > 2) {
				// REMOVE BEGIN & END EMBEDDED CHARACTERS
				final String value = iValue.substring(1, iValue.length() - 1);

				// RECORD
				final Object result = OStringSerializerAnyStreamable.INSTANCE.fromStream(value);
				if (result instanceof ODocument)
					((ODocument) result).addOwner(iSourceRecord);
				return result;
			} else
				return null;

		default:
			return fieldTypeFromStream((ODocument) iSourceRecord, iType, iValue);
		}
	}

	public Map<String, Object> embeddedMapFromStream(final ODocument iSourceDocument, final OType iLinkedType, final String iValue) {
		if (iValue.length() == 0)
			return null;

		// REMOVE BEGIN & END MAP CHARACTERS
		String value = iValue.substring(1, iValue.length() - 1);

		@SuppressWarnings("rawtypes")
		Map map;
		if (iLinkedType == OType.LINK || iLinkedType == OType.EMBEDDED)
			map = new ORecordLazyMap(iSourceDocument, ODocument.RECORD_TYPE);
		else
			map = new OTrackedMap<Object>(iSourceDocument);

		if (value.length() == 0)
			return map;

		final List<String> items = OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR);

		// EMBEDDED LITERALS

		if (map instanceof ORecordElement)
			((ORecordElement) map).setInternalStatus(STATUS.UNMARSHALLING);

		for (String item : items) {
			if (item != null && !item.isEmpty()) {
				final List<String> entries = OStringSerializerHelper.smartSplit(item, OStringSerializerHelper.ENTRY_SEPARATOR);
				if (!entries.isEmpty()) {
					final Object mapValueObject;
					if (entries.size() > 1) {
						String mapValue = entries.get(1);

						final OType linkedType;

						if (iLinkedType == null)
							if (!mapValue.isEmpty()) {
								linkedType = getType(mapValue);
								if (isConvertToLinkedMap(map, linkedType)) {
									// CONVERT IT TO A LAZY MAP
									map = new ORecordLazyMap(iSourceDocument, ODocument.RECORD_TYPE);
									((ORecordElement) map).setInternalStatus(STATUS.UNMARSHALLING);
								}
							} else
								linkedType = OType.EMBEDDED;
						else
							linkedType = iLinkedType;

						if (linkedType == OType.EMBEDDED)
							mapValue = mapValue.substring(1, mapValue.length() - 1);

						mapValueObject = fieldTypeFromStream(iSourceDocument, linkedType, mapValue);

						if (mapValueObject != null && mapValueObject instanceof ODocument)
							((ODocument) mapValueObject).addOwner(iSourceDocument);
					} else
						mapValueObject = null;

					map.put(fieldTypeFromStream(iSourceDocument, OType.STRING, entries.get(0)), mapValueObject);
				}

			}
		}

		if (map instanceof ORecordElement)
			((ORecordElement) map).setInternalStatus(STATUS.LOADED);

		return map;
	}

	protected boolean isConvertToLinkedMap(Map<?, ?> map, final OType linkedType) {
		boolean convert = (linkedType == OType.LINK && !(map instanceof ORecordLazyMap));
		if (convert) {
			for (Object value : map.values())
				if (!(value instanceof OIdentifiable))
					return false;
		}
		return convert;
	}

	public void fieldToStream(final ODocument iRecord, final StringBuilder iOutput, OUserObject2RecordHandler iObjHandler,
			final OType iType, final OClass iLinkedClass, final OType iLinkedType, final String iName, final Object iValue,
			final Set<Integer> iMarshalledRecords, final boolean iSaveOnlyDirty) {
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

			if (iValue instanceof ORecordLazyList && ((ORecordLazyList) iValue).getStreamedContent() != null) {
				iOutput.append(((ORecordLazyList) iValue).getStreamedContent());
				OProfiler.getInstance().updateCounter("serializer.rec.str.linkList2string.cached", +1);
			} else {
				final ORecordLazyList coll;
				final Iterator<OIdentifiable> it;
				if (!(iValue instanceof ORecordLazyList)) {
					// FIRST TIME: CONVERT THE ENTIRE COLLECTION
					coll = new ORecordLazyList(iRecord);
					coll.addAll((Collection<? extends OIdentifiable>) iValue);
					((Collection<? extends OIdentifiable>) iValue).clear();

					iRecord.field(iName, coll);
					it = coll.rawIterator();
				} else {
					// LAZY LIST
					coll = (ORecordLazyList) iValue;
					if (coll.getStreamedContent() != null) {
						// APPEND STREAMED CONTENT
						iOutput.append(coll.getStreamedContent());
						OProfiler.getInstance().updateCounter("serializer.rec.str.linkList2string.cached", +1);
						it = coll.newItemsIterator();
					} else
						it = coll.rawIterator();
				}

				if (it != null && it.hasNext()) {
					final StringBuilder buffer = new StringBuilder();
					for (int items = 0; it.hasNext(); items++) {
						if (items > 0)
							buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

						final OIdentifiable item = it.next();

						final OIdentifiable newRid = linkToStream(buffer, iRecord, item);
						if (newRid != null)
							((OLazyIterator<OIdentifiable>) it).update(newRid);
					}

					coll.convertRecords2Links();

					iOutput.append(buffer);

					// UPDATE THE STREAM
					coll.setStreamedContent(buffer);
				}
			}

			iOutput.append(OStringSerializerHelper.COLLECTION_END);
			OProfiler.getInstance().stopChrono("serializer.rec.str.linkList2string", timer);
			break;
		}

		case LINKSET: {
			final OMVRBTreeRIDSet coll;

			if (!(iValue instanceof OMVRBTreeRIDSet)) {
				// FIRST TIME: CONVERT THE ENTIRE COLLECTION
				coll = new OMVRBTreeRIDSet(iRecord, (Collection<OIdentifiable>) iValue);
				((Collection<? extends OIdentifiable>) iValue).clear();

				iRecord.field(iName, coll);
			} else
				// LAZY SET
				coll = (OMVRBTreeRIDSet) iValue;

			linkSetToStream(iOutput, iRecord, coll);
			OProfiler.getInstance().stopChrono("serializer.rec.str.linkSet2string", timer);
			break;
		}

		case LINKMAP: {
			iOutput.append(OStringSerializerHelper.MAP_BEGIN);

			Map<Object, Object> map = (Map<Object, Object>) iValue;

			// LINKED MAP
			if (map instanceof OLazyObjectMap<?>)
				((OLazyObjectMap<?>) map).setConvertToRecord(false);

			boolean invalidMap = false;
			try {
				int items = 0;
				for (Map.Entry<Object, Object> entry : map.entrySet()) {
					if (items++ > 0)
						iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

					fieldTypeToString(iOutput, OType.STRING, entry.getKey());
					iOutput.append(OStringSerializerHelper.ENTRY_SEPARATOR);
					final Object link = linkToStream(iOutput, iRecord, entry.getValue());

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
				for (Map.Entry<Object, Object> entry : map.entrySet()) {
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
				iOutput.append(OStringSerializerHelper.EMBEDDED_BEGIN);
				toString((ODocument) iValue, iOutput, null, iObjHandler, iMarshalledRecords, false);
				iOutput.append(OStringSerializerHelper.EMBEDDED_END);
			} else if (iValue != null)
				iOutput.append(iValue.toString());
			OProfiler.getInstance().stopChrono("serializer.rec.str.embed2string", timer);
			break;

		case EMBEDDEDLIST:
			embeddedCollectionToStream(null, iObjHandler, iOutput, iLinkedClass, iLinkedType, iValue, iMarshalledRecords, iSaveOnlyDirty);
			OProfiler.getInstance().stopChrono("serializer.rec.str.embedList2string", timer);
			break;

		case EMBEDDEDSET:
			embeddedCollectionToStream(null, iObjHandler, iOutput, iLinkedClass, iLinkedType, iValue, iMarshalledRecords, iSaveOnlyDirty);
			OProfiler.getInstance().stopChrono("serializer.rec.str.embedSet2string", timer);
			break;

		case EMBEDDEDMAP: {
			embeddedMapToStream(null, iObjHandler, iOutput, iLinkedClass, iLinkedType, iValue, iMarshalledRecords, iSaveOnlyDirty);
			OProfiler.getInstance().stopChrono("serializer.rec.str.embedMap2string", timer);
			break;
		}

		default:
			fieldTypeToString(iOutput, iType, iValue);
		}
	}

	public static StringBuilder linkSetToStream(final StringBuilder iOutput, final ODocument iRecord, final OMVRBTreeRIDSet iSet) {
		iSet.toStream(iOutput);
		return iOutput;
	}

	public void embeddedMapToStream(ODatabaseComplex<?> iDatabase, final OUserObject2RecordHandler iObjHandler,
			final StringBuilder iOutput, final OClass iLinkedClass, OType iLinkedType, final Object iValue,
			final Set<Integer> iMarshalledRecords, final boolean iSaveOnlyDirty) {
		iOutput.append(OStringSerializerHelper.MAP_BEGIN);

		if (iValue != null) {
			int items = 0;
			// EMBEDDED OBJECTS
			for (Entry<String, Object> o : ((Map<String, Object>) iValue).entrySet()) {
				if (items > 0)
					iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

				if (o != null) {
					fieldTypeToString(iOutput, OType.STRING, o.getKey());
					iOutput.append(OStringSerializerHelper.ENTRY_SEPARATOR);

					if (o.getValue() instanceof ORecord<?>) {
						final ODocument record;
						if (o.getValue() instanceof ODocument)
							record = (ODocument) o.getValue();
						else {
							if (iDatabase == null && ODatabaseRecordThreadLocal.INSTANCE.isDefined())
								iDatabase = ODatabaseRecordThreadLocal.INSTANCE.get();

							record = OObjectSerializerHelper.toStream(o.getValue(), new ODocument(o.getValue().getClass().getSimpleName()),
									iDatabase instanceof ODatabaseObjectTx ? ((ODatabaseObjectTx) iDatabase).getEntityManager()
											: OEntityManagerInternal.INSTANCE, iLinkedClass, iObjHandler != null ? iObjHandler
											: new OUserObject2RecordHandler() {

												public Object getUserObjectByRecord(ORecordInternal<?> iRecord, final String iFetchPlan) {
													return iRecord;
												}

												public ORecordInternal<?> getRecordByUserObject(Object iPojo, boolean iCreateIfNotAvailable) {
													return new ODocument(iLinkedClass);
												}

												public boolean existsUserObjectByRID(ORID iRID) {
													return false;
												}

												public void registerUserObject(Object iObject, ORecordInternal<?> iRecord) {
												}
											}, null, iSaveOnlyDirty);
						}
						iOutput.append(OStringSerializerHelper.EMBEDDED_BEGIN);
						toString(record, iOutput, null, iObjHandler, iMarshalledRecords, false);
						iOutput.append(OStringSerializerHelper.EMBEDDED_END);
					} else if (o.getValue() instanceof Set<?>) {
						// SUB SET
						fieldTypeToString(iOutput, OType.EMBEDDEDSET, o.getValue());
					} else if (o.getValue() instanceof Collection<?>) {
						// SUB LIST
						fieldTypeToString(iOutput, OType.EMBEDDEDLIST, o.getValue());
					} else if (o.getValue() instanceof Map<?, ?>) {
						// SUB MAP
						fieldTypeToString(iOutput, OType.EMBEDDEDMAP, o.getValue());
					} else {
						// EMBEDDED LITERALS
						if (iLinkedType == null && o.getValue() != null) {
							fieldTypeToString(iOutput, OType.getTypeByClass(o.getValue().getClass()), o.getValue());
						} else {
							fieldTypeToString(iOutput, iLinkedType, o.getValue());
						}
					}
				}
				items++;
			}
		}

		iOutput.append(OStringSerializerHelper.MAP_END);
	}

	public Object embeddedCollectionFromStream(final ODocument iDocument, final OType iType, OClass iLinkedClass,
			final OType iLinkedType, final String iValue) {
		if (iValue.length() == 0)
			return null;

		// REMOVE BEGIN & END COLLECTIONS CHARACTERS IF IT'S A COLLECTION
		final String value = iValue.charAt(0) == '[' ? iValue.substring(1, iValue.length() - 1) : iValue;

		Collection<?> coll;
		if (iLinkedType == OType.LINK) {
			if (iDocument != null)
				coll = (Collection<?>) (iType == OType.EMBEDDEDLIST ? new ORecordLazyList(iDocument).setStreamedContent(new StringBuilder(
						value)) : new OMVRBTreeRIDSet(iDocument).fromStream(new StringBuilder(value)));
			else {
				if (iType == OType.EMBEDDEDLIST)
					coll = (Collection<?>) new ORecordLazyList().setStreamedContent(new StringBuilder(value));
				else {
					return new OMVRBTreeRIDSet().fromStream(new StringBuilder(value));
				}
			}
		} else
			coll = iType == OType.EMBEDDEDLIST ? new OTrackedList<Object>(iDocument) : new OTrackedSet<Object>(iDocument);

		if (value.length() == 0)
			return coll;

		OType linkedType;

		if (coll instanceof ORecordElement)
			((ORecordElement) coll).setInternalStatus(STATUS.UNMARSHALLING);

		final List<String> items = OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR);
		for (String item : items) {
			Object objectToAdd = null;
			linkedType = null;

			if (item.length() > 2 && item.charAt(0) == OStringSerializerHelper.EMBEDDED_BEGIN) {
				// REMOVE EMBEDDED BEGIN/END CHARS
				item = item.substring(1, item.length() - 1);

				if (!item.isEmpty()) {
					// EMBEDDED RECORD, EXTRACT THE CLASS NAME IF DIFFERENT BY THE PASSED (SUB-CLASS OR IT WAS PASSED NULL)
					iLinkedClass = OStringSerializerHelper.getRecordClassName(item, iLinkedClass);

					if (iLinkedClass != null)
						objectToAdd = fromString(item, new ODocument(iLinkedClass.getName()));
					else
						// EMBEDDED OBJECT
						objectToAdd = fieldTypeFromStream(iDocument, OType.EMBEDDED, item);
				}
			} else {
				if (linkedType == null) {
					final char begin = value.charAt(0);

					// AUTO-DETERMINE LINKED TYPE
					if (begin == OStringSerializerHelper.LINK)
						linkedType = OType.LINK;
					else
						linkedType = getType(item);

					if (linkedType == null)
						throw new IllegalArgumentException(
								"Linked type cannot be null. Probably the serialized type has not stored the type along with data");
				}

				if (iLinkedType == OType.CUSTOM)
					item = item.substring(1, item.length() - 1);

				objectToAdd = fieldTypeFromStream(iDocument, linkedType, item);
			}

			if (objectToAdd != null) {
				if (objectToAdd instanceof ODocument && coll instanceof ORecordElement)
					((ODocument) objectToAdd).addOwner((ORecordElement) coll);
				((Collection<Object>) coll).add(objectToAdd);
			}
		}

		if (coll instanceof ORecordElement)
			((ORecordElement) coll).setInternalStatus(STATUS.LOADED);

		return coll;
	}

	public StringBuilder embeddedCollectionToStream(ODatabaseComplex<?> iDatabase, final OUserObject2RecordHandler iObjHandler,
			final StringBuilder iOutput, final OClass iLinkedClass, final OType iLinkedType, final Object iValue,
			final Set<Integer> iMarshalledRecords, final boolean iSaveOnlyDirty) {
		iOutput.append(OStringSerializerHelper.COLLECTION_BEGIN);

		final Iterator<Object> iterator = iValue instanceof Collection<?> ? ((Collection<Object>) iValue).iterator() : null;
		final int size = iValue instanceof Collection<?> ? ((Collection<Object>) iValue).size() : Array.getLength(iValue);
		OType linkedType = iLinkedType;

		for (int i = 0; i < size; ++i) {
			final Object o;
			if (iValue instanceof Collection<?>)
				o = iterator.next();
			else
				o = Array.get(iValue, i);

			if (i > 0)
				iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

			if (o == null)
				continue;

			OIdentifiable id = null;
			ODocument doc = null;

			final OClass linkedClass;
			if (!(o instanceof OIdentifiable)) {
				final String fieldBound = OObjectSerializerHelper.getDocumentBoundField(o.getClass());
				if (fieldBound != null) {
					OObjectSerializerHelper.invokeCallback(o, null, OBeforeSerialization.class);
					doc = (ODocument) OObjectSerializerHelper.getFieldValue(o, fieldBound);
					OObjectSerializerHelper.invokeCallback(o, doc, OAfterSerialization.class);
					id = doc;
				} else if (iLinkedType == null)
					linkedType = OType.getTypeByClass(o.getClass());

				linkedClass = iLinkedClass;
			} else {
				id = (OIdentifiable) o;

				if (iLinkedType == null)
					// AUTO-DETERMINE LINKED TYPE
					if (id.getIdentity().isValid())
						linkedType = OType.LINK;
					else
						linkedType = OType.EMBEDDED;

				if (id instanceof ODocument) {
					doc = (ODocument) id;

					if (id.getIdentity().isTemporary())
						doc.save();

					linkedClass = doc.getSchemaClass();
				} else
					linkedClass = null;
			}

			if (id != null && linkedType != OType.LINK)
				iOutput.append(OStringSerializerHelper.EMBEDDED_BEGIN);

			if (linkedType != OType.LINK && (linkedClass != null || doc != null)) {
				if (id == null) {
					// EMBEDDED OBJECTS
					if (iDatabase == null && ODatabaseRecordThreadLocal.INSTANCE.isDefined())
						iDatabase = ODatabaseRecordThreadLocal.INSTANCE.get();

					id = OObjectSerializerHelper.toStream(o, new ODocument(o.getClass().getSimpleName()),
							iDatabase instanceof ODatabaseObjectTx ? ((ODatabaseObjectTx) iDatabase).getEntityManager()
									: OEntityManagerInternal.INSTANCE, iLinkedClass, iObjHandler != null ? iObjHandler
									: new OUserObject2RecordHandler() {
										public Object getUserObjectByRecord(ORecordInternal<?> iRecord, final String iFetchPlan) {
											return iRecord;
										}

										public ORecordInternal<?> getRecordByUserObject(Object iPojo, boolean iCreateIfNotAvailable) {
											return new ODocument(linkedClass);
										}

										public boolean existsUserObjectByRID(ORID iRID) {
											return false;
										}

										public void registerUserObject(Object iObject, ORecordInternal<?> iRecord) {
										}
									}, null, iSaveOnlyDirty);
				}
				toString(doc, iOutput, null, iObjHandler, iMarshalledRecords, false);
			} else {
				// EMBEDDED LITERALS
				if (iLinkedType == null) {
					if (o != null)
						linkedType = OType.getTypeByClass(o.getClass());
				} else if (iLinkedType == OType.CUSTOM)
					iOutput.append(OStringSerializerHelper.CUSTOM_TYPE);
				fieldTypeToString(iOutput, linkedType, o);
			}

			if (id != null && linkedType != OType.LINK)
				iOutput.append(OStringSerializerHelper.EMBEDDED_END);
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
	private static OIdentifiable linkToStream(final StringBuilder buffer, final ORecordSchemaAware<?> iParentRecord, Object iLinked) {
		if (iLinked == null)
			// NULL REFERENCE
			return null;

		OIdentifiable resultRid = null;
		ORID rid;

		final ODatabaseRecord database = ODatabaseRecordThreadLocal.INSTANCE.get();

		if (iLinked instanceof ORID) {
			// JUST THE REFERENCE
			rid = (ORID) iLinked;

			if (rid.isValid() && rid.isNew()) {
				// SAVE AT THE FLY AND STORE THE NEW RID
				final ORecord<?> record = rid.getRecord();

				if (database.getTransaction().isActive()) {
					final OTransactionRecordEntry recordEntry = database.getTransaction().getRecordEntry(rid);
					if (recordEntry != null)
						// GET THE CLUSTER SPECIFIED
						database.save((ORecordInternal<?>) record, recordEntry.clusterName);
					else
						// USE THE DEFAULT CLUSTER
						database.save((ORecordInternal<?>) record);

				} else
					database.save((ORecordInternal<?>) record);

				rid = record.getIdentity();
				resultRid = rid;
			}
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
				if (iLinkedRecord instanceof ODocument) {
					final OClass schemaClass = ((ODocument) iLinkedRecord).getSchemaClass();
					database.save(iLinkedRecord, schemaClass != null ? database.getClusterNameById(schemaClass.getDefaultClusterId()) : null);
				} else
					// STORE THE TRAVERSED OBJECT TO KNOW THE RECORD ID. CALL THIS VERSION TO AVOID CLEAR OF STACK IN THREAD-LOCAL
					database.save(iLinkedRecord);

				final ODatabaseComplex<?> dbOwner = database.getDatabaseOwner();

				dbOwner.registerUserObject(dbOwner.getUserObjectByRecord(iLinkedRecord, null), iLinkedRecord);

				resultRid = iLinkedRecord;
			}

			if (iParentRecord != null && database instanceof ODatabaseRecord) {
				final ODatabaseRecord db = database;
				if (!db.isRetainRecords())
					// REPLACE CURRENT RECORD WITH ITS ID: THIS SAVES A LOT OF MEMORY
					resultRid = iLinkedRecord.getIdentity();
			}
		}

		if (rid.isValid())
			rid.toString(buffer);

		return resultRid;
	}
}
