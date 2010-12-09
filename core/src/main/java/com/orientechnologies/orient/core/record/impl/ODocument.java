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
package com.orientechnologies.orient.core.record.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.ORecordTrackedList;
import com.orientechnologies.orient.core.db.record.ORecordTrackedMap;
import com.orientechnologies.orient.core.db.record.ORecordTrackedSet;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordVirtualAbstract;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;

/**
 * ORecord implementation schema aware. It's able to handle records with, without or with a partial schema. Fields can be added at
 * run-time. Instances can be reused across calls by using the reset() before to re-use.
 */
@SuppressWarnings("unchecked")
public class ODocument extends ORecordVirtualAbstract<Object> implements Iterable<Entry<String, Object>> {
	public static final byte	RECORD_TYPE	= 'd';

	protected ODocument				_owner			= null;

	public ODocument() {
		setup();
	}

	/**
	 * Creates a new instance by the raw stream usually read from the database. New instances are not persistent until {@link #save()}
	 * is called.
	 * 
	 * @param iSource
	 *          Raw stream
	 */
	public ODocument(final byte[] iSource) {
		super(iSource);
		setup();
	}

	/**
	 * Creates a new instance and binds to the specified database. New instances are not persistent until {@link #save()} is called.
	 * 
	 * @param iDatabase
	 *          Database instance
	 */
	public ODocument(final ODatabaseRecord<?> iDatabase) {
		super(iDatabase);
		setup();
	}

	/**
	 * Creates a new instance in memory linked by the Record Id to the persistent one. New instances are not persistent until
	 * {@link #save()} is called.
	 * 
	 * @param iDatabase
	 *          Database instance
	 * @param iRID
	 *          Record Id
	 */
	public ODocument(final ODatabaseRecord<?> iDatabase, final ORID iRID) {
		this(iDatabase);
		_recordId = (ORecordId) iRID;
		_status = STATUS.NOT_LOADED;
		_dirty = false;
	}

	/**
	 * Creates a new instance in memory of the specified class, linked by the Record Id to the persistent one. New instances are not
	 * persistent until {@link #save()} is called.
	 * 
	 * @param iDatabase
	 *          Database instance
	 * @param iClassName
	 *          Class name
	 * @param iRID
	 *          Record Id
	 */
	public ODocument(final ODatabaseRecord<?> iDatabase, final String iClassName, final ORID iRID) {
		this(iDatabase, iClassName);
		_recordId = (ORecordId) iRID;
		_dirty = false;
		_status = STATUS.NOT_LOADED;
	}

	/**
	 * Creates a new instance in memory of the specified class. New instances are not persistent until {@link #save()} is called.
	 * 
	 * @param iDatabase
	 *          Database instance
	 * @param iClassName
	 *          Class name
	 */
	public ODocument(final ODatabaseRecord<?> iDatabase, final String iClassName) {
		super(iDatabase, iClassName);
		setup();
	}

	/**
	 * Creates a new instance in memory of the specified schema class. New instances are not persistent until {@link #save()} is
	 * called. The database reference is taken by the OClass instance received.
	 * 
	 * @param iClass
	 *          OClass instance
	 */
	public ODocument(final OClass iClass) {
		super(iClass.getDocument().getDatabase());
		setup();
		_clazz = iClass;
	}

	/**
	 * Fill a document passing the field array in form of pairs of field name and value.
	 * 
	 * @param iFields
	 *          Array of field pairs
	 */
	public ODocument(final Object[] iFields) {
		if (iFields != null && iFields.length > 0)
			for (int i = 0; i < iFields.length; i += 2) {
				field(iFields[i].toString(), iFields[i + 1]);
			}
	}

	/**
	 * Fill a document passing the field names/values
	 * 
	 */
	public ODocument(final String iFieldName, final Object iFieldValue, final Object... iFields) {
		this(iFields);
		field(iFieldName, iFieldValue);
	}

	/**
	 * Copies the current instance to a new one.
	 */
	public ODocument copy() {
		ODocument cloned = new ODocument();
		cloned._source = _source;
		cloned._database = _database;
		cloned._recordId = _recordId.copy();
		cloned._version = _version;
		cloned._dirty = _dirty;
		cloned._pinned = _pinned;
		cloned._clazz = _clazz;
		cloned._status = _status;
		cloned._recordFormat = _recordFormat;

		if (_fieldValues != null) {
			cloned._fieldValues = new LinkedHashMap<String, Object>();
			Object fieldValue;
			for (Entry<String, Object> entry : _fieldValues.entrySet()) {
				fieldValue = entry.getValue();

				if (fieldValue != null)
					// LISTS
					if (fieldValue instanceof ORecordLazyList) {
						final ORecordLazyList newList = new ORecordLazyList(cloned, ((ORecordLazyList) fieldValue).getRecordType());
						newList.addAll((ORecordLazyList) fieldValue);
						cloned._fieldValues.put(entry.getKey(), newList);

					} else if (fieldValue instanceof ORecordTrackedList) {
						final ORecordTrackedList newList = new ORecordTrackedList(cloned);
						newList.addAll((ORecordTrackedList) fieldValue);
						cloned._fieldValues.put(entry.getKey(), newList);

					} else if (fieldValue instanceof List<?>) {
						cloned._fieldValues.put(entry.getKey(), new ArrayList<Object>((List<Object>) fieldValue));

						// SETS
					} else if (fieldValue instanceof ORecordLazySet) {
						final ORecordLazySet newList = new ORecordLazySet(cloned, ((ORecordLazySet) fieldValue).getRecordType());
						newList.addAll((ORecordLazySet) fieldValue);
						cloned._fieldValues.put(entry.getKey(), newList);

					} else if (fieldValue instanceof ORecordTrackedSet) {
						final ORecordTrackedSet newList = new ORecordTrackedSet(cloned);
						newList.addAll((ORecordTrackedSet) fieldValue);
						cloned._fieldValues.put(entry.getKey(), newList);

					} else if (fieldValue instanceof Set<?>) {
						cloned._fieldValues.put(entry.getKey(), new HashSet<Object>((Set<Object>) fieldValue));

						// MAPS
					} else if (fieldValue instanceof ORecordLazyMap) {
						final ORecordLazyMap newMap = new ORecordLazyMap(cloned, ((ORecordLazyMap) fieldValue).getRecordType());
						newMap.putAll((ORecordLazyMap) fieldValue);
						cloned._fieldValues.put(entry.getKey(), newMap);

					} else if (fieldValue instanceof ORecordTrackedMap) {
						final ORecordTrackedMap newMap = new ORecordTrackedMap(cloned);
						newMap.putAll((ORecordTrackedMap) fieldValue);
						cloned._fieldValues.put(entry.getKey(), newMap);

					} else if (fieldValue instanceof Map<?, ?>) {
						cloned._fieldValues.put(entry.getKey(), new LinkedHashMap<String, Object>((Map<String, Object>) fieldValue));
					} else
						cloned._fieldValues.put(entry.getKey(), fieldValue);
			}
		}

		if (_fieldTypes != null)
			cloned._fieldTypes = new LinkedHashMap<String, OType>(_fieldTypes);

		return cloned;
	}

	/**
	 * Loads the record using a fetch plan.
	 */
	public ODocument load(final String iFetchPlan) {
		if (_database == null)
			throw new ODatabaseException("No database assigned to current record");

		Object result = null;
		try {
			result = _database.load(this, iFetchPlan);
		} catch (Exception e) {
			throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' was not found", e);
		}

		if (result == null)
			throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' was not found");

		return this;
	}

	/**
	 * Makes a deep comparison field by field to check if the passed ODocument instance is identical in the content to the current
	 * one. Instead equals() just checks if the RID are the same.
	 * 
	 * @param iOther
	 *          ODocument instance
	 * @return true if the two document are identical, otherwise false
	 * @see #equals(Object);
	 */
	public boolean hasSameContentOf(final ODocument iOther) {
		if (iOther == null)
			return false;

		if (!equals(iOther) && _recordId.isValid())
			return false;

		if (_status == STATUS.NOT_LOADED)
			load();
		if (iOther._status == STATUS.NOT_LOADED)
			iOther.load();

		checkForFields();

		iOther.checkForFields();

		if (_fieldValues.size() != iOther._fieldValues.size())
			return false;

		// CHECK FIELD-BY-FIELD
		Object myFieldValue;
		Object otherFieldValue;
		for (Entry<String, Object> f : _fieldValues.entrySet()) {
			myFieldValue = f.getValue();
			otherFieldValue = iOther._fieldValues.get(f.getKey());

			// CHECK FOR NULLS
			if (myFieldValue == null) {
				if (otherFieldValue != null)
					return false;
			} else if (otherFieldValue == null)
				return false;

			if (myFieldValue != null && otherFieldValue != null)
				if (myFieldValue instanceof List && otherFieldValue instanceof List) {
					// CHECK IF THE ORDER IS RESPECTED
					final List<?> myList = (List<?>) myFieldValue;
					final List<?> otherList = (List<?>) otherFieldValue;

					if (myList.size() != otherList.size())
						return false;

					for (int i = 0; i < myList.size(); ++i) {
						if (myList.get(i) instanceof ODocument) {
							if (!((ODocument) myList.get(i)).hasSameContentOf((ODocument) otherList.get(i)))
								return false;
						} else if (!myList.get(i).equals(otherList.get(i)))
							return false;
					}
				} else if (myFieldValue instanceof Map && otherFieldValue instanceof Map) {
					// CHECK IF THE ORDER IS RESPECTED
					final Map<?, ?> myMap = (Map<?, ?>) myFieldValue;
					final Map<?, ?> otherMap = (Map<?, ?>) otherFieldValue;

					if (myMap.size() != otherMap.size())
						return false;

					for (Entry<?, ?> myEntry : myMap.entrySet()) {
						if (!otherMap.containsKey(myEntry.getKey()))
							return false;

						if (myEntry.getValue() instanceof ODocument) {
							if (!((ODocument) myEntry.getValue()).hasSameContentOf((ODocument) otherMap.get(myEntry.getKey())))
								return false;
						} else if (!myEntry.getValue().equals(otherMap.get(myEntry.getKey())))
							return false;
					}
				} else {
					if (!myFieldValue.equals(otherFieldValue))
						return false;
				}
		}

		return true;
	}

	/**
	 * Dumps the instance as string.
	 */
	@Override
	public String toString() {
		checkForFields();

		StringBuilder buffer = new StringBuilder();

		if (_clazz != null)
			buffer.append(_clazz.getName());

		if (_recordId != null) {
			if (buffer.length() > 0)
				buffer.append("@");
			if (_recordId != null && _recordId.isValid())
				buffer.append(_recordId);
		}

		boolean first = true;
		ORecord<?> record;
		for (Entry<String, Object> f : _fieldValues.entrySet()) {
			buffer.append(first ? "{" : ",");
			buffer.append(f.getKey());
			buffer.append(":");
			if (f.getValue() instanceof Collection<?>) {
				buffer.append("[");
				buffer.append(((Collection<?>) f.getValue()).size());
				buffer.append("]");
			} else if (f.getValue() instanceof ORecord<?>) {
				record = (ORecord<?>) f.getValue();

				if (record.getIdentity() != null) {
					buffer.append("#");
					buffer.append(record.getIdentity());
				} else
					buffer.append(record.toString());
			} else
				buffer.append(f.getValue());

			if (first)
				first = false;
		}
		if (!first)
			buffer.append("}");

		return buffer.toString();
	}

	/**
	 * Returns the field number.
	 */
	public int size() {
		return _fieldValues == null ? 0 : _fieldValues.size();
	}

	/**
	 * Returns the set of field names.
	 */
	public Set<String> fieldNames() {
		checkForLoading();
		checkForFields();

		return new HashSet<String>(_fieldValues.keySet());
	}

	/**
	 * Returns the array of field values.
	 */
	public Object[] fieldValues() {
		checkForLoading();
		checkForFields();

		Object[] result = new Object[_fieldValues.values().size()];
		return _fieldValues.values().toArray(result);
	}

	public <RET> RET rawField(final String iPropertyName) {
		checkForLoading();
		checkForFields();

		int separatorPos = iPropertyName.indexOf('.');
		if (separatorPos > -1) {
			// GET THE LINKED OBJECT IF ANY
			String fieldName = iPropertyName.substring(0, separatorPos);
			Object linkedObject = _fieldValues.get(fieldName);

			if (linkedObject == null || !(linkedObject instanceof ODocument))
				// IGNORE IT BY RETURNING NULL
				return null;

			ODocument linkedRecord = (ODocument) linkedObject;
			if (linkedRecord.getInternalStatus() == STATUS.NOT_LOADED)
				// LAZY LOAD IT
				linkedRecord.load();

			// CALL MYSELF RECURSIVELY BY CROSSING ALL THE OBJECTS
			return (RET) linkedRecord.field(iPropertyName.substring(separatorPos + 1));
		}

		return (RET) _fieldValues.get(iPropertyName);
	}

	/**
	 * Reads the field value.
	 * 
	 * @param iPropertyName
	 *          field name
	 * @return field value if defined, otherwise null
	 */
	public <RET> RET field(final String iPropertyName) {
		RET value = this.<RET> rawField(iPropertyName);

		if (value instanceof ORID) {
			// CREATE THE DOCUMENT OBJECT IN LAZY WAY
			value = (RET) new ODocument(_database, (ORID) value);
			_fieldValues.put(iPropertyName, value);
		}

		// CHECK FOR CONVERSION
		final OType t = fieldType(iPropertyName);
		if (t != null) {
			if (t == OType.BINARY && value instanceof String) {
				byte[] buffer = OBase64Utils.decode((String) value);
				field(iPropertyName, buffer);
				value = (RET) buffer;
			}
		}

		return value;
	}

	/**
	 * Reads the field value forcing the return type. Use this method to force return of ORID instead of the entire document by
	 * passing ORID.class as iType.
	 * 
	 * @param iPropertyName
	 *          field name
	 * @param iType
	 *          Forced type.
	 * @return field value if defined, otherwise null
	 */
	public <RET> RET field(final String iPropertyName, final Class<?> iType) {
		RET value = this.<RET> rawField(iPropertyName);

		value = convertField(iPropertyName, iType, value);

		return value;
	}

	/**
	 * Reads the field value forcing the return type. Use this method to force return of binary data.
	 * 
	 * @param iPropertyName
	 *          field name
	 * @param iType
	 *          Forced type.
	 * @return field value if defined, otherwise null
	 */
	public <RET> RET field(final String iPropertyName, final OType iType) {
		setFieldType(iPropertyName, iType);
		return (RET) field(iPropertyName);
	}

	/**
	 * Writes the field value.
	 * 
	 * @param iPropertyName
	 *          field name
	 * @param iPropertyValue
	 *          field value
	 * @return The Record instance itself giving a "fluent interface". Useful to call multiple methods in chain.
	 */
	public ODocument field(final String iPropertyName, Object iPropertyValue) {
		return field(iPropertyName, iPropertyValue, null);
	}

	/**
	 * Fill a document passing the field names/values
	 * 
	 */
	public ODocument fields(final String iFieldName, final Object iFieldValue, final Object... iFields) {
		field(iFieldName, iFieldValue);
		if (iFields != null && iFields.length > 0)
			for (int i = 0; i < iFields.length; i += 2) {
				field(iFields[i].toString(), iFields[i + 1]);
			}
		return this;
	}

	/**
	 * Writes the field value forcing the type.
	 * 
	 * @param iPropertyName
	 *          field name
	 * @param iPropertyValue
	 *          field value
	 * @param iType
	 *          Forced type (not auto-determined)
	 * @return The Record instance itself giving a "fluent interface". Useful to call multiple methods in chain.
	 */
	public ODocument field(final String iPropertyName, Object iPropertyValue, OType iType) {
		checkForLoading();
		checkForFields();

		final boolean knownProperty = _fieldValues.containsKey(iPropertyName);
		final Object oldValue = _fieldValues.get(iPropertyName);

		if (knownProperty)
			// CHECK IF IS REALLY CHANGED
			if (iPropertyValue == null) {
				if (oldValue == null)
					// BOTH NULL: UNCHANGED
					return this;
			} else {
				try {
					if (iPropertyValue == oldValue)
						// BOTH NULL: UNCHANGED
						return this;
				} catch (Exception e) {
					OLogManager.instance().warn(this, "Error on checking the value of property %s against the record %s", e, iPropertyName,
							getIdentity());
				}
			}

		if (_clazz != null) {
			OProperty prop = _clazz.getProperty(iPropertyName);

			if (prop != null) {
				if (iPropertyValue instanceof Enum)
					// ENUM
					if (prop.getType().isAssignableFrom(1))
						iPropertyValue = ((Enum<?>) iPropertyValue).ordinal();
					else
						iPropertyValue = ((Enum<?>) iPropertyValue).name();

				if (!(iPropertyValue instanceof String) && !prop.getType().isAssignableFrom(iPropertyValue))
					throw new IllegalArgumentException("Property '" + iPropertyName + "' of type '" + prop.getType()
							+ "' can't accept value of type: " + iPropertyValue.getClass());
			}
		}

		if (knownProperty && _trackingChanges) {
			// SAVE THE OLD VALUE IN A SEPARATE MAP
			if (_fieldOriginalValues == null)
				_fieldOriginalValues = new HashMap<String, Object>();
			_fieldOriginalValues.put(iPropertyName, oldValue);
		}

		if (_status != STATUS.UNMARSHALLING)
			setDirty();

		if (oldValue != null) {
			// DETERMINE THE TYPE FROM THE PREVIOUS CONTENT
			if (oldValue instanceof ORecord<?> && iPropertyValue instanceof String)
				// CONVERT TO RECORD-ID
				iPropertyValue = new ORecordId((String) iPropertyValue);
			else if (oldValue instanceof Collection<?> && iPropertyValue instanceof String) {
				// CONVERT TO COLLECTION
				final List<ODocument> newValue = new ArrayList<ODocument>();
				iPropertyValue = newValue;

				final String stringValue = (String) iPropertyValue;

				if (stringValue != null && stringValue.length() > 0) {
					final String[] items = stringValue.split(",");
					for (String s : items) {
						newValue.add(new ODocument(_database, new ORecordId(s)));
					}
				}
			} else if (iPropertyValue instanceof Enum) {
				// ENUM
				if (oldValue instanceof Number)
					iPropertyValue = ((Enum<?>) iPropertyValue).ordinal();
				else
					iPropertyValue = iPropertyValue.toString();
			}
		} else {
			if (iPropertyValue instanceof Enum)
				// ENUM
				iPropertyValue = iPropertyValue.toString();
		}

		_fieldValues.put(iPropertyName, iPropertyValue);

		setFieldType(iPropertyName, iType);

		return this;
	}

	/**
	 * Removes a field.
	 */
	public Object removeField(final String iPropertyName) {
		checkForLoading();
		checkForFields();

		final boolean knownProperty = _fieldValues.containsKey(iPropertyName);
		final Object oldValue = _fieldValues.get(iPropertyName);

		if (knownProperty && _trackingChanges) {
			// SAVE THE OLD VALUE IN A SEPARATE MAP
			if (_fieldOriginalValues == null)
				_fieldOriginalValues = new HashMap<String, Object>();
			_fieldOriginalValues.put(iPropertyName, oldValue);
		}

		_fieldValues.remove(iPropertyName);

		setDirty();
		return oldValue;
	}

	/**
	 * Merge current document with the document passed as parameter. If the field already exists then the conflicts are managed based
	 * on the value of the parameter 'iConflictsOtherWins'.
	 * 
	 * @param iOther
	 *          Other ODocument instance to merge
	 * @param iConflictsOtherWins
	 *          if true, the other document wins in case of conflicts, otherwise the current document wins
	 * @param iMergeSingleItemsOfMultiValueFields
	 * @return
	 */
	public ODocument merge(final ODocument iOther, boolean iConflictsOtherWins, boolean iMergeSingleItemsOfMultiValueFields) {
		return merge(iOther._fieldValues, iConflictsOtherWins, iMergeSingleItemsOfMultiValueFields);
	}

	/**
	 * Merge current document with the document passed as parameter. If the field already exists then the conflicts are managed based
	 * on the value of the parameter 'iConflictsOtherWins'.
	 * 
	 * @param iOther
	 *          Other ODocument instance to merge
	 * @param iConflictsOtherWins
	 *          if true, the other document wins in case of conflicts, otherwise the current document wins
	 * @param iMergeSingleItemsOfMultiValueFields
	 * @return
	 */
	public ODocument merge(final Map<String, Object> iOther, boolean iConflictsOtherWins, boolean iMergeSingleItemsOfMultiValueFields) {
		checkForLoading();
		checkForFields();

		for (String f : iOther.keySet()) {
			if (!containsField(f) || iConflictsOtherWins) {
				if (iMergeSingleItemsOfMultiValueFields) {
					Object field = field(f);
					if (field instanceof Map<?, ?>) {
						final Map<String, Object> map = (Map<String, Object>) field;
						final Map<String, Object> otherMap = (Map<String, Object>) iOther.get(f);

						for (Entry<String, Object> entry : otherMap.entrySet()) {
							map.put(entry.getKey(), entry.getValue());
						}
						continue;
					} else if (field instanceof Collection<?>) {
						final Collection<Object> coll = (Collection<Object>) field;
						final Collection<Object> otherColl = (Collection<Object>) iOther.get(f);

						for (Object item : otherColl) {
							if (!coll.contains(item))
								coll.add(item);
						}
						continue;
					}

				}

				field(f, iOther.get(f));
			}
		}

		return this;
	}

	/**
	 * Returns the original value of a field before it has been changed.
	 * 
	 * @param iPropertyName
	 *          Property name to retrieve the original value
	 */
	public Set<String> getDirtyFields() {
		return _fieldOriginalValues != null ? Collections.unmodifiableSet(_fieldOriginalValues.keySet()) : null;
	}

	/**
	 * Returns the original value of a field before it has been changed.
	 * 
	 * @param iPropertyName
	 *          Property name to retrieve the original value
	 */
	public Object getOriginalValue(final String iPropertyName) {
		return _fieldOriginalValues != null ? _fieldOriginalValues.get(iPropertyName) : null;
	}

	/**
	 * Returns the iterator against the field entries as name and value.
	 */
	public Iterator<Entry<String, Object>> iterator() {
		if (_fieldValues == null)
			return OEmptyIterator.INSTANCE;

		return _fieldValues.entrySet().iterator();
	}

	/**
	 * Checks if a field exists.
	 * 
	 * @return True if exists, otherwise false.
	 */
	@Override
	public boolean containsField(final String iFieldName) {
		checkForLoading();
		checkForFields();
		return _fieldValues.containsKey(iFieldName);
	}

	/**
	 * Internal.
	 */
	public byte getRecordType() {
		return RECORD_TYPE;
	}

	public ODocument getOwner() {
		return _owner;
	}

	/**
	 * Internal.
	 */
	public ODocument setOwner(ODocument owner) {
		this._owner = owner;
		return this;
	}

	/**
	 * Propagates the dirty status to the owner, if any. This happens when the object is embedded in another one.
	 */
	@Override
	public ORecordAbstract<Object> setDirty() {
		if (_owner != null)
			// PROPAGATES TO THE OWNER
			_owner.setDirty();
		return super.setDirty();
	}

	/**
	 * Internal.
	 */
	@Override
	protected void setup() {
		super.setup();
		_recordFormat = ORecordSerializerFactory.instance().getFormat(ORecordSerializerSchemaAware2CSV.NAME);
	}

	private <RET> RET convertField(final String iPropertyName, final Class<?> iType, RET iValue) {
		if (iType == null)
			return iValue;

		if (iValue instanceof ORID && !ORID.class.equals(iType) && !ORecordId.class.equals(iType)) {
			// CREATE THE DOCUMENT OBJECT IN LAZY WAY
			iValue = (RET) new ODocument(_database, (ORID) iValue);
			_fieldValues.put(iPropertyName, iValue);

		} else if (Set.class.isAssignableFrom(iType) && !(iValue instanceof Set)) {
			// CONVERT IT TO SET
			final Collection<Object> newValue;

			if (iValue instanceof ORecordLazyList || iValue instanceof ORecordLazyMap)
				newValue = new ORecordLazySet(this, RECORD_TYPE);
			else
				newValue = new ORecordTrackedSet(this);

			if (iValue instanceof Collection)
				newValue.addAll((Collection<Object>) iValue);
			else if (iValue instanceof Map)
				newValue.addAll(((Map<String, Object>) iValue).values());

			_fieldValues.put(iPropertyName, newValue);
			iValue = (RET) newValue;

		} else if (List.class.isAssignableFrom(iType) && !(iValue instanceof List)) {
			// CONVERT IT TO LIST
			final Collection<Object> newValue;

			if (iValue instanceof ORecordLazySet || iValue instanceof ORecordLazyMap)
				newValue = new ORecordLazyList(this, RECORD_TYPE);
			else
				newValue = new ORecordTrackedList(this);

			if (iValue instanceof Collection)
				newValue.addAll((Collection<Object>) iValue);
			else if (iValue instanceof Map)
				newValue.addAll(((Map<String, Object>) iValue).values());

			_fieldValues.put(iPropertyName, newValue);
			iValue = (RET) newValue;
		} else
			iValue = (RET) OType.convert(iValue, iType);

		return iValue;
	}

	protected void setFieldType(final String iPropertyName, OType iType) {
		if (iType == null)
			return;

		// SAVE FORCED TYPE
		if (_fieldTypes == null)
			_fieldTypes = new HashMap<String, OType>();
		_fieldTypes.put(iPropertyName, iType);
	}

	public void fromString(final String iValue) {
		_dirty = true;
		_source = OBinaryProtocol.string2bytes(iValue);
		_fieldOriginalValues = null;
		_fieldTypes = null;
		_fieldValues = null;
		_cursor = 0;
	}
}
