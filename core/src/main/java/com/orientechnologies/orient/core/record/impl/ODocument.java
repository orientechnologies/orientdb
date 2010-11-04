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
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
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

		if (_fieldValues != null)
			cloned._fieldValues = new LinkedHashMap<String, Object>(_fieldValues);

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
		for (Entry<String, Object> f : _fieldValues.entrySet()) {
			buffer.append(first ? "{" : ",");
			buffer.append(f.getKey());
			buffer.append(":");
			if (f.getValue() instanceof Collection<?>) {
				buffer.append("[");
				buffer.append(((Collection<?>) f.getValue()).size());
				buffer.append("]");
			} else if (f.getValue() instanceof ORecord<?>) {
				buffer.append("#");
				buffer.append(((ORecord<?>) f.getValue()).getIdentity());
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

		if (value instanceof ORID && !ORID.class.equals(iType) && !ORecordId.class.equals(iType)) {
			// CREATE THE DOCUMENT OBJECT IN LAZY WAY
			value = (RET) new ODocument(_database, (ORID) value);
			_fieldValues.put(iPropertyName, value);
		}

		return value;
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
					if (prop.getType().isAssignableFrom(""))
						iPropertyValue = iPropertyValue.toString();
					else if (prop.getType().isAssignableFrom(1))
						iPropertyValue = ((Enum<?>) iPropertyValue).ordinal();

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

		if (iType != null) {
			// SAVE FORCED TYPE
			if (_fieldTypes == null)
				_fieldTypes = new HashMap<String, OType>();
			_fieldTypes.put(iPropertyName, iType);
		}

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
}
