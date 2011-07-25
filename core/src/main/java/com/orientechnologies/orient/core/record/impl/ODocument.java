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

import java.lang.ref.WeakReference;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.ORecordTrackedList;
import com.orientechnologies.orient.core.db.record.ORecordTrackedSet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
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
 * Document representation to handle values dynamically. Can be used in schema-less, schema-mixed and schema-full modes. Fields can
 * be added at run-time. Instances can be reused across calls by using the reset() before to re-use.
 */
@SuppressWarnings({ "unchecked", "serial" })
public class ODocument extends ORecordVirtualAbstract<Object> implements Iterable<Entry<String, Object>> {
	public static final byte											RECORD_TYPE	= 'd';

	protected List<WeakReference<ORecordElement>>	_owners			= null;

	/**
	 * Internal constructor used on unmarshalling.
	 */
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
	public ODocument(final ODatabaseRecord iDatabase) {
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
	public ODocument(final ODatabaseRecord iDatabase, final ORID iRID) {
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
	public ODocument(final ODatabaseRecord iDatabase, final String iClassName, final ORID iRID) {
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
	public ODocument(final ODatabaseRecord iDatabase, final String iClassName) {
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
		super(((OClassImpl) iClass).getDocument().getDatabase());
		setup();
		_clazz = iClass;
	}

	/**
	 * Fills a document passing the field array in form of pairs of field name and value.
	 * 
	 * @param iFields
	 *          Array of field pairs
	 */
	public ODocument(final Object[] iFields) {
		_recordId = new ORecordId();
		if (iFields != null && iFields.length > 0)
			for (int i = 0; i < iFields.length; i += 2) {
				field(iFields[i].toString(), iFields[i + 1]);
			}
	}

	/**
	 * Fills a document passing the field names/values pair, where the first pair is mandatory.
	 * 
	 */
	public ODocument(final String iFieldName, final Object iFieldValue, final Object... iFields) {
		this(iFields);
		field(iFieldName, iFieldValue);
	}

	/**
	 * Copies the current instance to a new one. Hasn't been choose the clone() to let ODocument return type. Once copied the new
	 * instance has the same identity and values but all the internal structure are totally independent by the source.
	 */
	public ODocument copy() {
		final ODocument cloned = (ODocument) copyTo(new ODocument());
		cloned._ordered = _ordered;
		cloned._clazz = _clazz;
		cloned._trackingChanges = _trackingChanges;

		if (_fieldValues != null) {
			cloned._fieldValues = new LinkedHashMap<String, Object>();
			for (Entry<String, Object> entry : _fieldValues.entrySet())
				copyFieldValue(cloned, entry);
		}

		if (_fieldTypes != null)
			cloned._fieldTypes = new HashMap<String, OType>(_fieldTypes);

		cloned._fieldOriginalValues = null;

		return cloned;
	}

	@Override
	public ODocument flatCopy() {
		if (isDirty())
			throw new IllegalStateException("Can't execute a flat copy of a dirty record");

		final ODocument cloned = new ODocument();
		cloned.fill(_database, _recordId, _version, _source, false);
		return cloned;
	}

	/**
	 * Returns an empty record as place-holder of the current. Used when a record is requested, but only the identity is needed.
	 * 
	 * @return
	 */
	public ORecord<?> placeholder() {
		final ODocument cloned = new ODocument();
		cloned._source = null;
		cloned._database = _database;
		cloned._recordId = _recordId.copy();
		cloned._status = STATUS.NOT_LOADED;
		return cloned;
	}

	public boolean detach() {
		_database = null;
		boolean fullyDetached = true;

		if (_fieldValues != null) {
			Object fieldValue;
			for (Map.Entry<String, Object> entry : _fieldValues.entrySet()) {
				fieldValue = entry.getValue();

				if (fieldValue instanceof ORecord<?>)
					if (((ORecord<?>) fieldValue).getIdentity().isNew())
						fullyDetached = false;
					else
						_fieldValues.put(entry.getKey(), ((ORecord<?>) fieldValue).getIdentity());
				else if (fieldValue instanceof ORecordLazyMultiValue) {
					if (!((ORecordLazyMultiValue) fieldValue).convertRecords2Links())
						fullyDetached = false;
				}
			}
		}

		return fullyDetached;
	}

	/**
	 * Loads the record using a fetch plan. Example:
	 * <p>
	 * <code>doc.load( "*:3" ); // LOAD THE DOCUMENT BY EARLY FETCHING UP TO 3rd LEVEL OF CONNECTIONS</code>
	 * </p>
	 * 
	 * @param iFetchPlan
	 *          Fetch plan to use
	 */
	public ODocument load(final String iFetchPlan) {
		return load(iFetchPlan, false);
	}

	/**
	 * Loads the record using a fetch plan. Example:
	 * <p>
	 * <code>doc.load( "*:3", true ); // LOAD THE DOCUMENT BY EARLY FETCHING UP TO 3rd LEVEL OF CONNECTIONS IGNORING THE CACHE</code>
	 * </p>
	 * 
	 * @param iIgnoreCache
	 *          Ignore the cache or use it
	 */
	public ODocument load(final String iFetchPlan, boolean iIgnoreCache) {
		if (_database == null)
			throw new ODatabaseException("No database assigned to current record");

		Object result = null;
		try {
			result = _database.load(this, iFetchPlan, iIgnoreCache);
		} catch (Exception e) {
			throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' was not found", e);
		}

		if (result == null)
			throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' was not found");

		return (ODocument) result;
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
	public boolean hasSameContentOf(ODocument iOther) {
		if (iOther == null)
			return false;

		if (!equals(iOther) && _recordId.isValid())
			return false;

		if (_status == STATUS.NOT_LOADED)
			reload();
		if (iOther._status == STATUS.NOT_LOADED)
			iOther = (ODocument) iOther.load();

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
				if (myFieldValue instanceof Collection && otherFieldValue instanceof Collection) {
					final Collection<?> myCollection = (Collection<?>) myFieldValue;
					final Collection<?> otherCollection = (Collection<?>) otherFieldValue;

					if (myCollection.size() != otherCollection.size())
						return false;

					Iterator<?> myIterator = myCollection.iterator();
					Iterator<?> otherIterator = otherCollection.iterator();

					while (myIterator.hasNext()) {
						hasSameContentItem(myIterator.next(), otherIterator.next());
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

	private boolean hasSameContentItem(final Object my, final Object other) {
		if (my instanceof ODocument) {
			if (other instanceof ORID) {
				if (!((ODocument) my).isDirty()) {
					if (!((ODocument) my).getIdentity().equals(other))
						return false;
				} else {
					ODocument otherDoc = (ODocument) getDatabase().load((ORID) other);
					if (!((ODocument) my).hasSameContentOf(otherDoc))
						return false;
				}
			} else if (!((ODocument) my).hasSameContentOf((ODocument) other))
				return false;
		} else if (!my.equals(other))
			return false;
		return true;
	}

	/**
	 * Dumps the instance as string.
	 */
	@Override
	public String toString() {
		final boolean saveDirtyStatus = _dirty;

		final StringBuilder buffer = new StringBuilder();

		try {
			checkForFields();
			if (_clazz != null)
				buffer.append(_clazz.getStreamableName());

			if (_recordId != null) {
				if (_recordId.isValid())
					buffer.append(_recordId);
			}

			boolean first = true;
			ORecord<?> record;
			for (Entry<String, Object> f : _fieldValues.entrySet()) {
				buffer.append(first ? '{' : ',');
				buffer.append(f.getKey());
				buffer.append(':');
				if (f.getValue() instanceof Collection<?>) {
					buffer.append('[');
					buffer.append(((Collection<?>) f.getValue()).size());
					buffer.append(']');
				} else if (f.getValue() instanceof ORecord<?>) {
					record = (ORecord<?>) f.getValue();

					if (record.getIdentity() != null)
						record.getIdentity().toString(buffer);
					else
						buffer.append(record.toString());
				} else
					buffer.append(f.getValue());

				if (first)
					first = false;
			}
			if (!first)
				buffer.append('}');

			buffer.append(" v");
			buffer.append(_version);

		} finally {
			_dirty = saveDirtyStatus;
		}

		return buffer.toString();
	}

	/**
	 * Fills the ODocument directly with the string representation of the document itself. Use it for faster insertion but pay
	 * attention to respect the OrientDB record format.
	 * <p>
	 * <code>
	 * record.reset();<br/>
	 * record.setClassName("Account");<br/>
	 * record.fromString(new String("Account@id:" + data.getCyclesDone() + ",name:'Luca',surname:'Garulli',birthDate:" + date.getTime()<br/>
	 * 		+ ",salary:" + 3000f + i));<br/>
	 * record.save();<br/>
</code>
	 * </p>
	 * 
	 * @param iValue
	 */
	public void fromString(final String iValue) {
		_dirty = true;
		_source = OBinaryProtocol.string2bytes(iValue);
		_fieldOriginalValues = null;
		_fieldTypes = null;
		_fieldValues = null;
	}

	/**
	 * Returns the set of field names.
	 */
	public Set<String> fieldNames() {
		checkForLoading();
		checkForFields();

		return new LinkedHashSet<String>(_fieldValues.keySet());
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
				linkedRecord.reload();

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

		final OType t = fieldType(iPropertyName);

		if (_lazyLoad && value instanceof ORID && t != OType.LINK && _database != null) {
			// CREATE THE DOCUMENT OBJECT IN LAZY WAY
			value = (RET) _database.load((ORID) value);
			_fieldValues.put(iPropertyName, value);
		}

		// CHECK FOR CONVERSION
		if (t != null) {
			Object newValue = null;

			if (t == OType.BINARY && value instanceof String)
				newValue = OBase64Utils.decode((String) value);
			else if ((t == OType.DATE || t == OType.DATE) && value instanceof Long)
				newValue = (RET) new Date(((Long) value).longValue());

			if (newValue != null) {
				// VALUE CHANGED: SET THE NEW ONE
				_fieldValues.put(iPropertyName, newValue);
				value = (RET) newValue;
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

		if (value != null)
			value = (RET) convertField(iPropertyName, iType, value);

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
	public ODocument field(String iPropertyName, Object iPropertyValue, OType iType) {
		iPropertyName = checkFieldName(iPropertyName);

		checkForLoading();
		checkForFields();

		_source = null;

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
					if (iPropertyValue == oldValue) {
						if (!(iPropertyValue instanceof ORecordElement))
							// SAME BUT NOT TRACKABLE: SET THE RECORD AS DIRTY TO BE SURE IT'S SAVED
							setDirty();

						// SAVE VALUE: UNCHANGED
						return this;
					}
				} catch (Exception e) {
					OLogManager.instance().warn(this, "Error on checking the value of property %s against the record %s", e, iPropertyName,
							getIdentity());
				}
			}

		if (iType != null)
			setFieldType(iPropertyName, iType);
		else if (_clazz != null) {
			// SCHEMAFULL?
			final OProperty prop = _clazz.getProperty(iPropertyName);
			if (prop != null)
				iType = prop.getType();
		}

		if (iPropertyValue == null) {
			_fieldValues.put(iPropertyName, iPropertyValue);
		} else {
			if (iType != null) {
				iPropertyValue = convertField(iPropertyName, iType.getDefaultJavaType(), iPropertyValue);
			} else if (iPropertyValue instanceof Enum) {
				iPropertyValue = iPropertyValue.toString();
			}

			if (_status != STATUS.UNMARSHALLING) {
				setDirty();

				if (_trackingChanges && _recordId.isValid()) {
					// SAVE THE OLD VALUE IN A SEPARATE MAP
					if (_fieldOriginalValues == null)
						_fieldOriginalValues = new HashMap<String, Object>();

					// INSERT IT ONLY IF NOT EXISTS TO AVOID LOOSE OF THE ORIGINAL VALUE (FUNDAMENTAL FOR INDEX HOOK)
					if (!_fieldOriginalValues.containsKey(iPropertyName))
						_fieldOriginalValues.put(iPropertyName, oldValue);
				}
			}

			_fieldValues.put(iPropertyName, iPropertyValue);
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

			// INSERT IT ONLY IF NOT EXISTS TO AVOID LOOSE OF THE ORIGINAL VALUE (FUNDAMENTAL FOR INDEX HOOK)
			if (!_fieldOriginalValues.containsKey(iPropertyName))
				_fieldOriginalValues.put(iPropertyName, oldValue);
		}

		_fieldValues.remove(iPropertyName);
		_source = null;

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
		iOther.checkForLoading();
		iOther.checkForFields();

		if (_clazz == null && iOther.getSchemaClass() != null)
			_clazz = iOther.getSchemaClass();

		return merge(iOther._fieldValues, iConflictsOtherWins, iMergeSingleItemsOfMultiValueFields);
	}

	/**
	 * Merge current document with the document passed as parameter. If the field already exists then the conflicts are managed based
	 * on the value of the parameter 'iConflictsOtherWins'.
	 * 
	 * @param iOther
	 *          Other ODocument instance to merge
	 * @param iAddOnlyMode
	 *          if true, the other document properties will be always added. If false, the missed propertie in the "other" document
	 *          will be removed by original too
	 * @param iMergeSingleItemsOfMultiValueFields
	 * @return
	 */
	public ODocument merge(final Map<String, Object> iOther, final boolean iAddOnlyMode, boolean iMergeSingleItemsOfMultiValueFields) {
		checkForLoading();
		checkForFields();

		_source = null;

		for (String f : iOther.keySet()) {
			if (containsField(f) && iMergeSingleItemsOfMultiValueFields) {
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

					// JUMP RAW REPLACE
					continue;
				}
			}

			// RAW SET/REPLACE
			field(f, iOther.get(f));
		}

		if (!iAddOnlyMode) {
			// REMOVE PROPERTIES NOT FOUND IN OTHER DOC
			final Set<String> fieldNames = fieldNames();
			for (String f : fieldNames)
				if (!iOther.containsKey(f))
					removeField(f);
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
		return (Set<String>) (_fieldOriginalValues != null ? Collections.unmodifiableSet(_fieldOriginalValues.keySet()) : Collections
				.emptySet());
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
	 * Returns the iterator against only the changed fields if tracking was enabled
	 */
	public Iterator<Entry<String, Object>> iterator() {
		checkForLoading();
		checkForFields();

		if (_fieldValues == null)
			return OEmptyIterator.INSTANCE;

		return _fieldValues.entrySet().iterator();
	}

	@Override
	public boolean setDatabase(final ODatabaseRecord iDatabase) {
		if (super.setDatabase(iDatabase)) {
			if (_fieldValues != null)
				for (Object f : _fieldValues.values()) {
					if (f instanceof ORecordElement)
						((ORecordElement) f).setDatabase(iDatabase);
				}
			return true;
		}
		return false;
	}

	/**
	 * Checks if a field exists.
	 * 
	 * @return True if exists, otherwise false.
	 */
	public boolean containsField(final String iFieldName) {
		if (iFieldName == null)
			return false;

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

	/**
	 * Returns true if the record has some owner.
	 */
	public boolean hasOwners() {
		return _owners != null && !_owners.isEmpty();
	}

	/**
	 * Internal.
	 * 
	 * @return
	 */
	public ODocument addOwner(final ORecordElement iOwner) {
		if (_owners == null)
			_owners = new ArrayList<WeakReference<ORecordElement>>();
		this._owners.add(new WeakReference<ORecordElement>(iOwner));
		return this;
	}

	public ODocument removeOwner(final ORecordElement iRecordElement) {
		if (_owners != null) {
			// PROPAGATES TO THE OWNER
			ORecordElement e;
			for (int i = 0; i < _owners.size(); ++i) {
				e = _owners.get(i).get();
				if (e == iRecordElement) {
					_owners.remove(i);
					break;
				}
			}
		}
		return this;
	}

	/**
	 * Propagates the dirty status to the owner, if any. This happens when the object is embedded in another one.
	 */
	@Override
	public ORecordAbstract<Object> setDirty() {
		if (_owners != null) {
			// PROPAGATES TO THE OWNER
			ORecordElement e;
			for (WeakReference<ORecordElement> o : _owners) {
				e = o.get();
				if (e != null)
					e.setDirty();
			}
		}
		// THIS IS IMPORTANT TO BE SURE THAT FIELDS ARE LOADED BEFORE IT'S TOO LATE AND THE RECORD _SOURCE IS NULL
		checkForFields();

		return super.setDirty();
	}

	@Override
	public void onBeforeIdentityChanged(final ORID iRID) {
		if (_owners != null) {
			final List<WeakReference<ORecordElement>> temp = new ArrayList<WeakReference<ORecordElement>>(_owners);

			ORecordElement e;
			for (WeakReference<ORecordElement> o : temp) {
				e = o.get();
				if (e != null)
					e.onBeforeIdentityChanged(iRID);
			}
		}
	}

	@Override
	public void onAfterIdentityChanged(final ORecord<?> iRecord) {
		if (_owners != null) {
			final List<WeakReference<ORecordElement>> temp = new ArrayList<WeakReference<ORecordElement>>(_owners);

			ORecordElement e;
			for (WeakReference<ORecordElement> o : temp) {
				e = o.get();
				if (e != null)
					e.onAfterIdentityChanged(iRecord);
			}
		}
	}

	/**
	 * Internal.
	 */
	@Override
	protected void setup() {
		super.setup();
		_recordFormat = ORecordSerializerFactory.instance().getFormat(ORecordSerializerSchemaAware2CSV.NAME);
	}

	private <RET> RET convertField(final String iPropertyName, final Class<?> iType, Object iValue) {
		if (iType == null)
			return (RET) iValue;

		if (ORID.class.isAssignableFrom(iType)) {
			if (iValue instanceof ORID) {
				return (RET) iValue;
			} else if (iValue instanceof String) {
				return (RET) new ORecordId((String) iValue);
			} else if (iValue instanceof ORecord<?>) {
				return (RET) ((ORecord<?>) iValue).getIdentity();
			}
		} else if (ORecord.class.isAssignableFrom(iType)) {
			if (iValue instanceof ORID || iValue instanceof ORecord<?>) {
				return (RET) iValue;
			} else if (iValue instanceof String) {
				return (RET) new ORecordId((String) iValue);
			}
		} else if (Set.class.isAssignableFrom(iType)) {
			if (!(iValue instanceof Set)) {
				// CONVERT IT TO SET
				final Collection<?> newValue;

				if (iValue instanceof ORecordLazyList || iValue instanceof ORecordLazyMap)
					newValue = new ORecordLazySet(this);
				else
					newValue = new OTrackedSet<Object>(this);

				if (iValue instanceof Collection<?>) {
					((Collection<Object>) newValue).addAll((Collection<Object>) iValue);
					return (RET) newValue;
				} else if (iValue instanceof Map) {
					((Collection<Object>) newValue).addAll(((Map<String, Object>) iValue).values());
					return (RET) newValue;
				} else if (iValue instanceof String) {
					final String stringValue = (String) iValue;

					if (stringValue != null && stringValue.length() > 0) {
						final String[] items = stringValue.split(",");
						for (String s : items) {
							((Collection<Object>) newValue).add(s);
						}
					}
					return (RET) newValue;
				}
			} else {
				return (RET) iValue;
			}
		} else if (List.class.isAssignableFrom(iType)) {
			if (!(iValue instanceof List)) {
				// CONVERT IT TO LIST
				final Collection<?> newValue;

				if (iValue instanceof ORecordLazySet || iValue instanceof ORecordLazyMap)
					newValue = new ORecordLazyList(this);
				else
					newValue = new OTrackedList<Object>(this);

				if (iValue instanceof Collection) {
					((Collection<Object>) newValue).addAll((Collection<Object>) iValue);
					return (RET) newValue;
				} else if (iValue instanceof Map) {
					((Collection<Object>) newValue).addAll(((Map<String, Object>) iValue).values());
					return (RET) newValue;
				} else if (iValue instanceof String) {
					final String stringValue = (String) iValue;

					if (stringValue != null && stringValue.length() > 0) {
						final String[] items = stringValue.split(",");
						for (String s : items) {
							((Collection<Object>) newValue).add(s);
						}
					}
					return (RET) newValue;
				}
			} else {
				return (RET) iValue;
			}
		} else if (iValue instanceof Enum) {
			// ENUM
			if (Number.class.isAssignableFrom(iType))
				iValue = ((Enum<?>) iValue).ordinal();
			else
				iValue = iValue.toString();
			if (!(iValue instanceof String) && !iType.isAssignableFrom(iValue.getClass()))
				throw new IllegalArgumentException("Property '" + iPropertyName + "' of type '" + iType + "' can't accept value of type: "
						+ iValue.getClass());
		} else if (Date.class.isAssignableFrom(iType)) {
			if (iValue instanceof String && _database != null) {
				final OStorageConfiguration config = _database.getStorage().getConfiguration();

				DateFormat formatter = config.getDateFormatInstance();

				if (((String) iValue).length() > config.dateFormat.length()) {
					// ASSUMES YOU'RE USING THE DATE-TIME FORMATTE
					formatter = config.getDateTimeFormatInstance();
				}

				try {
					Date newValue = formatter.parse((String) iValue);
					// _fieldValues.put(iPropertyName, newValue);
					return (RET) newValue;
				} catch (ParseException pe) {
					final String dateFormat = ((String) iValue).length() > config.dateFormat.length() ? config.dateTimeFormat
							: config.dateFormat;
					throw new OQueryParsingException("Error on conversion of date '" + iValue + "' using the format: " + dateFormat);
				}
			}
		}

		iValue = OType.convert(iValue, iType);

		return (RET) iValue;
	}

	protected void setFieldType(final String iPropertyName, final OType iType) {
		if (iType == null)
			return;

		// SAVE FORCED TYPE
		if (_fieldTypes == null)
			_fieldTypes = new HashMap<String, OType>();
		_fieldTypes.put(iPropertyName, iType);
	}

	private void copyFieldValue(final ODocument iCloned, final Entry<String, Object> iEntry) {
		final Object fieldValue = iEntry.getValue();

		if (fieldValue != null)
			// LISTS
			if (fieldValue instanceof ORecordLazyList) {
				iCloned._fieldValues.put(iEntry.getKey(), ((ORecordLazyList) fieldValue).copy(iCloned));

			} else if (fieldValue instanceof ORecordTrackedList) {
				final ORecordTrackedList newList = new ORecordTrackedList(iCloned);
				newList.addAll((ORecordTrackedList) fieldValue);
				iCloned._fieldValues.put(iEntry.getKey(), newList);

			} else if (fieldValue instanceof OTrackedList<?>) {
				final OTrackedList<Object> newList = new OTrackedList<Object>(iCloned);
				newList.addAll((OTrackedList<Object>) fieldValue);
				iCloned._fieldValues.put(iEntry.getKey(), newList);

			} else if (fieldValue instanceof List<?>) {
				iCloned._fieldValues.put(iEntry.getKey(), new ArrayList<Object>((List<Object>) fieldValue));

				// SETS
			} else if (fieldValue instanceof ORecordLazySet) {
				iCloned._fieldValues.put(iEntry.getKey(), ((ORecordLazySet) fieldValue).copy(iCloned));

			} else if (fieldValue instanceof ORecordTrackedSet) {
				final ORecordTrackedSet newList = new ORecordTrackedSet(iCloned);
				newList.addAll((ORecordTrackedSet) fieldValue);
				iCloned._fieldValues.put(iEntry.getKey(), newList);

			} else if (fieldValue instanceof OTrackedSet<?>) {
				final OTrackedSet<Object> newList = new OTrackedSet<Object>(iCloned);
				newList.addAll((OTrackedSet<Object>) fieldValue);
				iCloned._fieldValues.put(iEntry.getKey(), newList);

			} else if (fieldValue instanceof Set<?>) {
				iCloned._fieldValues.put(iEntry.getKey(), new HashSet<Object>((Set<Object>) fieldValue));

				// MAPS
			} else if (fieldValue instanceof ORecordLazyMap) {
				final ORecordLazyMap newMap = new ORecordLazyMap(iCloned, ((ORecordLazyMap) fieldValue).getRecordType());
				newMap.putAll((ORecordLazyMap) fieldValue);
				iCloned._fieldValues.put(iEntry.getKey(), newMap);

			} else if (fieldValue instanceof OTrackedMap) {
				final OTrackedMap<Object> newMap = new OTrackedMap<Object>(iCloned);
				newMap.putAll((OTrackedMap<Object>) fieldValue);
				iCloned._fieldValues.put(iEntry.getKey(), newMap);

			} else if (fieldValue instanceof Map<?, ?>) {
				iCloned._fieldValues.put(iEntry.getKey(), new LinkedHashMap<String, Object>((Map<String, Object>) fieldValue));
			} else
				iCloned._fieldValues.put(iEntry.getKey(), fieldValue);
	}

	protected String checkFieldName(String iPropertyName) {
		if (iPropertyName == null)
			throw new IllegalArgumentException("Field name is null");

		iPropertyName = iPropertyName.trim();

		if (iPropertyName.length() == 0)
			throw new IllegalArgumentException("Field name is empty");

		for (int i = 0; i < iPropertyName.length(); ++i) {
			final char c = iPropertyName.charAt(i);
			if (c == ':' || c == ',')
				throw new IllegalArgumentException("Invalid field name '" + iPropertyName + "'");
		}

		// if (!Character.isJavaIdentifierStart(iPropertyName.charAt(0)))
		// throw new IllegalArgumentException("Invalid property name");
		//
		// for (int i = 1; i < iPropertyName.length(); ++i)
		// if (!Character.isJavaIdentifierPart(iPropertyName.charAt(i)))
		// throw new IllegalArgumentException("Invalid property name");

		return iPropertyName;
	}
}
