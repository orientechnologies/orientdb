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
import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import com.orientechnologies.orient.core.index.OPropertyIndexManager;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordSchemaAwareAbstract;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;

/**
 * Document representation to handle values dynamically. Can be used in schema-less, schema-mixed and schema-full modes. Fields can
 * be added at run-time. Instances can be reused across calls by using the reset() before to re-use.
 */
@SuppressWarnings({ "unchecked", "serial" })
public class ODocument extends ORecordSchemaAwareAbstract<Object> implements Iterable<Entry<String, Object>> {
	public static final byte											RECORD_TYPE				= 'd';
	protected Map<String, Object>									_fieldValues;
	protected Map<String, Object>									_fieldOriginalValues;
	protected Map<String, OType>									_fieldTypes;
	protected boolean															_trackingChanges	= true;
	protected boolean															_ordered					= true;
	protected boolean															_lazyLoad					= true;

	protected List<WeakReference<ORecordElement>>	_owners						= null;

	private static final String[]									EMPTY_STRINGS			= new String[] {};

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
		_source = iSource;
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
		super(iDatabase);
		setClassName(iClassName);
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
			cloned._fieldValues = _fieldValues instanceof LinkedHashMap ? new LinkedHashMap<String, Object>()
					: new HashMap<String, Object>();
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
	public String[] fieldNames() {
		checkForLoading();
		checkForFields();

		if (_fieldValues == null || _fieldValues.size() == 0)
			return EMPTY_STRINGS;

		return _fieldValues.keySet().toArray(new String[_fieldValues.keySet().size()]);
	}

	/**
	 * Returns the array of field values.
	 */
	public Object[] fieldValues() {
		checkForLoading();
		checkForFields();

		return _fieldValues.values().toArray(new Object[_fieldValues.values().size()]);
	}

	public <RET> RET rawField(final String iFieldName) {
		checkForLoading();
		checkForFields();

		int separatorPos = 0;

		final int fieldNameLength = iFieldName.length();
		Object value = null;
		ODocument current = this;

		do {
			char separator = ' ';
			for (; separatorPos < fieldNameLength; ++separatorPos) {
				separator = iFieldName.charAt(separatorPos);
				if (separator == '.' || separator == '[')
					break;
			}

			final String fieldName;
			if (separatorPos < fieldNameLength)
				fieldName = iFieldName.substring(0, separatorPos);
			else
				fieldName = iFieldName;

			if (separator == '.') {
				// GET THE LINKED OBJECT IF ANY
				value = current._fieldValues.get(fieldName);

				if (value == null || !(value instanceof ODocument))
					// IGNORE IT BY RETURNING NULL
					return null;

				current = (ODocument) value;
				if (current.getInternalStatus() == STATUS.NOT_LOADED)
					// LAZY LOAD IT
					current.reload();
			} else if (separator == '[') {
				if (value == null)
					value = current._fieldValues.get(fieldName);

				final int end = iFieldName.indexOf(']', separatorPos);
				if (end == -1)
					throw new IllegalArgumentException("Missed closed ']'");

				final String index = OStringSerializerHelper.getStringContent(iFieldName.substring(separatorPos + 1, end));
				separatorPos = end + 1;

				if (value == null)
					return null;
				else if (value instanceof ODocument)
					value = ((ODocument) value).field(index);
				else if (value instanceof Map<?, ?>)
					value = ((Map<?, ?>) value).get(index);
				else if (value instanceof List<?>)
					value = ((List<?>) value).get(Integer.parseInt(index));
				else if (value.getClass().isArray())
					value = Array.get(value, Integer.parseInt(index));
			} else
				value = current._fieldValues.get(fieldName);

		} while (separatorPos < fieldNameLength);

		return (RET) value;
	}

	/**
	 * Reads the field value.
	 * 
	 * @param iFieldName
	 *          field name
	 * @return field value if defined, otherwise null
	 */
	public <RET> RET field(final String iFieldName) {
		RET value = this.<RET> rawField(iFieldName);

		final OType t = fieldType(iFieldName);

		if (_lazyLoad && value instanceof ORID && t != OType.LINK && _database != null) {
			// CREATE THE DOCUMENT OBJECT IN LAZY WAY
			value = (RET) _database.load((ORID) value);
			_fieldValues.put(iFieldName, value);
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
				_fieldValues.put(iFieldName, newValue);
				value = (RET) newValue;
			}
		}

		return value;
	}

	/**
	 * Reads the field value forcing the return type. Use this method to force return of ORID instead of the entire document by
	 * passing ORID.class as iFieldType.
	 * 
	 * @param iFieldName
	 *          field name
	 * @param iFieldType
	 *          Forced type.
	 * @return field value if defined, otherwise null
	 */
	public <RET> RET field(final String iFieldName, final Class<?> iFieldType) {
		RET value = this.<RET> rawField(iFieldName);

		if (value != null)
			value = (RET) convertField(iFieldName, iFieldType, value);

		return value;
	}

	/**
	 * Reads the field value forcing the return type. Use this method to force return of binary data.
	 * 
	 * @param iFieldName
	 *          field name
	 * @param iFieldType
	 *          Forced type.
	 * @return field value if defined, otherwise null
	 */
	public <RET> RET field(final String iFieldName, final OType iFieldType) {
		setFieldType(iFieldName, iFieldType);
		return (RET) field(iFieldName);
	}

	/**
	 * Writes the field value.
	 * 
	 * @param iFieldName
	 *          field name
	 * @param iPropertyValue
	 *          field value
	 * @return The Record instance itself giving a "fluent interface". Useful to call multiple methods in chain.
	 */
	public ODocument field(final String iFieldName, Object iPropertyValue) {
		return field(iFieldName, iPropertyValue, null);
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
	 * @param iFieldName
	 *          field name
	 * @param iPropertyValue
	 *          field value
	 * @param iFieldType
	 *          Forced type (not auto-determined)
	 * @return The Record instance itself giving a "fluent interface". Useful to call multiple methods in chain.
	 */
	public ODocument field(String iFieldName, Object iPropertyValue, OType iFieldType) {
		iFieldName = checkFieldName(iFieldName);

		checkForLoading();
		checkForFields();

		_source = null;

		final boolean knownProperty = _fieldValues.containsKey(iFieldName);
		final Object oldValue = _fieldValues.get(iFieldName);

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
					OLogManager.instance().warn(this, "Error on checking the value of property %s against the record %s", e, iFieldName,
							getIdentity());
				}
			}

		if (iFieldType != null)
			setFieldType(iFieldName, iFieldType);
		else if (_clazz != null) {
			// SCHEMAFULL?
			final OProperty prop = _clazz.getProperty(iFieldName);
			if (prop != null)
				iFieldType = prop.getType();
		}

		if (iPropertyValue == null) {
			_fieldValues.put(iFieldName, iPropertyValue);
		} else {
			if (iFieldType != null) {
				iPropertyValue = convertField(iFieldName, iFieldType.getDefaultJavaType(), iPropertyValue);
			} else if (iPropertyValue instanceof Enum) {
				iPropertyValue = iPropertyValue.toString();
			}

			if (_status != STATUS.UNMARSHALLING) {
				setDirty();

				if (_trackingChanges && _recordId.isValid()) {
					// SAVE THE OLD VALUE IN A SEPARATE MAP ONLY IF TRACKING IS ACTIVE AND THE RECORD IS NOT NEW
					if (_fieldOriginalValues == null)
						_fieldOriginalValues = new HashMap<String, Object>();

					// INSERT IT ONLY IF NOT EXISTS TO AVOID LOOSE OF THE ORIGINAL VALUE (FUNDAMENTAL FOR INDEX HOOK)
					if (!_fieldOriginalValues.containsKey(iFieldName))
						_fieldOriginalValues.put(iFieldName, oldValue);
				}
			}

			_fieldValues.put(iFieldName, iPropertyValue);
		}

		return this;
	}

	/**
	 * Removes a field.
	 */
	public Object removeField(final String iFieldName) {
		checkForLoading();
		checkForFields();

		final boolean knownProperty = _fieldValues.containsKey(iFieldName);
		final Object oldValue = _fieldValues.get(iFieldName);

		if (knownProperty && _trackingChanges) {
			// SAVE THE OLD VALUE IN A SEPARATE MAP
			if (_fieldOriginalValues == null)
				_fieldOriginalValues = new HashMap<String, Object>();

			// INSERT IT ONLY IF NOT EXISTS TO AVOID LOOSE OF THE ORIGINAL VALUE (FUNDAMENTAL FOR INDEX HOOK)
			if (!_fieldOriginalValues.containsKey(iFieldName))
				_fieldOriginalValues.put(iFieldName, oldValue);
		}

		_fieldValues.remove(iFieldName);
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
			for (String f : fieldNames())
				if (!iOther.containsKey(f))
					removeField(f);
		}

		return this;
	}

	/**
	 * Returns the original value of a field before it has been changed.
	 * 
	 * @param iFieldName
	 *          Property name to retrieve the original value
	 */
	public String[] getDirtyFields() {
		if (_fieldOriginalValues == null || _fieldOriginalValues.size() == 0)
			return EMPTY_STRINGS;

		return _fieldOriginalValues.keySet().toArray(new String[_fieldOriginalValues.keySet().size()]);
	}

	/**
	 * Returns the original value of a field before it has been changed.
	 * 
	 * @param iFieldName
	 *          Property name to retrieve the original value
	 */
	public Object getOriginalValue(final String iFieldName) {
		return _fieldOriginalValues != null ? _fieldOriginalValues.get(iFieldName) : null;
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

	@Override
	public ODocument fromStream(byte[] iRecordBuffer) {
		_fieldValues = null;
		_fieldTypes = null;
		_fieldOriginalValues = null;
		return (ODocument) super.fromStream(iRecordBuffer);
	}

	@Override
	public void unsetDirty() {
		_fieldOriginalValues = null;
		super.unsetDirty();
	}

	/**
	 * Returns the forced field type if any.
	 * 
	 * @param iFieldName
	 */
	public OType fieldType(final String iFieldName) {
		return _fieldTypes != null ? _fieldTypes.get(iFieldName) : null;
	}

	@Override
	public ODocument unload() {
		super.unload();
		if (_fieldValues != null)
			_fieldValues.clear();
		return this;
	}

	/**
	 * Clear all the field values and types.
	 */
	@Override
	public ODocument clear() {
		super.clear();
		if (_fieldValues != null)
			_fieldValues.clear();
		return this;
	}

	/**
	 * Reset the record values and class type to being reused.
	 */
	@Override
	public ODocument reset() {
		super.reset();
		if (_fieldValues != null)
			_fieldValues.clear();
		return this;
	}

	public boolean isLazyLoad() {
		return _lazyLoad;
	}

	public void setLazyLoad(final boolean iLazyLoad) {
		this._lazyLoad = iLazyLoad;
	}

	public boolean isTrackingChanges() {
		return _trackingChanges;
	}

	/**
	 * Enabled or disabled the tracking of changes in the document. This is needed by some triggers like {@link OPropertyIndexManager}
	 * to determine what fields are changed to update indexes.
	 * 
	 * @param iTrackingChanges
	 *          True to enable it, otherwise false
	 * @return
	 */
	public ODocument setTrackingChanges(final boolean iTrackingChanges) {
		this._trackingChanges = iTrackingChanges;
		if (!iTrackingChanges)
			// FREE RESOURCES
			this._fieldOriginalValues = null;
		return this;
	}

	public boolean isOrdered() {
		return _ordered;
	}

	public ODocument setOrdered(final boolean iOrdered) {
		this._ordered = iOrdered;
		return this;
	}

	@Override
	public boolean equals(Object obj) {
		if (!super.equals(obj))
			return false;

		return this == obj || _recordId.isValid();
	}

	/**
	 * Returns the number of fields in memory.
	 */
	public int fields() {
		return _fieldValues == null ? 0 : _fieldValues.size();
	}

	public boolean isEmpty() {
		return _fieldValues == null || _fieldValues.isEmpty();
	}

	@Override
	protected void checkForFields() {
		if (_fieldValues == null)
			_fieldValues = _ordered ? new LinkedHashMap<String, Object>() : new HashMap<String, Object>();

		if (_status == ORecordElement.STATUS.LOADED && fields() == 0)
			// POPULATE FIELDS LAZY
			deserializeFields();
	}

	/**
	 * Internal.
	 */
	@Override
	protected void setup() {
		super.setup();
		_recordFormat = ORecordSerializerFactory.instance().getFormat(ORecordSerializerSchemaAware2CSV.NAME);
	}

	private <RET> RET convertField(final String iFieldName, final Class<?> iFieldType, Object iValue) {
		if (iFieldType == null)
			return (RET) iValue;

		if (ORID.class.isAssignableFrom(iFieldType)) {
			if (iValue instanceof ORID) {
				return (RET) iValue;
			} else if (iValue instanceof String) {
				return (RET) new ORecordId((String) iValue);
			} else if (iValue instanceof ORecord<?>) {
				return (RET) ((ORecord<?>) iValue).getIdentity();
			}
		} else if (ORecord.class.isAssignableFrom(iFieldType)) {
			if (iValue instanceof ORID || iValue instanceof ORecord<?>) {
				return (RET) iValue;
			} else if (iValue instanceof String) {
				return (RET) new ORecordId((String) iValue);
			}
		} else if (Set.class.isAssignableFrom(iFieldType)) {
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
		} else if (List.class.isAssignableFrom(iFieldType)) {
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
			if (Number.class.isAssignableFrom(iFieldType))
				iValue = ((Enum<?>) iValue).ordinal();
			else
				iValue = iValue.toString();
			if (!(iValue instanceof String) && !iFieldType.isAssignableFrom(iValue.getClass()))
				throw new IllegalArgumentException("Property '" + iFieldName + "' of type '" + iFieldType
						+ "' can't accept value of type: " + iValue.getClass());
		} else if (Date.class.isAssignableFrom(iFieldType)) {
			if (iValue instanceof String && _database != null) {
				final OStorageConfiguration config = _database.getStorage().getConfiguration();

				DateFormat formatter = config.getDateFormatInstance();

				if (((String) iValue).length() > config.dateFormat.length()) {
					// ASSUMES YOU'RE USING THE DATE-TIME FORMATTE
					formatter = config.getDateTimeFormatInstance();
				}

				try {
					Date newValue = formatter.parse((String) iValue);
					// _fieldValues.put(iFieldName, newValue);
					return (RET) newValue;
				} catch (ParseException pe) {
					final String dateFormat = ((String) iValue).length() > config.dateFormat.length() ? config.dateTimeFormat
							: config.dateFormat;
					throw new OQueryParsingException("Error on conversion of date '" + iValue + "' using the format: " + dateFormat);
				}
			}
		}

		iValue = OType.convert(iValue, iFieldType);

		return (RET) iValue;
	}

	/**
	 * Sets the field type. This overrides the schema property settings if any.
	 * 
	 * @param iFieldName
	 *          Field name
	 * @param iFieldType
	 *          Type to set between OType enumaration values
	 */
	public ODocument setFieldType(final String iFieldName, final OType iFieldType) {
		if (iFieldType != null) {
			// SET THE FORCED TYPE
			if (_fieldTypes == null)
				_fieldTypes = new HashMap<String, OType>();
			_fieldTypes.put(iFieldName, iFieldType);
		} else if (_fieldTypes != null) {
			// REMOVE THE FIELD TYPE
			_fieldTypes.remove(iFieldName);
			if (_fieldTypes.size() == 0)
				// EMPTY: OPTIMIZE IT BY REMOVING THE ENTIRE MAP
				_fieldTypes = null;
		}
		return this;
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

	protected String checkFieldName(String iFieldName) {
		if (iFieldName == null)
			throw new IllegalArgumentException("Field name is null");

		iFieldName = iFieldName.trim();

		if (iFieldName.length() == 0)
			throw new IllegalArgumentException("Field name is empty");

		for (int i = 0; i < iFieldName.length(); ++i) {
			final char c = iFieldName.charAt(i);
			if (c == ':' || c == ',')
				throw new IllegalArgumentException("Invalid field name '" + iFieldName + "'");
		}

		// if (!Character.isJavaIdentifierStart(iFieldName.charAt(0)))
		// throw new IllegalArgumentException("Invalid property name");
		//
		// for (int i = 1; i < iFieldName.length(); ++i)
		// if (!Character.isJavaIdentifierPart(iFieldName.charAt(i)))
		// throw new IllegalArgumentException("Invalid property name");

		return iFieldName;
	}
}
