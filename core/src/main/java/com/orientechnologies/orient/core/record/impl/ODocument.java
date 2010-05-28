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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
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
	 * Creates a new instance of the specified class. New instances are not persistent until {@link #save()} is called.
	 * 
	 * @param iClassName
	 *          Class name
	 */
	public ODocument(final String iClassName) {
		setup();
		setClassName(iClassName);
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
		recordId = (ORecordId) iRID;
		status = STATUS.NOT_LOADED;
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
		recordId = (ORecordId) iRID;
		dirty = false;
		status = STATUS.NOT_LOADED;
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
		super(iClass.getDatabase());
		setup();
		clazz = iClass;
	}

	/**
	 * Copies the current instance to a new one.
	 */
	public ODocument copy() {
		ODocument cloned = new ODocument();
		cloned.source = source;
		cloned.database = database;
		cloned.recordId = recordId.copy();
		cloned.version = version;
		cloned.dirty = dirty;
		cloned.pinned = pinned;
		cloned.clazz = clazz;
		cloned.status = status;
		cloned.recordFormat = recordFormat;

		if (fields != null)
			cloned.fields = new LinkedHashMap<String, Object>(fields);

		return cloned;
	}

	/**
	 * Dumps the instance as string.
	 */
	@Override
	public String toString() {
		checkForFields();

		StringBuilder buffer = new StringBuilder();

		buffer.append(clazz == null ? "<unknown>" : clazz.getName());

		if (recordId != null) {
			buffer.append("@");
			if (recordId != null && recordId.isValid())
				buffer.append(recordId);
		}

		boolean first = true;
		for (Entry<String, Object> f : fields.entrySet()) {
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
		return fields == null ? 0 : fields.size();
	}

	/**
	 * Returns the array of field names.
	 */
	public String[] fieldNames() {
		checkForFields();

		String[] result = new String[fields.keySet().size()];
		return fields.keySet().toArray(result);
	}

	/**
	 * Returns the array of field values.
	 */
	public Object[] fieldValues() {
		checkForFields();

		Object[] result = new Object[fields.values().size()];
		return fields.values().toArray(result);
	}

	/**
	 * Reads the field value.
	 * 
	 * @param iPropertyName
	 *          field name
	 * @return field value if defined, otherwise null
	 */
	public <RET> RET field(final String iPropertyName) {
		checkForFields();

		int separatorPos = iPropertyName.indexOf('.');
		if (separatorPos > -1) {
			// GET THE LINKED OBJECT IF ANY
			String fieldName = iPropertyName.substring(0, separatorPos);
			Object linkedObject = fields.get(fieldName);

			if (linkedObject == null || !(linkedObject instanceof ODocument))
				// IGNORE IT BY RETURNING NULL
				return null;

			ODocument linkedRecord = (ODocument) linkedObject;
			if (linkedRecord.getStatus() == STATUS.NOT_LOADED)
				linkedRecord.load();

			// CALL MYSELF RECURSIVELY BY CROSSING ALL THE OBJECTS
			return (RET) linkedRecord.field(iPropertyName.substring(separatorPos + 1));
		}

		RET value = (RET) fields.get(iPropertyName);

		if (value instanceof ORecord<?>) {
			// RELATION X->1
			lazyLoadRecord((ORecord<?>) value);

		} else if (value instanceof Collection<?>) {
			// RELATION 1-N
			Collection<?> coll = (Collection<?>) value;
			if (coll.size() > 0) {
				Object o;
				for (Iterator<?> it = coll.iterator(); it.hasNext();) {
					o = it.next();
					if (o instanceof ORecord<?>)
						lazyLoadRecord((ORecord<?>) o);
				}
			}
		} else if (value instanceof Map<?, ?>) {
			// RELATION 1-N
			Map<String, ?> map = (Map<String, ?>) value;
			if (map.size() > 0) {
				Object o;
				for (Iterator<?> it = map.values().iterator(); it.hasNext();) {
					o = it.next();
					if (o instanceof ORecord<?>)
						lazyLoadRecord((ORecord<?>) o);
				}
			}
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
		checkForFields();
		if (clazz != null) {
			OProperty prop = clazz.getProperty(iPropertyName);

			if (prop != null) {
				if (!(iPropertyValue instanceof String) && !prop.getType().isAssignableFrom(iPropertyValue))
					throw new IllegalArgumentException("Property '" + iPropertyName + "' can't accept value of " + iPropertyValue.getClass());
			}
		}

		setDirty();

		Object oldValue = fields.get(iPropertyName);

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
						newValue.add(new ODocument(database, new ORecordId(s)));
					}
				}
			}
		}

		fields.put(iPropertyName, iPropertyValue);

		if (iType != null) {
			// SAVE FORCED TYPE
			if (fieldTypes == null)
				fieldTypes = new HashMap<String, OType>();
			fieldTypes.put(iPropertyName, iType);
		}

		return this;
	}

	/**
	 * Remove a field.
	 */
	public ORecordSchemaAware<Object> removeField(final String iPropertyName) {
		fields.remove(iPropertyName);
		return this;
	}

	/**
	 * Returns the iterator against the field entries as name and value.
	 */
	public Iterator<Entry<String, Object>> iterator() {
		if (fields == null)
			return OEmptyIterator.INSTANCE;

		return fields.entrySet().iterator();
	}

	/**
	 * Checks if a field exists.
	 * 
	 * @return True if exists, otherwise false.
	 */
	@Override
	public boolean containsField(final String iFieldName) {
		return fields != null ? fields.containsKey(iFieldName) : false;
	}

	/**
	 * Internal.
	 */
	public byte getRecordType() {
		return RECORD_TYPE;
	}

	/**
	 * Internal.
	 */
	@Override
	protected void setup() {
		super.setup();
		recordFormat = ORecordSerializerFactory.instance().getFormat(ORecordSerializerSchemaAware2CSV.NAME);
	}

	/**
	 * Lazy loads the record.
	 * 
	 * @param <RET>
	 * @param record
	 */
	private <RET> void lazyLoadRecord(final ORecord<?> record) {
		if (record.getStatus() == STATUS.NOT_LOADED)
			try {
				record.load();
			} catch (ORecordNotFoundException e) {
				// IGNORE IT CAUSE CAN BE RAISED BY NETWORK ISSUES
			}
	}
}
