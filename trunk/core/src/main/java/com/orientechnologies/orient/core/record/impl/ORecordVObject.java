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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.vobject.ODatabaseVObject;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.ORecordVirtualAbstract;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;

/**
 * Record representation. The object can be reused across calls to the database by using the reset() at every re-use. Field
 * population and serialization occurs always in lazy way.
 */
@SuppressWarnings("unchecked")
public class ORecordVObject extends ORecordVirtualAbstract<Object> implements Iterable<Entry<String, Object>> {

	public static final byte	RECORD_TYPE	= 'v';

	public ORecordVObject() {
		setup();
	}

	public ORecordVObject(byte[] iSource) {
		super(iSource);
		setup();
	}

	public ORecordVObject(String iClassName) {
		setup();
		setClassName(iClassName);
	}

	public ORecordVObject(ODatabaseVObject iDatabase) {
		super(iDatabase);
		setup();
	}

	public ORecordVObject(ODatabaseVObject iDatabase, ORID iRID) {
		this(iDatabase);
		recordId = (ORecordId) iRID;
		status = STATUS.NOT_LOADED;
	}

	public ORecordVObject(ODatabaseVObject iDatabase, String iClassName, ORID iRID) {
		this(iDatabase, iClassName);
		recordId = (ORecordId) iRID;
		status = STATUS.NOT_LOADED;
	}

	public ORecordVObject(ODatabaseVObject iDatabase, String iClassName) {
		super(iDatabase, iClassName);
		setup();
	}

	public ORecordVObject(OClass iLinkedClass) {
		setup();
		clazz = iLinkedClass;
	}

	public ORecordVObject copy() {
		ORecordVObject cloned = new ORecordVObject();
		cloned.source = source;
		cloned.database = database;
		cloned.recordId = recordId.copy();
		cloned.version = version;
		cloned.dirty = dirty;
		cloned.pinned = pinned;
		cloned.clazz = clazz;
		cloned.status = status;
		cloned.recordFormat = recordFormat;

		cloned.fields = new LinkedHashMap<String, Object>(fields);

		return cloned;
	}

	@Override
	public String toString() {
		checkForFields();

		if (clazz == null)
			return "<unknown>";

		StringBuilder buffer = new StringBuilder();

		buffer.append(clazz.getName());

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
			buffer.append(f.getValue());

			if (first)
				first = false;
		}
		if (!first)
			buffer.append("}");

		return buffer.toString();
	}

	@Override
	public ORecordVObject fromStream(byte[] iRecordBuffer) {
		super.fromStream(iRecordBuffer);
		deserializeFields();
		return this;
	}

	public int size() {
		return fields.size();
	}

	protected void setup() {
		super.setup();
		recordFormat = ORecordSerializerFactory.instance().getFormat(ORecordSerializerSchemaAware2CSV.NAME);
	}

	public String[] fields() {
		checkForFields();

		String[] result = new String[fields.keySet().size()];
		return fields.keySet().toArray(result);
	}

	public <RET> RET field(final String iPropertyName) {
		checkForFields();

		int separatorPos = iPropertyName.indexOf('.');
		if (separatorPos > -1) {
			// GET THE LINKED OBJECT IF ANY
			String fieldName = iPropertyName.substring(0, separatorPos);
			Object linkedObject = fields.get(fieldName);

			if (linkedObject == null || !(linkedObject instanceof ORecordVObject))
				// IGNORE IT BY RETURNING NULL
				return null;

			ORecordVObject linkedRecord = (ORecordVObject) linkedObject;
			if (linkedRecord.getStatus() == STATUS.NOT_LOADED)
				linkedRecord.load();

			// CALL MYSELF RECURSIVELY BY CROSSING ALL THE OBJECTS
			return (RET) linkedRecord.field(iPropertyName.substring(separatorPos + 1));
		}

		RET value = (RET) fields.get(iPropertyName);

		if (value instanceof ORecord<?>) {
			// RELATION 1-1
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
		}

		return value;
	}

	/**
	 * Lazy load the record
	 * 
	 * @param <RET>
	 * @param record
	 */
	private <RET> void lazyLoadRecord(ORecord<?> record) {
		if (record.getStatus() == STATUS.NOT_LOADED)
			record.load();
	}

	public ORecordSchemaAware<Object> field(String iPropertyName, Object iPropertyValue) {
		checkForFields();
		if (clazz != null) {
			OProperty prop = clazz.getProperty(iPropertyName);

			if (prop != null) {
				if (!prop.getType().isAssignableFrom(iPropertyValue))
					throw new IllegalArgumentException("Property '" + iPropertyName + "' can't accept value of " + iPropertyValue.getClass());
			}
		}

		setDirty();
		fields.put(iPropertyName, iPropertyValue);

		return this;
	}

	public Iterator<Entry<String, Object>> iterator() {
		if (fields == null)
			return OEmptyIterator.INSTANCE;

		return fields.entrySet().iterator();
	}

	@Override
	public boolean containsField(String iFieldName) {
		return fields.containsKey(iFieldName);
	}

	public byte getRecordType() {
		return RECORD_TYPE;
	}
}
