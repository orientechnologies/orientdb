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
package com.orientechnologies.orient.core.record;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.index.OPropertyIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Abstract implementation for record-free implementations. The object can be reused across calls to the database by using the
 * reset() at every re-use. Field population and serialization occurs always in lazy way.
 */
@SuppressWarnings({ "unchecked", "serial" })
public abstract class ORecordVirtualAbstract<T> extends ORecordSchemaAwareAbstract<T> {
	protected Map<String, T>			_fieldValues;
	protected Map<String, T>			_fieldOriginalValues;
	protected Map<String, OType>	_fieldTypes;
	protected boolean							_trackingChanges	= true;
	protected boolean							_ordered					= true;
	protected boolean							_lazyLoad					= true;

	public ORecordVirtualAbstract() {
	}

	public ORecordVirtualAbstract(byte[] iSource) {
		_source = iSource;
	}

	public ORecordVirtualAbstract(String iClassName) {
		setClassName(iClassName);
	}

	public ORecordVirtualAbstract(ODatabaseRecord iDatabase) {
		super(iDatabase);
	}

	public ORecordVirtualAbstract(ODatabaseRecord iDatabase, String iClassName) {
		super(iDatabase);
		setClassName(iClassName);
	}

	@Override
	public ORecordAbstract<T> fromStream(byte[] iRecordBuffer) {
		_fieldValues = null;
		_fieldTypes = null;
		_fieldOriginalValues = null;
		return super.fromStream(iRecordBuffer);
	}

	@Override
	public void unsetDirty() {
		_fieldOriginalValues = null;
		super.unsetDirty();
	}

	/**
	 * Returns the forced field type if any.
	 * 
	 * @param iPropertyName
	 */
	public OType fieldType(final String iPropertyName) {
		return _fieldTypes != null ? _fieldTypes.get(iPropertyName) : null;
	}

	@Override
	public ORecordAbstract<T> unload() {
		super.unload();
		if (_fieldValues != null)
			_fieldValues.clear();
		return this;
	}

	/**
	 * Clear all the field values and types.
	 */
	@Override
	public ORecordSchemaAwareAbstract<T> clear() {
		super.clear();
		if (_fieldValues != null)
			_fieldValues.clear();
		return this;
	}

	/**
	 * Reset the record values and class type to being reused.
	 */
	@Override
	public ORecordSchemaAwareAbstract<T> reset() {
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
	public <RET extends ORecordVirtualAbstract<?>> RET setTrackingChanges(final boolean iTrackingChanges) {
		this._trackingChanges = iTrackingChanges;
		if (!iTrackingChanges)
			// FREE RESOURCES
			this._fieldOriginalValues = null;
		return (RET) this;
	}

	public boolean isOrdered() {
		return _ordered;
	}

	public <RET extends ORecordVirtualAbstract<?>> RET setOrdered(final boolean iOrdered) {
		this._ordered = iOrdered;
		return (RET) this;
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

	@Override
	protected void checkForFields() {
		if (_fieldValues == null)
			_fieldValues = _ordered ? new LinkedHashMap<String, T>() : new HashMap<String, T>();

		if (_status == ORecordElement.STATUS.LOADED && fields() == 0)
			// POPULATE FIELDS LAZY
			deserializeFields();
	}
}
