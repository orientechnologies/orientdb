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

import java.text.ParseException;
import java.util.Collection;
import java.util.Date;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;

@SuppressWarnings("unchecked")
public abstract class ORecordSchemaAwareAbstract<T> extends ORecordAbstract<T> implements ORecordSchemaAware<T> {

	protected OClass	_clazz;
	protected int			_cursor;

	public ORecordSchemaAwareAbstract() {
	}

	public ORecordSchemaAwareAbstract(final ODatabaseRecord<?> iDatabase) {
		super(iDatabase);
	}

	public ORecordSchemaAwareAbstract<T> fill(final ODatabaseRecord<?> iDatabase, final int iClassId, final int iClusterId,
			final long iPosition, final int iVersion) {
		super.fill(iDatabase, iClusterId, iPosition, iVersion);
		setClass(_database.getMetadata().getSchema().getClassById(iClassId));
		return this;
	}

	public abstract boolean containsField(String name);

	@Override
	public ORecordAbstract<T> save() {
		OSerializationThreadLocal.INSTANCE.get().clear();
		return super.save();
	}

	@Override
	public ORecordAbstract<T> save(String iClusterName) {
		OSerializationThreadLocal.INSTANCE.get().clear();
		return super.save(iClusterName);
	}

	/**
	 * Validate the record following the declared constraints such as mandatory, notNull, min and max.
	 * 
	 * @see OProperty
	 */
	public void validate() throws OValidationException {
		if (_clazz != null)
			for (OProperty p : _clazz.properties()) {
				validateField(this, p);
			}
	}

	public OClass getSchemaClass() {
		return _clazz;
	}

	public String getClassName() {
		checkForFields();
		return _clazz != null ? _clazz.getName() : null;
	}

	public void setClassName(final String iClassName) {
		if (_database == null || iClassName == null) {
			_clazz = null;
			return;
		}

		setClass(_database.getMetadata().getSchema().getClass(iClassName));

		if (_clazz == null) {
			// CREATE THE CLASS AT THE FLY
			setClass(_database.getMetadata().getSchema().createClass(iClassName));
			_database.getMetadata().getSchema().save();
		}
	}

	public boolean hasNext() {
		checkForFields();
		return _cursor < size();
	}

	@Override
	public ORecordSchemaAwareAbstract<T> reset() {
		super.reset();
		_cursor = 0;
		return this;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	protected void checkForFields() {
		if (_status == STATUS.LOADED && size() == 0)
			// POPULATE FIELDS LAZY
			deserializeFields();
	}

	protected void deserializeFields() {
		if (_source == null)
			return;

		_status = STATUS.UNMARSHALLING;
		_recordFormat.fromStream(_database, _source, this);
		_status = STATUS.LOADED;
	}

	protected void setClass(final OClass iClass) {
		_clazz = iClass;
	}

	protected void checkFieldAccess(final int iIndex) {
		if (iIndex < 0 || iIndex >= size())
			throw new IndexOutOfBoundsException("Index " + iIndex + " is out of range allowed: 0-" + size());
	}

	public static void validateField(ORecordSchemaAwareAbstract<?> iRecord, OProperty p) throws OValidationException {
		Object fieldValue;
		if (p.isMandatory())
			if (!iRecord.containsField(p.getName()))
				throw new OValidationException("The field " + p.getName() + " is mandatory");

		fieldValue = iRecord.field(p.getName());

		if (p.isNotNull() && fieldValue == null)
			throw new OValidationException("The field " + p.getName() + " is null");

		OType type = p.getType();

		if (p.getMin() != null) {
			String min = p.getMin();

			if (p.getType().equals(OType.STRING) && (fieldValue != null && ((String) fieldValue).length() < Integer.parseInt(min)))
				throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName()
						+ " contains less characters than " + min + " requested");
			else if (p.getType().equals(OType.BINARY) && (fieldValue != null && ((byte[]) fieldValue).length < Integer.parseInt(min)))
				throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName() + " contains less bytes than "
						+ min + " requested");
			else if (p.getType().equals(OType.INTEGER) && (fieldValue != null && type.asInt(fieldValue) < Integer.parseInt(min)))
				throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName() + " is minor than " + min);
			else if (p.getType().equals(OType.LONG) && (fieldValue != null && type.asLong(fieldValue) < Long.parseLong(min)))
				throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName() + " is minor than " + min);
			else if (p.getType().equals(OType.FLOAT) && (fieldValue != null && type.asFloat(fieldValue) < Float.parseFloat(min)))
				throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName() + " is minor than " + min);
			else if (p.getType().equals(OType.DOUBLE) && (fieldValue != null && type.asDouble(fieldValue) < Double.parseDouble(min)))
				throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName() + " is minor than " + min);
			else if (p.getType().equals(OType.DATE)) {
				try {
					if (fieldValue != null
							&& ((Date) fieldValue).before(iRecord.getDatabase().getStorage().getConfiguration().getDateTimeFormatInstance()
									.parse(min)))
						throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName() + " contains the date "
								+ fieldValue + "that is before the date accepted (" + min + ")");
				} catch (ParseException e) {
				}
			} else if (p.getType().equals(OType.EMBEDDEDLIST) || p.getType().equals(OType.EMBEDDEDSET)
					|| p.getType().equals(OType.LINKLIST) || p.getType().equals(OType.LINKSET)
					&& (fieldValue != null && ((Collection<?>) fieldValue).size() < Integer.parseInt(min)))
				throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName() + " contains less items then "
						+ min + " requested");
		}

		if (p.getMax() != null) {
			String max = p.getMax();

			if (p.getType().equals(OType.STRING) && (fieldValue != null && ((String) fieldValue).length() > Integer.parseInt(max)))
				throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName()
						+ " contains more characters than " + max + " requested");
			else if (p.getType().equals(OType.BINARY) && (fieldValue != null && ((byte[]) fieldValue).length > Integer.parseInt(max)))
				throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName() + " contains more bytes than "
						+ max + " requested");
			else if (p.getType().equals(OType.INTEGER) && (fieldValue != null && type.asInt(fieldValue) > Integer.parseInt(max)))
				throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName() + " is major than " + max);
			else if (p.getType().equals(OType.LONG) && (fieldValue != null && type.asLong(fieldValue) > Long.parseLong(max)))
				throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName() + " is major than " + max);
			else if (p.getType().equals(OType.FLOAT) && (fieldValue != null && type.asFloat(fieldValue) > Float.parseFloat(max)))
				throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName() + " is major than " + max);
			else if (p.getType().equals(OType.DOUBLE) && (fieldValue != null && type.asDouble(fieldValue) > Double.parseDouble(max)))
				throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName() + " is major than " + max);
			else if (p.getType().equals(OType.DATE)) {
				try {
					if (fieldValue != null
							&& ((Date) fieldValue).before(iRecord.getDatabase().getStorage().getConfiguration().getDateTimeFormatInstance()
									.parse(max)))
						throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName() + " contains the date "
								+ fieldValue + "that is after the date accepted (" + max + ")");
				} catch (ParseException e) {
				}
			} else if (p.getType().equals(OType.EMBEDDEDLIST) || p.getType().equals(OType.EMBEDDEDSET)
					|| p.getType().equals(OType.LINKLIST) || p.getType().equals(OType.LINKSET)
					&& (fieldValue != null && ((Collection<?>) fieldValue).size() > Integer.parseInt(max)))
				throw new OValidationException("The field " + iRecord.getClassName() + "." + p.getName() + " contains more items then "
						+ max + " requested");
		}
	}

	protected void checkForLoading() {
		if (_status == STATUS.NOT_LOADED)
			load();
	}

}
