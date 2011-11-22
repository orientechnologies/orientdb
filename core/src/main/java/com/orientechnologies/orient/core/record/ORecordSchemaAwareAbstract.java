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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;

@SuppressWarnings({ "unchecked", "serial" })
public abstract class ORecordSchemaAwareAbstract<T> extends ORecordAbstract<T> implements ORecordSchemaAware<T> {

	protected OClass	_clazz;

	public ORecordSchemaAwareAbstract() {
	}

	public ORecordSchemaAwareAbstract(final ODatabaseRecord iDatabase) {
		super(iDatabase);
	}

	public ORecordSchemaAwareAbstract<T> fill(final ODatabaseRecord iDatabase, final int iClassId, final ORecordId iRid,
			final int iVersion, final byte[] iBuffer, boolean iDirty) {
		fill(iDatabase, iRid, iVersion, iBuffer, iDirty);
		setClass(null);
		return this;
	}

	@Override
	public ORecordAbstract<T> save() {
		if (_clazz != null)
			return save(getDatabase().getClusterNameById(_clazz.getDefaultClusterId()));

		return super.save();
	}

	@Override
	public ORecordAbstract<T> save(final String iClusterName) {
		OSerializationThreadLocal.INSTANCE.get().clear();
		try {
			validate();
			return super.save(iClusterName);
		} finally {
			OSerializationThreadLocal.INSTANCE.get().clear();
		}
	}

	/**
	 * Validates the record following the declared constraints defined in schema such as mandatory, notNull, min, max, regexp, etc. If
	 * the schema is not defined for the current class or there are not constraints then the validation is ignored.
	 * 
	 * @see OProperty
	 * @throws OValidationException
	 *           if the document breaks some validation constraints defined in the schema
	 */
	public void validate() throws OValidationException {
		if (_database != null && !_database.isValidationEnabled())
			return;

		checkForLoading();
		checkForFields();

		if (_clazz != null)
			for (OProperty p : _clazz.properties()) {
				validateField(this, p);
			}
	}

	public OClass getSchemaClass() {
		if (_clazz == null)
			// DESERIALIZE ONLY IF THE CLASS IS NOT SETTED: THIS PREVENT TO
			// UNMARSHALL THE RECORD EVEN IF SETTED BY fromString()
			checkForFields();
		return _clazz;
	}

	public String getClassName() {
		checkForLoading();
		checkForFields();
		return _clazz != null ? _clazz.getName() : null;
	}

	public void setClassName(final String iClassName) {
		if (_database == null || iClassName == null) {
			_clazz = null;
			return;
		}

		setClass(_database.getMetadata().getSchema().getOrCreateClass(iClassName));
	}

	public void setClassNameIfExists(final String iClassName) {
		if (_database == null || iClassName == null) {
			_clazz = null;
			return;
		}

		setClass(_database.getMetadata().getSchema().getClass(iClassName));
	}

	@Override
	public ORecordSchemaAwareAbstract<T> reset() {
		super.reset();
		_clazz = null;
		return this;
	}

	public byte[] toStream() {
		return toStream(false);
	}

	public byte[] toStream(final boolean iOnlyDelta) {
		if (_source == null)
			_source = _recordFormat.toStream(_database, this, iOnlyDelta);

		invokeListenerEvent(ORecordListener.EVENT.MARSHALL);

		return _source;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	protected void checkForFields() {
		if (_status == ORecordElement.STATUS.LOADED && fields() == 0)
			// POPULATE FIELDS LAZY
			deserializeFields();
	}

	public void deserializeFields() {
		if (_source == null)
			return;

		_status = ORecordElement.STATUS.UNMARSHALLING;
		_recordFormat.fromStream(_database, _source, this);
		_status = ORecordElement.STATUS.LOADED;
	}

	protected void setClass(final OClass iClass) {
		_clazz = iClass;
	}

	protected void checkFieldAccess(final int iIndex) {
		if (iIndex < 0 || iIndex >= fields())
			throw new IndexOutOfBoundsException("Index " + iIndex + " is out of range allowed: 0-" + fields());
	}

	public static void validateField(ORecordSchemaAwareAbstract<?> iRecord, OProperty p) throws OValidationException {
		Object fieldValue;
		if (p.isMandatory())
			if (!iRecord.containsField(p.getName()))
				throw new OValidationException("The field '" + p.getName() + "' is mandatory");

		if (iRecord instanceof ODocument)
			// AVOID CONVERSIONS: FASTER!
			fieldValue = ((ODocument) iRecord).rawField(p.getName());
		else
			fieldValue = iRecord.field(p.getName());

		if (p.isNotNull() && fieldValue == null)
			// NULLITY
			throw new OValidationException("The field '" + p.getName() + "' cannot be null");

		if (fieldValue != null && p.getRegexp() != null) {
			// REGEXP
			if (!fieldValue.toString().matches(p.getRegexp()))
				throw new OValidationException("The field '" + p.getName() + "' doesn't match the regular expression '" + p.getRegexp()
						+ "'. Field value is: " + fieldValue);
		}

		final OType type = p.getType();

		if (fieldValue != null && type != null) {
			// CHECK TYPE
			switch (type) {
			case LINK:
				if (!(fieldValue instanceof OIdentifiable))
					throw new OValidationException("The field '" + p.getName()
							+ "' has been declared as LINK but the value is not a record or a record-id");

				final ORecord<?> linkedRecord = ((OIdentifiable) fieldValue).getRecord();

				if (linkedRecord != null && p.getLinkedClass() != null) {
					if (!(linkedRecord instanceof ODocument))
						throw new OValidationException("The field '" + p.getName() + "' has been declared as LINK of type '"
								+ p.getLinkedClass() + "' but the value is the record " + linkedRecord.getIdentity() + " that is not a document");

					// AT THIS POINT CHECK THE CLASS ONLY IF != NULL BECAUSE IN CASE OF GRAPHS THE RECORD COULD BE PARTIAL
					if (((ODocument) linkedRecord).getSchemaClass() != null
							&& !p.getLinkedClass().isSuperClassOf(((ODocument) linkedRecord).getSchemaClass()))
						throw new OValidationException("The field '" + p.getName() + "' has been declared as LINK of type '"
								+ p.getLinkedClass().getName() + "' but the value is the document " + linkedRecord.getIdentity() + " of class '"
								+ ((ODocument) linkedRecord).getSchemaClass() + "'");

				}
			}
		}

		if (p.getMin() != null) {
			// MIN
			final String min = p.getMin();

			if (p.getType().equals(OType.STRING) && (fieldValue != null && ((String) fieldValue).length() < Integer.parseInt(min)))
				throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName()
						+ "' contains less characters than " + min + " requested");
			else if (p.getType().equals(OType.BINARY) && (fieldValue != null && ((byte[]) fieldValue).length < Integer.parseInt(min)))
				throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' contains less bytes than "
						+ min + " requested");
			else if (p.getType().equals(OType.INTEGER) && (fieldValue != null && type.asInt(fieldValue) < Integer.parseInt(min)))
				throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' is minor than " + min);
			else if (p.getType().equals(OType.LONG) && (fieldValue != null && type.asLong(fieldValue) < Long.parseLong(min)))
				throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' is minor than " + min);
			else if (p.getType().equals(OType.FLOAT) && (fieldValue != null && type.asFloat(fieldValue) < Float.parseFloat(min)))
				throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' is minor than " + min);
			else if (p.getType().equals(OType.DOUBLE) && (fieldValue != null && type.asDouble(fieldValue) < Double.parseDouble(min)))
				throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' is minor than " + min);
			else if (p.getType().equals(OType.DATE)) {
				try {
					if (fieldValue != null
							&& ((Date) fieldValue).before(iRecord.getDatabase().getStorage().getConfiguration().getDateFormatInstance()
									.parse(min)))
						throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' contains the date "
								+ fieldValue + "that is before the date accepted (" + min + ")");
				} catch (ParseException e) {
				}
			} else if (p.getType().equals(OType.DATETIME)) {
				try {
					if (fieldValue != null
							&& ((Date) fieldValue).before(iRecord.getDatabase().getStorage().getConfiguration().getDateTimeFormatInstance()
									.parse(min)))
						throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' contains the datetime "
								+ fieldValue + "that is before the datetime accepted (" + min + ")");
				} catch (ParseException e) {
				}
			} else if ((p.getType().equals(OType.EMBEDDEDLIST) || p.getType().equals(OType.EMBEDDEDSET)
					|| p.getType().equals(OType.LINKLIST) || p.getType().equals(OType.LINKSET))
					&& (fieldValue != null && ((Collection<?>) fieldValue).size() < Integer.parseInt(min)))
				throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' contains less items then "
						+ min + " requested");
		}

		if (p.getMax() != null) {
			// MAX
			final String max = p.getMax();

			if (p.getType().equals(OType.STRING) && (fieldValue != null && ((String) fieldValue).length() > Integer.parseInt(max)))
				throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName()
						+ "' contains more characters than " + max + " requested");
			else if (p.getType().equals(OType.BINARY) && (fieldValue != null && ((byte[]) fieldValue).length > Integer.parseInt(max)))
				throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' contains more bytes than "
						+ max + " requested");
			else if (p.getType().equals(OType.INTEGER) && (fieldValue != null && type.asInt(fieldValue) > Integer.parseInt(max)))
				throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' is major than " + max);
			else if (p.getType().equals(OType.LONG) && (fieldValue != null && type.asLong(fieldValue) > Long.parseLong(max)))
				throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' is major than " + max);
			else if (p.getType().equals(OType.FLOAT) && (fieldValue != null && type.asFloat(fieldValue) > Float.parseFloat(max)))
				throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' is major than " + max);
			else if (p.getType().equals(OType.DOUBLE) && (fieldValue != null && type.asDouble(fieldValue) > Double.parseDouble(max)))
				throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' is major than " + max);
			else if (p.getType().equals(OType.DATE)) {
				try {
					if (fieldValue != null
							&& ((Date) fieldValue).before(iRecord.getDatabase().getStorage().getConfiguration().getDateFormatInstance()
									.parse(max)))
						throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' contains the date "
								+ fieldValue + "that is after the date accepted (" + max + ")");
				} catch (ParseException e) {
				}
			} else if (p.getType().equals(OType.DATETIME)) {
				try {
					if (fieldValue != null
							&& ((Date) fieldValue).before(iRecord.getDatabase().getStorage().getConfiguration().getDateTimeFormatInstance()
									.parse(max)))
						throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' contains the datetime "
								+ fieldValue + "that is after the datetime accepted (" + max + ")");
				} catch (ParseException e) {
				}
			} else if ((p.getType().equals(OType.EMBEDDEDLIST) || p.getType().equals(OType.EMBEDDEDSET)
					|| p.getType().equals(OType.LINKLIST) || p.getType().equals(OType.LINKSET))
					&& (fieldValue != null && ((Collection<?>) fieldValue).size() > Integer.parseInt(max)))
				throw new OValidationException("The field '" + iRecord.getClassName() + "." + p.getName() + "' contains more items then "
						+ max + " requested");
		}
	}

	protected void checkForLoading() {
		if (_status == ORecordElement.STATUS.NOT_LOADED && _database != null)
			reload(null, true);
	}
}
