/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

@SuppressWarnings({ "unchecked", "serial" })
public abstract class ORecordSchemaAwareAbstract<T> extends ORecordAbstract<T> implements ORecordSchemaAware<T> {

  protected OClass _clazz;

  public ORecordSchemaAwareAbstract() {
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
    if (ODatabaseRecordThreadLocal.INSTANCE.isDefined() && !getDatabase().isValidationEnabled())
      return;

    checkForLoading();
    checkForFields();

    if (_clazz != null) {
      if (_clazz.isStrictMode()) {
        // CHECK IF ALL FIELDS ARE DEFINED
        for (String f : fieldNames()) {
          if (_clazz.getProperty(f) == null)
            throw new OValidationException("Found additional field '" + f + "'. It cannot be added because the schema class '"
                + _clazz.getName() + "' is defined as STRICT");
        }
      }

      for (OProperty p : _clazz.properties()) {
        validateField(this, p);
      }
    }
  }

  public OClass getSchemaClass() {
    if (_clazz == null) {
      // DESERIALIZE ONLY IF THE CLASS IS NOT SETTED: THIS PREVENT TO
      // UNMARSHALL THE RECORD EVEN IF SETTED BY fromString()
      checkForLoading();
      checkForFields("@class");
    }
    return _clazz;
  }

  public String getClassName() {
    if (_clazz != null)
      return _clazz.getName();

    // CLASS NOT FOUND: CHECK IF NEED LOADING AND UNMARSHALLING
    checkForLoading();
    checkForFields("@class");
    return _clazz != null ? _clazz.getName() : null;
  }

  public void setClassName(final String iClassName) {
    if (iClassName == null) {
      _clazz = null;
      return;
    }

    setClass(getDatabase().getMetadata().getSchema().getOrCreateClass(iClassName));
  }

  public void setClassNameIfExists(final String iClassName) {
    if (iClassName == null) {
      _clazz = null;
      return;
    }

    setClass(getDatabase().getMetadata().getSchema().getClass(iClassName));
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
      _source = _recordFormat.toStream(this, iOnlyDelta);

    invokeListenerEvent(ORecordListener.EVENT.MARSHALL);

    return _source;
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  protected boolean checkForFields(final String... iFields) {
    if (_status == ORecordElement.STATUS.LOADED && fields() == 0)
      // POPULATE FIELDS LAZY
      return deserializeFields(iFields);
    return true;
  }

  public boolean deserializeFields(final String... iFields) {
    if (_source == null)
      return false;

    _status = ORecordElement.STATUS.UNMARSHALLING;
    _recordFormat.fromStream(_source, this, iFields);
    _status = ORecordElement.STATUS.LOADED;

    return true;
  }

  protected void setClass(final OClass iClass) {
    _clazz = iClass;
  }

  protected void checkFieldAccess(final int iIndex) {
    if (iIndex < 0 || iIndex >= fields())
      throw new IndexOutOfBoundsException("Index " + iIndex + " is outside the range allowed: 0-" + fields());
  }

  public static void validateField(ORecordSchemaAwareAbstract<?> iRecord, OProperty p) throws OValidationException {
    final Object fieldValue;

    if (iRecord.containsField(p.getName())) {
      if (iRecord instanceof ODocument)
        // AVOID CONVERSIONS: FASTER!
        fieldValue = ((ODocument) iRecord).rawField(p.getName());
      else
        fieldValue = iRecord.field(p.getName());

      if (p.isNotNull() && fieldValue == null)
        // NULLITY
        throw new OValidationException("The field '" + p.getFullName() + "' cannot be null");

      if (fieldValue != null && p.getRegexp() != null) {
        // REGEXP
        if (!fieldValue.toString().matches(p.getRegexp()))
          throw new OValidationException("The field '" + p.getFullName() + "' does not match the regular expression '"
              + p.getRegexp() + "'. Field value is: " + fieldValue);
      }

    } else {
      if (p.isMandatory())
        throw new OValidationException("The field '" + p.getFullName() + "' is mandatory");
      fieldValue = null;
    }

    final OType type = p.getType();

    if (fieldValue != null && type != null) {
      // CHECK TYPE
      switch (type) {
      case LINK:
        final ORecord<?> linkedRecord;

        if (fieldValue instanceof OIdentifiable)
          linkedRecord = ((OIdentifiable) fieldValue).getRecord();
        else if (fieldValue instanceof String)
          linkedRecord = new ORecordId((String) fieldValue).getRecord();
        else
          throw new OValidationException("The field '" + p.getFullName()
              + "' has been declared as LINK but the value is not a record or a record-id");

        if (linkedRecord != null && p.getLinkedClass() != null) {
          if (!(linkedRecord instanceof ODocument))
            throw new OValidationException("The field '" + p.getFullName() + "' has been declared as LINK of type '"
                + p.getLinkedClass() + "' but the value is the record " + linkedRecord.getIdentity() + " that is not a document");

          // AT THIS POINT CHECK THE CLASS ONLY IF != NULL BECAUSE IN CASE OF GRAPHS THE RECORD COULD BE PARTIAL
          if (((ODocument) linkedRecord).getSchemaClass() != null
              && !p.getLinkedClass().isSuperClassOf(((ODocument) linkedRecord).getSchemaClass()))
            throw new OValidationException("The field '" + p.getFullName() + "' has been declared as LINK of type '"
                + p.getLinkedClass().getName() + "' but the value is the document " + linkedRecord.getIdentity() + " of class '"
                + ((ODocument) linkedRecord).getSchemaClass() + "'");
        }
      }
    }

    if (p.getMin() != null) {
      // MIN
      final String min = p.getMin();

      if (p.getType().equals(OType.STRING) && (fieldValue != null && ((String) fieldValue).length() < Integer.parseInt(min)))
        throw new OValidationException("The field '" + p.getFullName() + "' contains fewer characters than " + min + " requested");
      else if (p.getType().equals(OType.BINARY) && (fieldValue != null && ((byte[]) fieldValue).length < Integer.parseInt(min)))
        throw new OValidationException("The field '" + p.getFullName() + "' contains fewer bytes than " + min + " requested");
      else if (p.getType().equals(OType.INTEGER) && (fieldValue != null && type.asInt(fieldValue) < Integer.parseInt(min)))
        throw new OValidationException("The field '" + p.getFullName() + "' is less than " + min);
      else if (p.getType().equals(OType.LONG) && (fieldValue != null && type.asLong(fieldValue) < Long.parseLong(min)))
        throw new OValidationException("The field '" + p.getFullName() + "' is less than " + min);
      else if (p.getType().equals(OType.FLOAT) && (fieldValue != null && type.asFloat(fieldValue) < Float.parseFloat(min)))
        throw new OValidationException("The field '" + p.getFullName() + "' is less than " + min);
      else if (p.getType().equals(OType.DOUBLE) && (fieldValue != null && type.asDouble(fieldValue) < Double.parseDouble(min)))
        throw new OValidationException("The field '" + p.getFullName() + "' is less than " + min);
      else if (p.getType().equals(OType.DATE)) {
        try {
          if (fieldValue != null
              && ((Date) fieldValue).before(iRecord.getDatabase().getStorage().getConfiguration().getDateFormatInstance()
                  .parse(min)))
            throw new OValidationException("The field '" + p.getFullName() + "' contains the date " + fieldValue
                + " which precedes the first acceptable date (" + min + ")");
        } catch (ParseException e) {
        }
      } else if (p.getType().equals(OType.DATETIME)) {
        try {
          if (fieldValue != null
              && ((Date) fieldValue).before(iRecord.getDatabase().getStorage().getConfiguration().getDateTimeFormatInstance()
                  .parse(min)))
            throw new OValidationException("The field '" + p.getFullName() + "' contains the datetime " + fieldValue
                + " which precedes the first acceptable datetime (" + min + ")");
        } catch (ParseException e) {
        }
      } else if ((p.getType().equals(OType.EMBEDDEDLIST) || p.getType().equals(OType.EMBEDDEDSET)
          || p.getType().equals(OType.LINKLIST) || p.getType().equals(OType.LINKSET))
          && (fieldValue != null && ((Collection<?>) fieldValue).size() < Integer.parseInt(min)))
        throw new OValidationException("The field '" + p.getFullName() + "' contains fewer items than " + min + " requested");
    }

    if (p.getMax() != null) {
      // MAX
      final String max = p.getMax();

      if (p.getType().equals(OType.STRING) && (fieldValue != null && ((String) fieldValue).length() > Integer.parseInt(max)))
        throw new OValidationException("The field '" + p.getFullName() + "' contains more characters than " + max + " requested");
      else if (p.getType().equals(OType.BINARY) && (fieldValue != null && ((byte[]) fieldValue).length > Integer.parseInt(max)))
        throw new OValidationException("The field '" + p.getFullName() + "' contains more bytes than " + max + " requested");
      else if (p.getType().equals(OType.INTEGER) && (fieldValue != null && type.asInt(fieldValue) > Integer.parseInt(max)))
        throw new OValidationException("The field '" + p.getFullName() + "' is greater than " + max);
      else if (p.getType().equals(OType.LONG) && (fieldValue != null && type.asLong(fieldValue) > Long.parseLong(max)))
        throw new OValidationException("The field '" + p.getFullName() + "' is greater than " + max);
      else if (p.getType().equals(OType.FLOAT) && (fieldValue != null && type.asFloat(fieldValue) > Float.parseFloat(max)))
        throw new OValidationException("The field '" + p.getFullName() + "' is greater than " + max);
      else if (p.getType().equals(OType.DOUBLE) && (fieldValue != null && type.asDouble(fieldValue) > Double.parseDouble(max)))
        throw new OValidationException("The field '" + p.getFullName() + "' is greater than " + max);
      else if (p.getType().equals(OType.DATE)) {
        try {
          if (fieldValue != null
              && ((Date) fieldValue).before(iRecord.getDatabase().getStorage().getConfiguration().getDateFormatInstance()
                  .parse(max)))
            throw new OValidationException("The field '" + p.getFullName() + "' contains the date " + fieldValue
                + " which is after the last acceptable date (" + max + ")");
        } catch (ParseException e) {
        }
      } else if (p.getType().equals(OType.DATETIME)) {
        try {
          if (fieldValue != null
              && ((Date) fieldValue).before(iRecord.getDatabase().getStorage().getConfiguration().getDateTimeFormatInstance()
                  .parse(max)))
            throw new OValidationException("The field '" + p.getFullName() + "' contains the datetime " + fieldValue
                + " which is after the last acceptable datetime (" + max + ")");
        } catch (ParseException e) {
        }
      } else if ((p.getType().equals(OType.EMBEDDEDLIST) || p.getType().equals(OType.EMBEDDEDSET)
          || p.getType().equals(OType.LINKLIST) || p.getType().equals(OType.LINKSET))
          && (fieldValue != null && ((Collection<?>) fieldValue).size() > Integer.parseInt(max)))
        throw new OValidationException("The field '" + p.getFullName() + "' contains more items than " + max + " requested");
    }
  }
}
