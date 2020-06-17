/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.serialization.serializer.record.string;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ORecordSerializerSchemaAware2CSV extends ORecordSerializerCSVAbstract {
  public static final String NAME = "ORecordDocument2csv";
  public static final ORecordSerializerSchemaAware2CSV INSTANCE =
      new ORecordSerializerSchemaAware2CSV();
  private static final long serialVersionUID = 1L;

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public int getCurrentVersion() {
    return 0;
  }

  @Override
  public int getMinSupportedVersion() {
    return 0;
  }

  public String getClassName(String content) {
    content = content.trim();

    if (content.length() == 0) return null;

    final int posFirstValue = content.indexOf(OStringSerializerHelper.ENTRY_SEPARATOR);
    final int pos = content.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);

    if (pos > -1 && (pos < posFirstValue || posFirstValue == -1)) return content.substring(0, pos);

    return null;
  }

  @Override
  public ORecord fromString(String iContent, final ORecord iRecord, final String[] iFields) {
    iContent = iContent.trim();

    if (iContent.length() == 0) return iRecord;

    // UNMARSHALL THE CLASS NAME
    final ODocument record = (ODocument) iRecord;

    int pos;
    final ODatabaseDocumentInternal database = ODatabaseRecordThreadLocal.instance().getIfDefined();
    final int posFirstValue = iContent.indexOf(OStringSerializerHelper.ENTRY_SEPARATOR);
    pos = iContent.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
    if (pos > -1 && (pos < posFirstValue || posFirstValue == -1)) {
      if ((record.getIdentity().getClusterId() < 0 || database == null))
        ODocumentInternal.fillClassNameIfNeeded(((ODocument) iRecord), iContent.substring(0, pos));
      iContent = iContent.substring(pos + 1);
    } else record.setClassNameIfExists(null);

    if (iFields != null && iFields.length == 1 && iFields[0].equals("@class"))
      // ONLY THE CLASS NAME HAS BEEN REQUESTED: RETURN NOW WITHOUT UNMARSHALL THE ENTIRE RECORD
      return iRecord;

    final List<String> fields =
        OStringSerializerHelper.smartSplit(
            iContent, OStringSerializerHelper.RECORD_SEPARATOR, true, true);

    String fieldName = null;
    String fieldValue;
    OType type;
    OClass linkedClass;
    OType linkedType;
    OProperty prop;

    final Set<String> fieldSet;

    if (iFields != null && iFields.length > 0) {
      fieldSet = new HashSet<String>(iFields.length);
      for (String f : iFields) fieldSet.add(f);
    } else fieldSet = null;

    // UNMARSHALL ALL THE FIELDS
    for (String fieldEntry : fields) {
      fieldEntry = fieldEntry.trim();
      boolean uncertainType = false;

      try {
        pos = fieldEntry.indexOf(FIELD_VALUE_SEPARATOR);
        if (pos > -1) {
          // GET THE FIELD NAME
          fieldName = fieldEntry.substring(0, pos);

          // CHECK IF THE FIELD IS REQUESTED TO BEING UNMARSHALLED
          if (fieldSet != null && !fieldSet.contains(fieldName)) continue;

          if (record.containsField(fieldName))
            // ALREADY UNMARSHALLED: DON'T OVERWRITE IT
            continue;

          // GET THE FIELD VALUE
          fieldValue = fieldEntry.length() > pos + 1 ? fieldEntry.substring(pos + 1) : null;

          boolean setFieldType = false;

          // SEARCH FOR A CONFIGURED PROPERTY
          if (ODocumentInternal.getImmutableSchemaClass(record) != null) {
            prop = ODocumentInternal.getImmutableSchemaClass(record).getProperty(fieldName);
          } else {
            prop = null;
          }
          if (prop != null && prop.getType() != OType.ANY) {
            // RECOGNIZED PROPERTY
            type = prop.getType();
            linkedClass = prop.getLinkedClass();
            linkedType = prop.getLinkedType();

          } else {
            // SCHEMA PROPERTY NOT FOUND FOR THIS FIELD: TRY TO AUTODETERMINE THE BEST TYPE
            type = record.fieldType(fieldName);
            if (type == OType.ANY) type = null;
            if (type != null) setFieldType = true;
            linkedClass = null;
            linkedType = null;

            // NOT FOUND: TRY TO DETERMINE THE TYPE FROM ITS CONTENT
            if (fieldValue != null && type == null) {
              if (fieldValue.length() > 1
                  && fieldValue.charAt(0) == '"'
                  && fieldValue.charAt(fieldValue.length() - 1) == '"') {
                type = OType.STRING;
              } else if (fieldValue.startsWith(OStringSerializerHelper.LINKSET_PREFIX)) {
                type = OType.LINKSET;
              } else if (fieldValue.charAt(0) == OStringSerializerHelper.LIST_BEGIN
                      && fieldValue.charAt(fieldValue.length() - 1)
                          == OStringSerializerHelper.LIST_END
                  || fieldValue.charAt(0) == OStringSerializerHelper.SET_BEGIN
                      && fieldValue.charAt(fieldValue.length() - 1)
                          == OStringSerializerHelper.SET_END) {
                // EMBEDDED LIST/SET
                type =
                    fieldValue.charAt(0) == OStringSerializerHelper.LIST_BEGIN
                        ? OType.EMBEDDEDLIST
                        : OType.EMBEDDEDSET;

                final String value = fieldValue.substring(1, fieldValue.length() - 1);

                if (!value.isEmpty()) {
                  if (value.charAt(0) == OStringSerializerHelper.LINK) {
                    // TODO replace with regex
                    // ASSURE ALL THE ITEMS ARE RID
                    int max = value.length();
                    boolean allLinks = true;
                    boolean checkRid = true;
                    for (int i = 0; i < max; ++i) {
                      char c = value.charAt(i);
                      if (checkRid) {
                        if (c != '#') {
                          allLinks = false;
                          break;
                        }
                        checkRid = false;
                      } else if (c == ',') checkRid = true;
                    }

                    if (allLinks) {
                      type =
                          fieldValue.charAt(0) == OStringSerializerHelper.LIST_BEGIN
                              ? OType.LINKLIST
                              : OType.LINKSET;
                      linkedType = OType.LINK;
                    }
                  } else if (value.charAt(0) == OStringSerializerHelper.EMBEDDED_BEGIN) {
                    linkedType = OType.EMBEDDED;
                  } else if (value.charAt(0) == OStringSerializerHelper.CUSTOM_TYPE) {
                    linkedType = OType.CUSTOM;
                  } else if (Character.isDigit(value.charAt(0))
                      || value.charAt(0) == '+'
                      || value.charAt(0) == '-') {
                    String[] items = value.split(",");
                    linkedType = getType(items[0]);
                  } else if (value.charAt(0) == '\'' || value.charAt(0) == '"')
                    linkedType = OType.STRING;
                } else uncertainType = true;

              } else if (fieldValue.charAt(0) == OStringSerializerHelper.MAP_BEGIN
                  && fieldValue.charAt(fieldValue.length() - 1)
                      == OStringSerializerHelper.MAP_END) {
                type = OType.EMBEDDEDMAP;
              } else if (fieldValue.charAt(0) == OStringSerializerHelper.LINK) type = OType.LINK;
              else if (fieldValue.charAt(0) == OStringSerializerHelper.EMBEDDED_BEGIN) {
                // TEMPORARY PATCH
                if (fieldValue.startsWith("(ORIDs")) type = OType.LINKSET;
                else type = OType.EMBEDDED;
              } else if (fieldValue.charAt(0) == OStringSerializerHelper.BAG_BEGIN) {
                type = OType.LINKBAG;
              } else if (fieldValue.equals("true") || fieldValue.equals("false"))
                type = OType.BOOLEAN;
              else type = getType(fieldValue);
            }
          }
          final Object value =
              fieldFromStream(iRecord, type, linkedClass, linkedType, fieldName, fieldValue);
          if ("@class".equals(fieldName)) {
            ODocumentInternal.fillClassNameIfNeeded(((ODocument) iRecord), value.toString());
          } else {
            record.field(fieldName, value, type);
          }

          if (uncertainType) record.setFieldType(fieldName, null);
        }
      } catch (Exception e) {
        throw OException.wrapException(
            new OSerializationException(
                "Error on unmarshalling field '"
                    + fieldName
                    + "' in record "
                    + iRecord.getIdentity()
                    + " with value: "
                    + fieldEntry),
            e);
      }
    }

    return iRecord;
  }

  @Override
  public byte[] toStream(ORecord iRecord) {
    final byte[] result = super.toStream(iRecord);
    if (result == null || result.length > 0) return result;

    // Fix of nasty IBM JDK bug. In case of very depth recursive graph serialization
    // ODocument#_source property may be initialized incorrectly.
    final ODocument recordSchemaAware = (ODocument) iRecord;
    if (recordSchemaAware.fields() > 0) return null;

    return result;
  }

  public byte[] writeClassOnly(ORecord iSource) {
    final ODocument record = (ODocument) iSource;
    StringBuilder iOutput = new StringBuilder();
    if (ODocumentInternal.getImmutableSchemaClass(record) != null) {
      iOutput.append(ODocumentInternal.getImmutableSchemaClass(record).getStreamableName());
      iOutput.append(OStringSerializerHelper.CLASS_SEPARATOR);
    }
    try {
      return iOutput.toString().getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSerializationException("error writing class name"), e);
    }
  }

  @Override
  protected StringBuilder toString(
      ORecord iRecord,
      final StringBuilder iOutput,
      final String iFormat,
      final boolean autoDetectCollectionType) {
    if (iRecord == null) throw new OSerializationException("Expected a record but was null");

    if (!(iRecord instanceof ODocument))
      throw new OSerializationException(
          "Cannot marshall a record of type " + iRecord.getClass().getSimpleName());

    final ODocument record = (ODocument) iRecord;

    if (ODocumentInternal.getImmutableSchemaClass(record) != null) {
      iOutput.append(ODocumentInternal.getImmutableSchemaClass(record).getStreamableName());
      iOutput.append(OStringSerializerHelper.CLASS_SEPARATOR);
    }

    OProperty prop;
    OType type;
    OClass linkedClass;
    OType linkedType;
    String fieldClassName;
    int i = 0;

    final String[] fieldNames = record.fieldNames();

    // MARSHALL ALL THE FIELDS OR DELTA IF TRACKING IS ENABLED
    for (String fieldName : fieldNames) {
      Object fieldValue = record.rawField(fieldName);
      if (i > 0) iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

      // SEARCH FOR A CONFIGURED PROPERTY
      if (ODocumentInternal.getImmutableSchemaClass(record) != null) {
        prop = ODocumentInternal.getImmutableSchemaClass(record).getProperty(fieldName);
      } else {
        prop = null;
      }
      fieldClassName = getClassName(fieldValue);

      type = record.fieldType(fieldName);
      if (type == OType.ANY) type = null;

      linkedClass = null;
      linkedType = null;

      if (prop != null && prop.getType() != OType.ANY) {
        // RECOGNIZED PROPERTY
        type = prop.getType();
        linkedClass = prop.getLinkedClass();
        linkedType = prop.getLinkedType();

      } else if (fieldValue != null) {
        // NOT FOUND: TRY TO DETERMINE THE TYPE FROM ITS CONTENT
        if (type == null) {
          if (fieldValue.getClass() == byte[].class) type = OType.BINARY;
          else if (ODatabaseRecordThreadLocal.instance().isDefined()
              && fieldValue instanceof ORecord) {
            if (type == null)
              // DETERMINE THE FIELD TYPE
              if (fieldValue instanceof ODocument && ((ODocument) fieldValue).hasOwners())
                type = OType.EMBEDDED;
              else type = OType.LINK;

            linkedClass = getLinkInfo(ODatabaseRecordThreadLocal.instance().get(), fieldClassName);
          } else if (fieldValue instanceof ORID)
            // DETERMINE THE FIELD TYPE
            type = OType.LINK;
          else if (fieldValue instanceof Date) type = OType.DATETIME;
          else if (fieldValue instanceof String) type = OType.STRING;
          else if (fieldValue instanceof Integer || fieldValue instanceof BigInteger)
            type = OType.INTEGER;
          else if (fieldValue instanceof Long) type = OType.LONG;
          else if (fieldValue instanceof Float) type = OType.FLOAT;
          else if (fieldValue instanceof Short) type = OType.SHORT;
          else if (fieldValue instanceof Byte) type = OType.BYTE;
          else if (fieldValue instanceof Double) type = OType.DOUBLE;
          else if (fieldValue instanceof BigDecimal) type = OType.DECIMAL;
          else if (fieldValue instanceof ORidBag) type = OType.LINKBAG;

          if (fieldValue instanceof OMultiCollectionIterator<?>) {
            type =
                ((OMultiCollectionIterator<?>) fieldValue).isEmbedded()
                    ? OType.EMBEDDEDLIST
                    : OType.LINKLIST;
            linkedType =
                ((OMultiCollectionIterator<?>) fieldValue).isEmbedded()
                    ? OType.EMBEDDED
                    : OType.LINK;
          } else if (fieldValue instanceof Collection<?> || fieldValue.getClass().isArray()) {
            final int size = OMultiValue.getSize(fieldValue);

            Boolean autoConvertLinks = null;
            if (fieldValue instanceof ORecordLazyMultiValue) {
              autoConvertLinks = ((ORecordLazyMultiValue) fieldValue).isAutoConvertToRecord();
              if (autoConvertLinks)
                // DISABLE AUTO CONVERT
                ((ORecordLazyMultiValue) fieldValue).setAutoConvertToRecord(false);
            }

            if (autoDetectCollectionType)
              if (size > 0) {
                final Object firstValue = OMultiValue.getFirstValue(fieldValue);

                if (firstValue != null) {
                  if (firstValue instanceof ORID) {
                    linkedClass = null;
                    linkedType = OType.LINK;
                    if (fieldValue instanceof Set<?>) type = OType.LINKSET;
                    else type = OType.LINKLIST;
                  } else if (ODatabaseRecordThreadLocal.instance().isDefined()
                      && (firstValue instanceof ODocument && !((ODocument) firstValue).isEmbedded())
                      && (firstValue instanceof ORecord)) {
                    linkedClass =
                        getLinkInfo(
                            ODatabaseRecordThreadLocal.instance().get(), getClassName(firstValue));
                    if (type == null) {
                      // LINK: GET THE CLASS
                      linkedType = OType.LINK;

                      if (fieldValue instanceof Set<?>) type = OType.LINKSET;
                      else type = OType.LINKLIST;
                    } else linkedType = OType.EMBEDDED;
                  } else {
                    // EMBEDDED COLLECTION
                    if (firstValue instanceof ODocument
                        && ((((ODocument) firstValue).hasOwners())
                            || type == OType.EMBEDDEDSET
                            || type == OType.EMBEDDEDLIST
                            || type == OType.EMBEDDEDMAP)) linkedType = OType.EMBEDDED;
                    else if (firstValue instanceof Enum<?>) linkedType = OType.STRING;
                    else {
                      linkedType = OType.getTypeByClass(firstValue.getClass());

                      if (linkedType != OType.LINK)
                        // EMBEDDED FOR SURE DON'T USE THE LINKED TYPE
                        linkedType = null;
                    }

                    if (type == null)
                      if (fieldValue instanceof ORecordLazySet) type = OType.LINKSET;
                      else if (fieldValue instanceof Set<?>) type = OType.EMBEDDEDSET;
                      else type = OType.EMBEDDEDLIST;
                  }
                }
              } else if (type == null) type = OType.EMBEDDEDLIST;

            if (fieldValue instanceof ORecordLazyMultiValue && autoConvertLinks) {
              // REPLACE PREVIOUS SETTINGS
              ((ORecordLazyMultiValue) fieldValue).setAutoConvertToRecord(true);
            }

          } else if (fieldValue instanceof Map<?, ?> && type == null) {
            final int size = OMultiValue.getSize(fieldValue);

            Boolean autoConvertLinks = null;
            if (fieldValue instanceof ORecordLazyMap) {
              autoConvertLinks = ((ORecordLazyMap) fieldValue).isAutoConvertToRecord();
              if (autoConvertLinks)
                // DISABLE AUTO CONVERT
                ((ORecordLazyMap) fieldValue).setAutoConvertToRecord(false);
            }

            if (size > 0) {
              final Object firstValue = OMultiValue.getFirstValue(fieldValue);

              if (firstValue != null) {
                if (ODatabaseRecordThreadLocal.instance().isDefined()
                    && (firstValue instanceof ODocument && !((ODocument) firstValue).isEmbedded())
                    && (firstValue instanceof ORecord)) {
                  linkedClass =
                      getLinkInfo(
                          ODatabaseRecordThreadLocal.instance().get(), getClassName(firstValue));
                  // LINK: GET THE CLASS
                  linkedType = OType.LINK;
                  type = OType.LINKMAP;
                }
              }
            }

            if (type == null) type = OType.EMBEDDEDMAP;

            if (fieldValue instanceof ORecordLazyMap && autoConvertLinks)
              // REPLACE PREVIOUS SETTINGS
              ((ORecordLazyMap) fieldValue).setAutoConvertToRecord(true);
          }
        }
      }

      if (type == OType.TRANSIENT)
        // TRANSIENT FIELD
        continue;

      if (type == null) type = OType.EMBEDDED;

      iOutput.append(fieldName);
      iOutput.append(FIELD_VALUE_SEPARATOR);
      fieldToStream(record, iOutput, type, linkedClass, linkedType, fieldName, fieldValue, true);

      i++;
    }

    // GET THE OVERSIZE IF ANY
    final float overSize;
    if (ODocumentInternal.getImmutableSchemaClass(record) != null)
      // GET THE CONFIGURED OVERSIZE SETTED PER CLASS
      overSize = ODocumentInternal.getImmutableSchemaClass(record).getOverSize();
    else overSize = 0;

    // APPEND BLANKS IF NEEDED
    final int newSize;
    if (record.hasOwners())
      // EMBEDDED: GET REAL SIZE
      newSize = iOutput.length();
    else if (record.getSize() == iOutput.length())
      // IDENTICAL! DO NOTHING
      newSize = record.getSize();
    else if (record.getSize() > iOutput.length()
        && !OGlobalConfiguration.RECORD_DOWNSIZING_ENABLED.getValueAsBoolean()) {
      // APPEND EXTRA SPACES TO FILL ALL THE AVAILABLE SPACE AND AVOID FRAGMENTATION
      newSize = record.getSize();
    } else if (overSize > 0) {
      // APPEND EXTRA SPACES TO GET A LARGER iOutput
      newSize = (int) (iOutput.length() * overSize);
    } else
      // NO OVERSIZE
      newSize = iOutput.length();

    if (newSize > iOutput.length()) {
      iOutput.ensureCapacity(newSize);
      for (int b = iOutput.length(); b < newSize; ++b) iOutput.append(' ');
    }

    return iOutput;
  }

  private String getClassName(final Object iValue) {
    if (iValue instanceof ODocument) return ((ODocument) iValue).getClassName();

    return iValue != null ? iValue.getClass().getSimpleName() : null;
  }

  private OClass getLinkInfo(final ODatabaseInternal<?> iDatabase, final String iFieldClassName) {
    if (iDatabase == null || iDatabase.isClosed() || iFieldClassName == null) return null;

    OClass linkedClass =
        ((OMetadataInternal) iDatabase.getMetadata())
            .getImmutableSchemaSnapshot()
            .getClass(iFieldClassName);

    return linkedClass;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
