/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.serialization.serializer.record.string;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.fetch.json.OJSONFetchContext;
import com.orientechnologies.orient.core.fetch.json.OJSONFetchListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.ORecordStringable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;
import com.orientechnologies.orient.core.util.ODateHelper;
import com.orientechnologies.orient.core.version.ODistributedVersion;

import java.io.IOException;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("serial")
public class ORecordSerializerJSON extends ORecordSerializerStringAbstract {

  public static final String                NAME                  = "json";
  public static final ORecordSerializerJSON INSTANCE              = new ORecordSerializerJSON();
  public static final String                ATTRIBUTE_FIELD_TYPES = "@fieldTypes";
  public static final char[]                PARAMETER_SEPARATOR   = new char[] { ':', ',' };
  public static final int                   INITIAL_SIZE          = 5000;
  private static final Long                 MAX_INT               = (long) Integer.MAX_VALUE;
  private static final Long                 MIN_INT               = (long) Integer.MIN_VALUE;
  private static final Double               MAX_FLOAT             = (double) Float.MAX_VALUE;
  private static final Double               MIN_FLOAT             = (double) Float.MIN_VALUE;

  private interface CollectionItemVisitor {
    void visitItem(Object item);
  }

  public class FormatSettings {
    public boolean includeVer;
    public boolean includeType;
    public boolean includeId;
    public boolean includeClazz;
    public boolean attribSameRow;
    public boolean alwaysFetchEmbeddedDocuments;
    public int     indentLevel;
    public String  fetchPlan   = null;
    public boolean keepTypes   = true;
    public boolean dateAsLong  = false;
    public boolean prettyPrint = false;

    public FormatSettings(final String iFormat) {
      if (iFormat == null) {
        includeType = true;
        includeVer = true;
        includeId = true;
        includeClazz = true;
        attribSameRow = true;
        indentLevel = 1;
        fetchPlan = "";
        keepTypes = true;
        alwaysFetchEmbeddedDocuments = true;
      } else {
        includeType = false;
        includeVer = false;
        includeId = false;
        includeClazz = false;
        attribSameRow = false;
        alwaysFetchEmbeddedDocuments = false;
        indentLevel = 1;
        keepTypes = false;

        final String[] format = iFormat.split(",");
        for (String f : format)
          if (f.equals("type"))
            includeType = true;
          else if (f.equals("rid"))
            includeId = true;
          else if (f.equals("version"))
            includeVer = true;
          else if (f.equals("class"))
            includeClazz = true;
          else if (f.equals("attribSameRow"))
            attribSameRow = true;
          else if (f.startsWith("indent"))
            indentLevel = Integer.parseInt(f.substring(f.indexOf(':') + 1));
          else if (f.startsWith("fetchPlan"))
            fetchPlan = f.substring(f.indexOf(':') + 1);
          else if (f.startsWith("keepTypes"))
            keepTypes = true;
          else if (f.startsWith("alwaysFetchEmbedded"))
            alwaysFetchEmbeddedDocuments = true;
          else if (f.startsWith("dateAsLong"))
            dateAsLong = true;
          else if (f.startsWith("prettyPrint"))
            prettyPrint = true;
      }
    }
  }

  public ORecordInternal<?> fromString(String iSource, ORecordInternal<?> iRecord, final String[] iFields, boolean needReload) {
    return fromString(iSource, iRecord, iFields, null, needReload);
  }

  @Override
  public ORecordInternal<?> fromString(String iSource, ORecordInternal<?> iRecord, final String[] iFields) {
    return fromString(iSource, iRecord, iFields, null, false);
  }

  public ORecordInternal<?> fromString(String iSource, ORecordInternal<?> iRecord, final String[] iFields, final String iOptions,
      boolean needReload) {
    if (iSource == null)
      throw new OSerializationException("Error on unmarshalling JSON content: content is null");

    iSource = iSource.trim();
    if (!iSource.startsWith("{") || !iSource.endsWith("}"))
      throw new OSerializationException("Error on unmarshalling JSON content: content must be between { }");

    if (iRecord != null)
      // RESET ALL THE FIELDS
      iRecord.clear();

    iSource = iSource.substring(1, iSource.length() - 1).trim();

    // PARSE OPTIONS
    boolean noMap = false;
    if (iOptions != null) {
      final String[] format = iOptions.split(",");
      for (String f : format)
        if (f.equals("noMap"))
          noMap = true;
    }

    final List<String> fields = OStringSerializerHelper.smartSplit(iSource, PARAMETER_SEPARATOR, 0, -1, true, true, false, false,
        ' ', '\n', '\r', '\t');

    if (fields.size() % 2 != 0)
      throw new OSerializationException("Error on unmarshalling JSON content: wrong format. Use <field> : <value>");

    Map<String, Character> fieldTypes = null;

    if (fields != null && fields.size() > 0) {
      // SEARCH FOR FIELD TYPES IF ANY
      for (int i = 0; i < fields.size(); i += 2) {
        final String fieldName = OStringSerializerHelper.getStringContent(fields.get(i));
        final String fieldValue = fields.get(i + 1);
        final String fieldValueAsString = OStringSerializerHelper.getStringContent(fieldValue);

        if (fieldName.equals(ATTRIBUTE_FIELD_TYPES) && iRecord instanceof ODocument) {
          // LOAD THE FIELD TYPE MAP
          final String[] fieldTypesParts = fieldValueAsString.split(",");
          if (fieldTypesParts.length > 0) {
            fieldTypes = new HashMap<String, Character>();
            String[] part;
            for (String f : fieldTypesParts) {
              part = f.split("=");
              if (part.length == 2)
                fieldTypes.put(part[0], part[1].charAt(0));
            }
          }
        } else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_TYPE)) {
          if (iRecord == null || iRecord.getRecordType() != fieldValueAsString.charAt(0)) {
            // CREATE THE RIGHT RECORD INSTANCE
            iRecord = Orient.instance().getRecordFactoryManager().newInstance((byte) fieldValueAsString.charAt(0));
          }
        } else if (needReload && fieldName.equals(ODocumentHelper.ATTRIBUTE_RID) && iRecord instanceof ODocument) {
          if (fieldValue != null && fieldValue.length() > 0) {
            ORecordInternal<?> localRecord = ODatabaseRecordThreadLocal.INSTANCE.get().load(new ORecordId(fieldValueAsString));
            if (localRecord != null)
              iRecord = localRecord;
          }
        } else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_CLASS) && iRecord instanceof ODocument) {
          ((ODocument) iRecord).setClassNameIfExists("null".equals(fieldValueAsString) ? null : fieldValueAsString);
        }
      }

      if (iRecord == null)
        iRecord = new ODocument();

      try {
        int recordVersion = 0;
        long timestamp = 0L;
        long macAddress = 0L;
        for (int i = 0; i < fields.size(); i += 2) {
          final String fieldName = OStringSerializerHelper.getStringContent(fields.get(i));
          final String fieldValue = fields.get(i + 1);
          final String fieldValueAsString = OStringSerializerHelper.getStringContent(fieldValue);

          // RECORD ATTRIBUTES
          if (fieldName.equals(ODocumentHelper.ATTRIBUTE_RID))
            iRecord.setIdentity(new ORecordId(fieldValueAsString));

          else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_VERSION))
            if (OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.getValueAsBoolean())
              recordVersion = Integer.parseInt(fieldValue);
            else
              iRecord.getRecordVersion().setCounter(Integer.parseInt(fieldValue));
          else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_VERSION_TIMESTAMP)) {
            if (OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.getValueAsBoolean())
              timestamp = Long.parseLong(fieldValue);
          } else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_VERSION_MACADDRESS)) {
            if (OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.getValueAsBoolean())
              macAddress = Long.parseLong(fieldValue);

          } else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_TYPE)) {
            continue;
          } else if (fieldName.equals(ATTRIBUTE_FIELD_TYPES) && iRecord instanceof ODocument) {
            continue;
          } else if (fieldName.equals("value") && !(iRecord instanceof ODocument)) {
            // RECORD VALUE(S)
            if ("null".equals(fieldValue))
              iRecord.fromStream(new byte[] {});
            else if (iRecord instanceof ORecordBytes) {
              // BYTES
              iRecord.fromStream(OBase64Utils.decode(fieldValueAsString));
            } else if (iRecord instanceof ORecordStringable) {
              ((ORecordStringable) iRecord).value(fieldValueAsString);
            }
          } else {
            if (iRecord instanceof ODocument) {
              final ODocument doc = ((ODocument) iRecord);

              // DETERMINE THE TYPE FROM THE SCHEMA
              OType type = null;
              final OClass cls = doc.getSchemaClass();
              if (cls != null) {
                final OProperty prop = cls.getProperty(fieldName);
                if (prop != null)
                  type = prop.getType();
              }

              final Object v = getValue(doc, fieldName, fieldValue, fieldValueAsString, type, null, fieldTypes, noMap, iOptions);

              if (v != null)
                if (v instanceof Collection<?> && !((Collection<?>) v).isEmpty()) {
                  if (v instanceof ORecordLazyMultiValue)
                    ((ORecordLazyMultiValue) v).setAutoConvertToRecord(false);

                  // CHECK IF THE COLLECTION IS EMBEDDED
                  if (type == null) {
                    // TRY TO UNDERSTAND BY FIRST ITEM
                    Object first = ((Collection<?>) v).iterator().next();
                    if (first != null && first instanceof ORecord<?> && !((ORecord<?>) first).getIdentity().isValid())
                      type = v instanceof Set<?> ? OType.EMBEDDEDSET : OType.EMBEDDEDLIST;
                  }

                  if (type != null) {
                    // TREAT IT AS EMBEDDED
                    doc.field(fieldName, v, type);
                    continue;
                  }
                } else if (v instanceof Map<?, ?> && !((Map<?, ?>) v).isEmpty()) {
                  // CHECK IF THE MAP IS EMBEDDED
                  Object first = ((Map<?, ?>) v).values().iterator().next();
                  if (first != null && first instanceof ORecord<?> && !((ORecord<?>) first).getIdentity().isValid()) {
                    doc.field(fieldName, v, OType.EMBEDDEDMAP);
                    continue;
                  }
                } else if (v instanceof ODocument && type != null && type.isLink()) {
                  String className = ((ODocument) v).getClassName();
                  if (className != null && className.length() > 0)
                    ((ODocument) v).save();
                }

              if (type == null && fieldTypes != null && fieldTypes.containsKey(fieldName))
                type = ORecordSerializerStringAbstract.getType(fieldValue, fieldTypes.get(fieldName));

              if (type != null)
                doc.field(fieldName, v, type);
              else
                doc.field(fieldName, v);
            }
          }
        }
        if (timestamp != 0 && OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.getValueAsBoolean()) {
          ((ODistributedVersion) iRecord.getRecordVersion()).update(recordVersion, timestamp, macAddress);
        }

      } catch (Exception e) {
        e.printStackTrace();
        if (iRecord.getIdentity().isValid())
          throw new OSerializationException("Error on unmarshalling JSON content for record " + iRecord.getIdentity(), e);
        else
          throw new OSerializationException("Error on unmarshalling JSON content for record: " + iSource, e);
      }
    }
    return iRecord;
  }

  @Override
  public StringBuilder toString(final ORecordInternal<?> iRecord, final StringBuilder iOutput, final String iFormat,
      final OUserObject2RecordHandler iObjHandler, final Set<ODocument> iMarshalledRecords, boolean iOnlyDelta,
      boolean autoDetectCollectionType) {
    try {
      final StringWriter buffer = new StringWriter(INITIAL_SIZE);
      final OJSONWriter json = new OJSONWriter(buffer, iFormat);
      final FormatSettings settings = new FormatSettings(iFormat);

      json.beginObject();
      OJSONFetchContext context = new OJSONFetchContext(json, settings);
      context.writeSignature(json, iRecord);

      if (iRecord instanceof ORecordSchemaAware<?>) {

        OFetchHelper.fetch(iRecord, null, OFetchHelper.buildFetchPlan(settings.fetchPlan), new OJSONFetchListener(), context,
            iFormat);
      } else if (iRecord instanceof ORecordStringable) {

        // STRINGABLE
        final ORecordStringable record = (ORecordStringable) iRecord;
        json.writeAttribute(settings.indentLevel + 1, true, "value", record.value());

      } else if (iRecord instanceof ORecordBytes) {
        // BYTES
        final ORecordBytes record = (ORecordBytes) iRecord;
        json.writeAttribute(settings.indentLevel + 1, true, "value", OBase64Utils.encodeBytes(record.toStream()));
      } else

        throw new OSerializationException("Error on marshalling record of type '" + iRecord.getClass()
            + "' to JSON. The record type cannot be exported to JSON");

      json.endObject(0, true);

      iOutput.append(buffer);
      return iOutput;
    } catch (IOException e) {
      throw new OSerializationException("Error on marshalling of record to JSON", e);
    }
  }

  @Override
  public String toString() {
    return NAME;
  }

  @SuppressWarnings("unchecked")
  private Object getValue(final ODocument iRecord, String iFieldName, String iFieldValue, String iFieldValueAsString, OType iType,
      OType iLinkedType, final Map<String, Character> iFieldTypes, final boolean iNoMap, final String iOptions) {
    if (iFieldValue.equals("null"))
      return null;

    if (iFieldName != null && iRecord.getSchemaClass() != null) {
      final OProperty p = iRecord.getSchemaClass().getProperty(iFieldName);
      if (p != null) {
        iType = p.getType();
        iLinkedType = p.getLinkedType();
      }
    }

    if (iType == null && iFieldTypes != null && iFieldTypes.containsKey(iFieldName))
      iType = ORecordSerializerStringAbstract.getType(iFieldValue, iFieldTypes.get(iFieldName));

    if (iFieldValue.startsWith("{") && iFieldValue.endsWith("}")) {
      return getValueAsObjectOrMap(iRecord, iFieldValue, iType, iLinkedType, iFieldTypes, iNoMap, iOptions);
    } else if (iFieldValue.startsWith("[") && iFieldValue.endsWith("]")) {
      return getValueAsCollection(iRecord, iFieldValue, iType, iLinkedType, iFieldTypes, iNoMap, iOptions);
    }

    if (iType == null)
      // TRY TO DETERMINE THE CONTAINED TYPE from THE FIRST VALUE
      if (iFieldValue.charAt(0) != '\"' && iFieldValue.charAt(0) != '\'') {
        if (iFieldValue.equalsIgnoreCase("false") || iFieldValue.equalsIgnoreCase("true"))
          iType = OType.BOOLEAN;
        else {
          Character c = null;
          if (iFieldTypes != null) {
            c = iFieldTypes.get(iFieldName);
            if (c != null)
              iType = ORecordSerializerStringAbstract.getType(iFieldValue + c);
          }

          if (c == null && !iFieldValue.isEmpty()) {
            // TRY TO AUTODETERMINE THE BEST TYPE
            if (iFieldValue.charAt(0) == ORID.PREFIX && iFieldValue.contains(":"))
              iType = OType.LINK;
            else if (OStringSerializerHelper.contains(iFieldValue, '.')) {
              // DECIMAL FORMAT: DETERMINE IF DOUBLE OR FLOAT
              final Double v = new Double(OStringSerializerHelper.getStringContent(iFieldValue));

              if (canBeTrunkedToFloat(v))
                return v.floatValue();
              else
                return v;
            } else {
              final Long v = new Long(OStringSerializerHelper.getStringContent(iFieldValue));
              // INTEGER FORMAT: DETERMINE IF DOUBLE OR FLOAT

              if (canBeTrunkedToInt(v))
                return v.intValue();
              else
                return v;
            }
          }
        }
      } else if (iFieldValue.startsWith("{") && iFieldValue.endsWith("}"))
        iType = OType.EMBEDDED;
      else {
        if (iFieldValueAsString.length() >= 4 && iFieldValueAsString.charAt(0) == ORID.PREFIX && iFieldValueAsString.contains(":")) {
          // IS IT A LINK?
          final List<String> parts = OStringSerializerHelper.split(iFieldValueAsString, 1, -1, ':');
          if (parts.size() == 2)
            try {
              Short.parseShort(parts.get(0));
              // YES, IT'S A LINK
              if (parts.get(1).matches("\\d+")) {
                iType = OType.LINK;
              }
            } catch (Exception ignored) {
            }
        }

        if (iFieldTypes != null) {
          Character c = iFieldTypes.get(iFieldName);
          if (c != null)
            iType = ORecordSerializerStringAbstract.getType(iFieldValueAsString, c);
        }

        if (iType == null) {
          if (iFieldValueAsString.length() == ODateHelper.getDateFormat().length())
            // TRY TO PARSE AS DATE
            try {
              return ODateHelper.getDateFormatInstance().parseObject(iFieldValueAsString);
            } catch (Exception e) {
              // IGNORE IT
            }

          if (iFieldValueAsString.length() == ODateHelper.getDateTimeFormat().length())
            // TRY TO PARSE AS DATETIME
            try {
              return ODateHelper.getDateTimeFormatInstance().parseObject(iFieldValueAsString);
            } catch (Exception e) {
              // IGNORE IT
            }

          iType = OType.STRING;
        }
      }

    if (iType != null)
      switch (iType) {
      case STRING:
        return decodeJSON(iFieldValueAsString);

      case LINK:
        final int pos = iFieldValueAsString.indexOf('@');
        if (pos > -1)
          // CREATE DOCUMENT
          return new ODocument(iFieldValueAsString.substring(1, pos), new ORecordId(iFieldValueAsString.substring(pos + 1)));
        else {
          // CREATE SIMPLE RID
          return new ORecordId(iFieldValueAsString);
        }

      case EMBEDDED:
        return fromString(iFieldValueAsString);

      case DATE:
        if (iFieldValueAsString == null || iFieldValueAsString.equals(""))
          return null;
        try {
          // TRY TO PARSE AS LONG
          return Long.parseLong(iFieldValueAsString);
        } catch (NumberFormatException e) {
          try {
            // TRY TO PARSE AS DATE
            return ODateHelper.getDateFormatInstance().parseObject(iFieldValueAsString);
          } catch (ParseException ex) {
            throw new OSerializationException("Unable to unmarshall date (format=" + ODateHelper.getDateFormat() + ") : "
                + iFieldValueAsString, e);
          }
        }

      case DATETIME:
        if (iFieldValueAsString == null || iFieldValueAsString.equals(""))
          return null;
        try {
          // TRY TO PARSE AS LONG
          return Long.parseLong(iFieldValueAsString);
        } catch (NumberFormatException e) {
          try {
            // TRY TO PARSE AS DATETIME
            return ODateHelper.getDateTimeFormatInstance().parseObject(iFieldValueAsString);
          } catch (ParseException ex) {
            throw new OSerializationException("Unable to unmarshall datetime (format=" + ODateHelper.getDateTimeFormat() + ") : "
                + iFieldValueAsString, e);
          }
        }
      case BINARY:
        return OStringSerializerHelper.fieldTypeFromStream(iRecord, iType, iFieldValueAsString);
      default:
        return OStringSerializerHelper.fieldTypeFromStream(iRecord, iType, iFieldValue);
      }

    return iFieldValueAsString;
  }

  private boolean canBeTrunkedToInt(Long v) {
    return (v > 0) ? v.compareTo(MAX_INT) <= 0 : v.compareTo(MIN_INT) >= 0;
  }

  private boolean canBeTrunkedToFloat(Double v) {
    // TODO not really correct check. Small numbers with high precision will be trunked while they shouldn't be

    return (v > 0) ? v.compareTo(MAX_FLOAT) <= 0 : v.compareTo(MIN_FLOAT) >= 0;
  }

  /**
   * OBJECT OR MAP. CHECK THE TYPE ATTRIBUTE TO KNOW IT.
   */
  private Object getValueAsObjectOrMap(ODocument iRecord, String iFieldValue, OType iType, OType iLinkedType,
      Map<String, Character> iFieldTypes, boolean iNoMap, String iOptions) {
    final String[] fields = OStringParser.getWords(iFieldValue.substring(1, iFieldValue.length() - 1), ":,", true);

    if (fields == null || fields.length == 0)
      if (iNoMap)
        return new ODocument().addOwner(iRecord);
      else
        return new HashMap<String, Object>();

    if (iNoMap || hasTypeField(fields)) {
      return getValueAsRecord(iRecord, iFieldValue, iType, iOptions, fields);
    } else {
      return getValueAsMap(iRecord, iFieldValue, iLinkedType, iFieldTypes, false, iOptions, fields);
    }
  }

  private Object getValueAsMap(ODocument iRecord, String iFieldValue, OType iLinkedType, Map<String, Character> iFieldTypes,
      boolean iNoMap, String iOptions, String[] fields) {
    if (fields.length % 2 == 1)
      throw new OSerializationException("Bad JSON format on map. Expected pairs of field:value but received '" + iFieldValue + "'");

    final Map<String, Object> embeddedMap = new LinkedHashMap<String, Object>();

    for (int i = 0; i < fields.length; i += 2) {
      String iFieldName = fields[i];
      if (iFieldName.length() >= 2)
        iFieldName = iFieldName.substring(1, iFieldName.length() - 1);
      iFieldValue = fields[i + 1];
      final String valueAsString = OStringSerializerHelper.getStringContent(iFieldValue);

      embeddedMap.put(iFieldName,
          getValue(iRecord, null, iFieldValue, valueAsString, iLinkedType, null, iFieldTypes, iNoMap, iOptions));
    }
    return embeddedMap;
  }

  private Object getValueAsRecord(ODocument iRecord, String iFieldValue, OType iType, String iOptions, String[] fields) {
    boolean shouldReload = new ORecordId(OStringSerializerHelper.getStringContent(getFieldValue("@rid", fields))).isTemporary();
    final ORecordInternal<?> recordInternal = fromString(iFieldValue, new ODocument(), null, iOptions, shouldReload);
    if (shouldBeDeserializedAsEmbedded(recordInternal, iType))
      ((ODocument) recordInternal).addOwner(iRecord);
    return recordInternal;
  }

  private Object getValueAsCollection(ODocument iRecord, String iFieldValue, OType iType, OType iLinkedType,
      Map<String, Character> iFieldTypes, boolean iNoMap, String iOptions) {
    // remove square brackets
    iFieldValue = iFieldValue.substring(1, iFieldValue.length() - 1);

    if (iType == OType.LINKBAG) {
      final ORidBag bag = new ORidBag();

      parseCollection(iRecord, iFieldValue, iType, OType.LINK, iFieldTypes, iNoMap, iOptions, new CollectionItemVisitor() {
        @Override
        public void visitItem(Object item) {
          bag.add((OIdentifiable) item);
        }
      });

      return bag;
    } else if (iType == OType.LINKSET) {
      return getValueAsLinkedCollection(new OMVRBTreeRIDSet(iRecord), iRecord, iFieldValue, iType, iLinkedType, iFieldTypes,
          iNoMap, iOptions);
    } else if (iType == OType.LINKLIST) {
      return getValueAsLinkedCollection(new ORecordLazyList(iRecord), iRecord, iFieldValue, iType, iLinkedType, iFieldTypes,
          iNoMap, iOptions);
    } else if (iType == OType.EMBEDDEDSET) {
      return getValueAsEmbeddedCollection(new OTrackedSet<Object>(iRecord), iRecord, iFieldValue, iType, iLinkedType, iFieldTypes,
          iNoMap, iOptions);
    } else {
      return getValueAsEmbeddedCollection(new OTrackedList<Object>(iRecord), iRecord, iFieldValue, iType, iLinkedType, iFieldTypes,
          iNoMap, iOptions);
    }
  }

  private Object getValueAsLinkedCollection(final Collection<OIdentifiable> collection, ODocument iRecord, String iFieldValue,
      OType iType, OType iLinkedType, Map<String, Character> iFieldTypes, boolean iNoMap, String iOptions) {

    parseCollection(iRecord, iFieldValue, iType, iLinkedType, iFieldTypes, iNoMap, iOptions, new CollectionItemVisitor() {
      @Override
      public void visitItem(Object item) {
        collection.add((OIdentifiable) item);
      }
    });

    return collection;
  }

  private Object getValueAsEmbeddedCollection(final Collection<Object> collection, ODocument iRecord, String iFieldValue,
      OType iType, OType iLinkedType, Map<String, Character> iFieldTypes, boolean iNoMap, String iOptions) {

    parseCollection(iRecord, iFieldValue, iType, iLinkedType, iFieldTypes, iNoMap, iOptions, new CollectionItemVisitor() {
      @Override
      public void visitItem(Object item) {
        collection.add(item);
      }
    });

    return collection;
  }

  private void parseCollection(ODocument iRecord, String iFieldValue, OType iType, OType iLinkedType,
      Map<String, Character> iFieldTypes, boolean iNoMap, String iOptions, CollectionItemVisitor visitor) {
    if (!iFieldValue.isEmpty()) {
      for (String item : OStringSerializerHelper.smartSplit(iFieldValue, ',')) {
        final String itemValue = item.trim();

        final Object collectionItem = getValue(iRecord, null, itemValue, OStringSerializerHelper.getStringContent(itemValue),
            iLinkedType, null, iFieldTypes, iNoMap, iOptions);

        // TODO redundant in some cases, owner is already added by getValue in some cases
        if (shouldBeDeserializedAsEmbedded(collectionItem, iType))
          ((ODocument) collectionItem).addOwner(iRecord);

        if (collectionItem instanceof String && ((String) collectionItem).length() == 0)
          continue;

        visitor.visitItem(collectionItem);
      }
    }
  }

  private boolean shouldBeDeserializedAsEmbedded(Object record, OType iType) {
    return record instanceof ODocument && !((ODocument) record).getIdentity().isTemporary()
        && !((ODocument) record).getIdentity().isPersistent() && (iType == null || !iType.isLink());
  }

  private String decodeJSON(String iFieldValueAsString) {
    iFieldValueAsString = OStringParser.replaceAll(iFieldValueAsString, "\\\\", "\\");
    iFieldValueAsString = OStringParser.replaceAll(iFieldValueAsString, "\\\"", "\"");
    iFieldValueAsString = OStringParser.replaceAll(iFieldValueAsString, "\\/", "/");
    return iFieldValueAsString;
  }

  private boolean hasTypeField(final String[] fields) {
    return hasField("@type", fields);
  }

  /**
   * Checks if given collection of fields contain field with specified name.
   * 
   * @param field
   *          to find
   * @param fields
   *          collection of fields where search
   * @return true if collection contain specified field, false otherwise.
   */
  private boolean hasField(final String field, final String[] fields) {
    return getFieldValue(field, fields) != null;
  }

  private String getFieldValue(final String field, final String[] fields) {
    String doubleQuotes = "\"" + field + "\"";
    String singleQuotes = "'" + field + "'";
    for (int i = 0; i < fields.length; i = i + 2) {
      if (fields[i].equals(doubleQuotes) || fields[i].equals(singleQuotes)) {
        return fields[i + 1];
      }
    }
    return null;
  }
}
