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

import com.fasterxml.jackson.core.*;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.fetch.OFetchPlan;
import com.orientechnologies.orient.core.fetch.json.OJSONFetchContext;
import com.orientechnologies.orient.core.fetch.json.OJSONFetchListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordStringable;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEmbedded;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.io.*;
import java.text.ParseException;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("serial")
public class ORecordSerializerJSON extends ORecordSerializerStringAbstract {
  public static final String NAME = "json";
  public static final ORecordSerializerJSON INSTANCE = new ORecordSerializerJSON();
  public static final String ATTRIBUTE_FIELD_TYPES = "@fieldTypes";
  public static final char[] PARAMETER_SEPARATOR = new char[] {':', ','};
  public static final int INITIAL_SIZE = 5000;
  private static final Long MAX_INT = (long) Integer.MAX_VALUE;
  private static final Long MIN_INT = (long) Integer.MIN_VALUE;
  private static final Double MAX_FLOAT = (double) Float.MAX_VALUE;
  private static final Double MIN_FLOAT = (double) Float.MIN_VALUE;

  private static final JsonFactory factory = new JsonFactory();

  static {
    factory.enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);
    factory.enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES);
    factory.enable(JsonParser.Feature.ALLOW_TRAILING_COMMA);

    // factory.enable(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER);
    factory.enable(JsonParser.Feature.ALLOW_COMMENTS);
    // factory.enable(JsonParser.Feature.ALLOW_MISSING_VALUES);
  }

  private interface CollectionItemVisitor {
    void visitItem(Object item);
  }

  public static class FormatSettings {
    public boolean includeVer;
    public boolean includeType;
    public boolean includeId;
    public boolean includeClazz;
    public boolean attribSameRow;
    public boolean alwaysFetchEmbeddedDocuments;
    public int indentLevel;
    public String fetchPlan = null;
    public boolean keepTypes;
    public boolean dateAsLong = false;
    public boolean prettyPrint = false;
    // '@fieldTypes' become part of the signature
    public boolean earlyTypes;

    public FormatSettings(final String iFormat) {
      if (iFormat == null) {
        includeType = true;
        includeVer = true;
        includeId = true;
        includeClazz = true;
        attribSameRow = true;
        indentLevel = 0;
        fetchPlan = "";
        keepTypes = true;
        alwaysFetchEmbeddedDocuments = true;
        earlyTypes = true;
      } else {
        includeType = false;
        includeVer = false;
        includeId = false;
        includeClazz = false;
        attribSameRow = false;
        alwaysFetchEmbeddedDocuments = false;
        indentLevel = 0;
        keepTypes = false;
        earlyTypes = false;

        if (iFormat != null && !iFormat.isEmpty()) {
          final String[] format = iFormat.split(",");
          for (String f : format)
            if (f.equals("type")) includeType = true;
            else if (f.equals("rid")) includeId = true;
            else if (f.equals("version")) includeVer = true;
            else if (f.equals("class")) includeClazz = true;
            else if (f.equals("attribSameRow")) attribSameRow = true;
            else if (f.startsWith("indent"))
              indentLevel = Integer.parseInt(f.substring(f.indexOf(':') + 1));
            else if (f.startsWith("fetchPlan")) fetchPlan = f.substring(f.indexOf(':') + 1);
            else if (f.startsWith("keepTypes")) keepTypes = true;
            else if (f.startsWith("alwaysFetchEmbedded")) alwaysFetchEmbeddedDocuments = true;
            else if (f.startsWith("dateAsLong")) dateAsLong = true;
            else if (f.startsWith("prettyPrint")) prettyPrint = true;
            else if (f.startsWith("graph") || f.startsWith("shallow"))
              // SUPPORTED IN OTHER PARTS
              ;
            else if (f.startsWith("earlyTypes")) earlyTypes = true;
            else throw new IllegalArgumentException("Unrecognized JSON formatting option: " + f);
        }
      }
    }
  }

  @Override
  public int getCurrentVersion() {
    return 0;
  }

  @Override
  public int getMinSupportedVersion() {
    return 0;
  }

  public ORecord fromString(
      String source, ORecord record, final String[] fields, boolean needReload) {
    return fromString(source, record, fields, null, needReload);
  }

  @Override
  public ORecord fromString(String source, ORecord record, final String[] fields) {
    return fromString(source, record, fields, null, false);
  }

  @Override
  public ORecord fromStream(final InputStream source, ORecord record, final String[] fields) {
    return fromStream(source, record, fields, null, false);
  }

  public ORecord fromString(
      String source,
      ORecord record,
      final String[] fields,
      final String options,
      boolean needReload) {
    return fromString(source, record, fields, options, needReload, -1, new HashSet<>());
  }

  public ORecord fromStream(
      final InputStream source,
      ORecord record,
      final String[] fields,
      final String options,
      boolean needReload) {
    return fromStream(source, record, fields, options, needReload, -1, new HashSet<>());
  }

  public ORecord fromStream(
      final InputStream source,
      ORecord record,
      final String iOptions,
      boolean needReload,
      int maxRidbagSizeBeforeSkip,
      Set<Integer> skippedPartsIndexes)
      throws JsonParseException {
    String className = null;
    boolean noMap = false;
    if (iOptions != null) {
      final String[] format = iOptions.split(",");
      for (String f : format) if (f.equalsIgnoreCase("noMap")) noMap = true;
    }
    if (record != null) {
      // RESET ALL THE FIELDS
      record.clear();
    }
    if (record == null) {
      record = new ODocument();
    }

    try {
      final JsonParser parser = factory.createParser(source);

      JsonToken jsonToken = null;
      final Map<String, Character> fieldTypes = new HashMap<>();
      while ((jsonToken = parser.nextToken()) != null) {
        if (jsonToken.equals(JsonToken.START_OBJECT)) {
          while (jsonToken != JsonToken.END_OBJECT) {
            parser.nextToken();
            jsonToken = processRecord(parser, record, fieldTypes, iOptions, noMap, null);
          }
        }
      }
      if (className != null) {
        // Trigger the default value
        ((ODocument) record).setClassName(className);
      }
    } catch (final JsonParseException e) {
      // compatibility mode for invalid JSON
      OLogManager.instance().warn(this, "Falling back to legacy JSON parser due to invalid JSON.");
      throw e;
    } catch (final Exception e) {
      if (record.getIdentity().isValid()) {
        throw OException.wrapException(
            new OSerializationException(
                "Error on unmarshalling JSON content for record " + record.getIdentity()),
            e);
      } else {
        throw OException.wrapException(
            new OSerializationException(
                "Error on unmarshalling JSON content for record: " + source),
            e);
      }
    }
    // }
    return record;
  }

  /*public String reformat(final String json) throws IOException {
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final JsonGenerator generator = factory
        .createGenerator(stream, JsonEncoding.UTF8);

    final JsonParser parser = factory.createParser(json);
    JsonToken jsonToken = null;
    while ((jsonToken = parser.nextToken()) != null) {
      generator.
    }
    return new String(stream.toByteArray());
  }*/

  private JsonToken processRecord(
      final JsonParser parser,
      ORecord record,
      Map<String, Character> fieldTypes,
      String iOptions,
      boolean noMap,
      ORID rid)
      throws IOException {
    JsonToken jsonToken = parser.currentToken();
    if (jsonToken.equals(JsonToken.FIELD_NAME)) {
      final String fieldName = parser.getValueAsString();
      jsonToken = parser.nextToken();
      final String value = parser.getValueAsString();
      this.processRecords(parser, record, fieldTypes, noMap, iOptions, fieldName, value, rid);
    }
    return jsonToken;
  }

  public ORecord fromStream(
      final InputStream source,
      ORecord record,
      final String[] iFields,
      final String iOptions,
      boolean needReload,
      int maxRidbagSizeBeforeSkip,
      Set<Integer> skippedPartsIndexes) {
    try {
      return this.fromStream(
          source, record, iOptions, needReload, maxRidbagSizeBeforeSkip, skippedPartsIndexes);
    } catch (final JsonParseException e) {
      throw OException.wrapException(
          new OSerializationException(
              "Error on unmarshalling JSON content for record "
                  + record.getIdentity()
                  + " failed fromStream "
                  + e.getMessage()
                  + " and failed fallback to fromString"),
          e);
    }
    /*} catch (final JsonParseException e) {
      final OutputStream out = new ByteArrayOutputStream();
      try {
        OIOUtils.copyStream(source, out, -1);
        return this.fromStringV0(
            out.toString(),
            record,
            iOptions,
            needReload,
            maxRidbagSizeBeforeSkip,
            skippedPartsIndexes);
      } catch (final IOException ex) {
        throw OException.wrapException(
            new OSerializationException(
                "Error on unmarshalling JSON content for record "
                    + record.getIdentity()
                    + " failed fromStream "
                    + e.getMessage()
                    + " and failed fallback to fromString"),
            ex);
      }
    }*/
  }

  public ORecord fromString(
      String source,
      ORecord record,
      final String[] iFields,
      final String iOptions,
      boolean needReload,
      int maxRidbagSizeBeforeSkip,
      Set<Integer> skippedPartsIndexes) {
    return this.fromStringV0(
        source, record, iOptions, needReload, maxRidbagSizeBeforeSkip, skippedPartsIndexes);
  }

  public ORecord fromStream(
      final JsonParser parser,
      InputStream source,
      ORecord record,
      final String[] iFields,
      final String iOptions,
      boolean needReload,
      int maxRidbagSizeBeforeSkip,
      Set<Integer> skippedPartsIndexes)
      throws JsonParseException {
    return this.fromStream(
        source, record, iOptions, needReload, maxRidbagSizeBeforeSkip, skippedPartsIndexes);
  }

  @Deprecated
  public ORecord fromStringV0(
      String source,
      ORecord record,
      final String iOptions,
      boolean needReload,
      int maxRidbagSizeBeforeSkip,
      Set<Integer> skippedPartsIndexes) {
    source = source.trim();
    boolean brackets = source.startsWith("{") && source.endsWith("}");

    String className = null;
    boolean noMap = false;
    if (iOptions != null) {
      final String[] format = iOptions.split(",");
      for (String f : format) if (f.equalsIgnoreCase("noMap")) noMap = true;
    }

    if (record != null)
      // RESET ALL THE FIELDS
      record.clear();

    final List<String> fields =
        OStringSerializerHelper.smartSplit(
            source,
            PARAMETER_SEPARATOR,
            brackets ? 1 : 0,
            brackets ? (source.length() - 2) : -1,
            true,
            true,
            false,
            false,
            maxRidbagSizeBeforeSkip,
            skippedPartsIndexes,
            ' ',
            '\n',
            '\r',
            '\t');

    if (fields.size() % 2 != 0)
      throw new OSerializationException(
          "Error on unmarshalling JSON content: wrong format \""
              + source
              + "\". Use <field> : <value>");

    Map<String, Character> fieldTypes = null;

    if (fields != null && fields.size() > 0) {
      // SEARCH FOR FIELD TYPES IF ANY
      for (int i = 0; i < fields.size(); i += 2) {
        final String fieldName = OIOUtils.getStringContent(fields.get(i));
        final String fieldValue = fields.get(i + 1);
        final String fieldValueAsString = OIOUtils.getStringContent(fieldValue);

        if (fieldName.equals(ATTRIBUTE_FIELD_TYPES) && record instanceof ODocument) {
          fieldTypes = loadFieldTypesV0(fieldTypes, fieldValueAsString);
        } else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_TYPE)) {
          if (record == null
              || ORecordInternal.getRecordType(record) != fieldValueAsString.charAt(0)) {
            // CREATE THE RIGHT RECORD INSTANCE
            record =
                Orient.instance()
                    .getRecordFactoryManager()
                    .newInstance(
                        (byte) fieldValueAsString.charAt(0),
                        -1,
                        ODatabaseRecordThreadLocal.instance().getIfDefined());
          }
        } else if (needReload
            && fieldName.equals(ODocumentHelper.ATTRIBUTE_RID)
            && record instanceof ODocument) {
          if (fieldValue != null && fieldValue.length() > 0) {
            ORecord localRecord =
                ODatabaseRecordThreadLocal.instance().get().load(new ORecordId(fieldValueAsString));
            if (localRecord != null) {
              record = localRecord;
            }
          }
        } else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_CLASS)
            && record instanceof ODocument) {
          className = "null".equals(fieldValueAsString) ? null : fieldValueAsString;
          ODocumentInternal.fillClassNameIfNeeded(((ODocument) record), className);
        }
      }

      if (record == null) record = new ODocument();

      try {
        for (int i = 0; i < fields.size(); i += 2) {
          final String fieldName = OIOUtils.getStringContent(fields.get(i));
          final String fieldValue = fields.get(i + 1);
          final String fieldValueAsString = OIOUtils.getStringContent(fieldValue);

          processRecordsV0(
              record, fieldTypes, noMap, iOptions, fieldName, fieldValue, fieldValueAsString);
        }
        if (className != null) {
          // Trigger the default value
          ((ODocument) record).setClassName(className);
        }
      } catch (Exception e) {
        if (record.getIdentity().isValid())
          throw OException.wrapException(
              new OSerializationException(
                  "Error on unmarshalling JSON content for record " + record.getIdentity()),
              e);
        else
          throw OException.wrapException(
              new OSerializationException(
                  "Error on unmarshalling JSON content for record: " + source),
              e);
      }
    }
    return record;
  }

  private void processRecords(
      final JsonParser parser,
      ORecord record,
      Map<String, Character> fieldTypes,
      boolean noMap,
      String iOptions,
      String fieldName,
      String fieldValue,
      ORID rid)
      throws IOException {
    final JsonToken jsonToken = parser.currentToken();
    final String fieldValueAsString = parser.getValueAsString();

    // RECORD ATTRIBUTES
    if (fieldName.equals(ODocumentHelper.ATTRIBUTE_RID)) {
      ORecordInternal.setIdentity(record, new ORecordId(fieldValueAsString));
    } else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_VERSION)) {
      ORecordInternal.setVersion(record, Integer.parseInt(fieldValue));
    } else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_CLASS) && record instanceof ODocument) {
      final String className = "null".equals(fieldValueAsString) ? null : fieldValueAsString;
      ODocumentInternal.fillClassNameIfNeeded(((ODocument) record), className);

      if (className != null) {
        // Trigger the default value
        ((ODocument) record).setClassName(className);
      }
      return;
    } else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_TYPE)) {
      return;
    } else if (fieldName.equals(ATTRIBUTE_FIELD_TYPES) && record instanceof ODocument) {
      loadFieldTypes(fieldTypes, fieldValueAsString);
      return;
    } else if (fieldName.equals("value") && !(record instanceof ODocument)) {
      // RECORD VALUE(S)
      if ("null".equals(fieldValue)) record.fromStream(OCommonConst.EMPTY_BYTE_ARRAY);
      else if (record instanceof OBlob) {
        // BYTES
        record.fromStream(Base64.getDecoder().decode(fieldValueAsString));
      } else if (record instanceof ORecordStringable) {
        ((ORecordStringable) record).value(fieldValueAsString);
      } else throw new IllegalArgumentException("unsupported type of record");
    } else if (record instanceof ODocument) {
      final ODocument doc = ((ODocument) record);

      // DETERMINE THE TYPE FROM THE SCHEMA
      OType type = determineType(doc, fieldName);

      final Object value;
      if (OStringSerializerHelper.SKIPPED_VALUE.equals(fieldValue)) {
        value = new ORidBag();
      } else {
        value =
            getValue(
                parser,
                doc,
                fieldName,
                fieldValue,
                fieldValueAsString,
                type,
                null,
                fieldTypes,
                noMap,
                iOptions);
      }

      if (value != null)
        if (value instanceof Collection<?> && !((Collection<?>) value).isEmpty()) {
          if (value instanceof ORecordLazyMultiValue)
            ((ORecordLazyMultiValue) value).setAutoConvertToRecord(false);

          // CHECK IF THE COLLECTION IS EMBEDDED
          if (type == null) {
            // TRY TO UNDERSTAND BY FIRST ITEM
            Object first = ((Collection<?>) value).iterator().next();
            if (first != null
                && first instanceof ORecord
                && !((ORecord) first).getIdentity().isValid())
              type = value instanceof Set<?> ? OType.EMBEDDEDSET : OType.EMBEDDEDLIST;
          }

          if (type != null) {
            // TREAT IT AS EMBEDDED
            doc.setProperty(fieldName, value, type);
            return;
          }
        } else if (value instanceof Map<?, ?> && !((Map<?, ?>) value).isEmpty()) {
          // CHECK IF THE MAP IS EMBEDDED
          Object first = ((Map<?, ?>) value).values().iterator().next();
          if (first != null
              && first instanceof ORecord
              && !((ORecord) first).getIdentity().isValid()) {
            doc.setProperty(fieldName, value, OType.EMBEDDEDMAP);
            return;
          }
        } else if (value instanceof ODocument && type != null && type.isLink()) {
          String className1 = ((ODocument) value).getClassName();
          if (className1 != null && className1.length() > 0) ((ODocument) value).save();
        }

      if (type == null && fieldTypes != null && fieldTypes.containsKey(fieldName)) {
        type = ORecordSerializerStringAbstract.getType(fieldValue, fieldTypes.get(fieldName));
      }

      if (value instanceof OTrackedSet<?>) {
        if (OMultiValue.getFirstValue((Set<?>) value) instanceof OIdentifiable)
          type = OType.LINKSET;
      } else if (value instanceof OTrackedList<?>) {
        if (OMultiValue.getFirstValue((List<?>) value) instanceof OIdentifiable)
          type = OType.LINKLIST;
      }

      if (type != null) {
        doc.setProperty(fieldName, value, type);
      } else {
        doc.setProperty(fieldName, value);
      }
    }
  }

  @Deprecated
  private void processRecordsV0(
      ORecord record,
      Map<String, Character> fieldTypes,
      boolean noMap,
      String iOptions,
      String fieldName,
      String fieldValue,
      String fieldValueAsString) {
    // RECORD ATTRIBUTES
    if (fieldName.equals(ODocumentHelper.ATTRIBUTE_RID)) {
      ORecordInternal.setIdentity(record, new ORecordId(fieldValueAsString));
    } else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_VERSION)) {
      ORecordInternal.setVersion(record, Integer.parseInt(fieldValue));
    } else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_CLASS)) {
      return;
    } else if (fieldName.equals(ODocumentHelper.ATTRIBUTE_TYPE)) {
      return;
    } else if (fieldName.equals(ATTRIBUTE_FIELD_TYPES) && record instanceof ODocument) {
      return;
    } else if (fieldName.equals("value") && !(record instanceof ODocument)) {
      // RECORD VALUE(S)
      if ("null".equals(fieldValue)) record.fromStream(OCommonConst.EMPTY_BYTE_ARRAY);
      else if (record instanceof OBlob) {
        // BYTES
        record.fromStream(Base64.getDecoder().decode(fieldValueAsString));
      } else if (record instanceof ORecordStringable) {
        ((ORecordStringable) record).value(fieldValueAsString);
      } else throw new IllegalArgumentException("unsupported type of record");
    } else if (record instanceof ODocument) {
      final ODocument doc = ((ODocument) record);

      // DETERMINE THE TYPE FROM THE SCHEMA
      OType type = determineType(doc, fieldName);

      final Object v;
      if (OStringSerializerHelper.SKIPPED_VALUE.equals(fieldValue)) {
        v = new ORidBag();
      } else {
        v =
            getValueV0(
                doc,
                fieldName,
                fieldValue,
                fieldValueAsString,
                type,
                null,
                fieldTypes,
                noMap,
                iOptions);
      }

      if (v != null)
        if (v instanceof Collection<?> && !((Collection<?>) v).isEmpty()) {
          if (v instanceof ORecordLazyMultiValue)
            ((ORecordLazyMultiValue) v).setAutoConvertToRecord(false);

          // CHECK IF THE COLLECTION IS EMBEDDED
          if (type == null) {
            // TRY TO UNDERSTAND BY FIRST ITEM
            Object first = ((Collection<?>) v).iterator().next();
            if (first != null
                && first instanceof ORecord
                && !((ORecord) first).getIdentity().isValid())
              type = v instanceof Set<?> ? OType.EMBEDDEDSET : OType.EMBEDDEDLIST;
          }

          if (type != null) {
            // TREAT IT AS EMBEDDED
            doc.setProperty(fieldName, v, type);
            return;
          }
        } else if (v instanceof Map<?, ?> && !((Map<?, ?>) v).isEmpty()) {
          // CHECK IF THE MAP IS EMBEDDED
          Object first = ((Map<?, ?>) v).values().iterator().next();
          if (first != null
              && first instanceof ORecord
              && !((ORecord) first).getIdentity().isValid()) {
            doc.setProperty(fieldName, v, OType.EMBEDDEDMAP);
            return;
          }
        } else if (v instanceof ODocument && type != null && type.isLink()) {
          String className1 = ((ODocument) v).getClassName();
          if (className1 != null && className1.length() > 0) ((ODocument) v).save();
        }

      if (type == null && fieldTypes != null && fieldTypes.containsKey(fieldName))
        type = ORecordSerializerStringAbstract.getType(fieldValue, fieldTypes.get(fieldName));

      if (v instanceof OTrackedSet<?>) {
        if (OMultiValue.getFirstValue((Set<?>) v) instanceof OIdentifiable) type = OType.LINKSET;
      } else if (v instanceof OTrackedList<?>) {
        if (OMultiValue.getFirstValue((List<?>) v) instanceof OIdentifiable) type = OType.LINKLIST;
      }

      if (type != null) doc.setProperty(fieldName, v, type);
      else doc.setProperty(fieldName, v);
    }
  }

  public void toString(
      final ORecord iRecord,
      final OJSONWriter json,
      final String iFormat,
      boolean autoDetectCollectionType) {
    try {
      final FormatSettings settings = new FormatSettings(iFormat);

      json.beginObject();

      OJSONFetchContext context = new OJSONFetchContext(json, settings);
      context.writeSignature(json, iRecord);

      if (iRecord instanceof ODocument) {
        final OFetchPlan fp = OFetchHelper.buildFetchPlan(settings.fetchPlan);

        OFetchHelper.fetch(iRecord, null, fp, new OJSONFetchListener(), context, iFormat);
      } else if (iRecord instanceof ORecordStringable) {

        // STRINGABLE
        final ORecordStringable record = (ORecordStringable) iRecord;
        json.writeAttribute(settings.indentLevel, true, "value", record.value());

      } else if (iRecord instanceof OBlob) {
        // BYTES
        final OBlob record = (OBlob) iRecord;
        json.writeAttribute(
            settings.indentLevel,
            true,
            "value",
            Base64.getEncoder().encodeToString(record.toStream()));
      } else
        throw new OSerializationException(
            "Error on marshalling record of type '"
                + iRecord.getClass()
                + "' to JSON. The record type cannot be exported to JSON");

      json.endObject(settings.indentLevel, true);
    } catch (IOException e) {
      throw OException.wrapException(
          new OSerializationException("Error on marshalling of record to JSON"), e);
    }
  }

  @Override
  public StringBuilder toString(
      final ORecord record,
      final StringBuilder output,
      final String format,
      boolean autoDetectCollectionType) {
    try {
      final StringWriter buffer = new StringWriter(INITIAL_SIZE);
      final OJSONWriter json = new OJSONWriter(buffer, format);
      final FormatSettings settings = new FormatSettings(format);

      json.beginObject();

      final OJSONFetchContext context = new OJSONFetchContext(json, settings);
      context.writeSignature(json, record);

      if (record instanceof ODocument) {
        final OFetchPlan fp = OFetchHelper.buildFetchPlan(settings.fetchPlan);

        OFetchHelper.fetch(record, null, fp, new OJSONFetchListener(), context, format);
      } else if (record instanceof ORecordStringable) {
        // STRINGABLE
        final ORecordStringable recordStringable = (ORecordStringable) record;
        json.writeAttribute(settings.indentLevel, true, "value", recordStringable.value());
      } else if (record instanceof OBlob) {
        // BYTES
        final OBlob recordBlob = (OBlob) record;
        json.writeAttribute(
            settings.indentLevel,
            true,
            "value",
            Base64.getEncoder().encodeToString(recordBlob.toStream()));
      } else {
        throw new OSerializationException(
            "Error on marshalling record of type '"
                + record.getClass()
                + "' to JSON. The record type cannot be exported to JSON");
      }
      json.endObject(settings.indentLevel, true);

      output.append(buffer);
      return output;
    } catch (final IOException e) {
      throw OException.wrapException(
          new OSerializationException("Error on marshalling of record to JSON"), e);
    }
  }

  @Override
  public String toString() {
    return NAME;
  }

  private OType determineType(ODocument doc, String fieldName) {
    OType type = null;
    final OClass cls = ODocumentInternal.getImmutableSchemaClass(doc);
    if (cls != null) {
      final OProperty prop = cls.getProperty(fieldName);
      if (prop != null) type = prop.getType();
    }
    return type;
  }

  @Deprecated
  private Map<String, Character> loadFieldTypesV0(
      Map<String, Character> fieldTypes, final String fieldValueAsString) {
    // LOAD THE FIELD TYPE MAP
    final String[] fieldTypesParts = fieldValueAsString.split(",");
    if (fieldTypesParts.length > 0) {
      fieldTypes = new HashMap<>();
      String[] part;
      for (String f : fieldTypesParts) {
        part = f.split("=");
        if (part.length == 2) fieldTypes.put(part[0], part[1].charAt(0));
      }
    }
    return fieldTypes;
  }

  private void loadFieldTypes(
      final Map<String, Character> fieldTypes, final String fieldValueAsString) {
    // LOAD THE FIELD TYPE MAP
    final String[] fieldTypesParts = fieldValueAsString.split(",");
    if (fieldTypesParts.length > 0) {
      String[] part;
      for (String f : fieldTypesParts) {
        part = f.split("=");
        if (part.length == 2) fieldTypes.put(part[0], part[1].charAt(0));
      }
    }
  }

  private String unwrapSource(String iSource) {
    if (iSource == null)
      throw new OSerializationException("Error on unmarshalling JSON content: content is null");

    iSource = iSource.trim();
    if (!iSource.startsWith("{") || !iSource.endsWith("}"))
      throw new OSerializationException(
          "Error on unmarshalling JSON content '" + iSource + "': content must be between { }");

    iSource = iSource.substring(1, iSource.length() - 1).trim();
    return iSource;
  }

  private Object getValue(
      final JsonParser parser,
      final ODocument iRecord,
      String fieldName,
      String fieldValue,
      String fieldValueAsString,
      OType type,
      OType linkedType,
      final Map<String, Character> fieldTypes,
      final boolean noMap,
      final String options)
      throws IOException {
    if (fieldName != null && ODocumentInternal.getImmutableSchemaClass(iRecord) != null) {
      final OProperty p = ODocumentInternal.getImmutableSchemaClass(iRecord).getProperty(fieldName);
      if (p != null) {
        type = p.getType();
        linkedType = p.getLinkedType();
      }
    }

    if (type == null && fieldTypes != null && fieldTypes.containsKey(fieldName)) {
      type = ORecordSerializerStringAbstract.getType(fieldValue, fieldTypes.get(fieldName));
    }

    final JsonToken jsonToken = parser.currentToken();
    if (JsonToken.START_OBJECT.equals(jsonToken)) {
      // Json object
      return getValueAsObjectOrMap(
          parser, iRecord, fieldValue, type, linkedType, fieldTypes, noMap, options);
    } else if (JsonToken.START_ARRAY.equals(jsonToken)) {
      // Json array
      return getValueAsCollection(parser, iRecord, fieldValue, type, linkedType, noMap, options);
    }

    if (type == null || type == OType.ANY) {
      if (JsonToken.VALUE_TRUE.equals(jsonToken) || JsonToken.VALUE_FALSE.equals(jsonToken)) {
        type = OType.BOOLEAN;
      } else if (JsonToken.VALUE_NUMBER_FLOAT.equals(jsonToken)) {
        return parser.getValueAsDouble();
      } else if (JsonToken.VALUE_NUMBER_INT.equals(jsonToken)) {
        final Long longValue = parser.getLongValue();
        if (canBeTrunkedToInt(longValue)) {
          return longValue.intValue();
        } else {
          return longValue;
        }
      } else if (JsonToken.START_OBJECT.equals(jsonToken)
          || JsonToken.END_OBJECT.equals(jsonToken)) {
        return fromString(fieldValueAsString, new ODocumentEmbedded(), null);
      } else {
        if (fieldValue == null || fieldValue.equals("null")) {
          return null;
        }
        if (ORecordId.isA(fieldValueAsString)) {
          final int pos = fieldValueAsString.indexOf('@');
          if (pos > -1)
            // CREATE DOCUMENT
            return new ODocument(
                fieldValueAsString.substring(1, pos),
                new ORecordId(fieldValueAsString.substring(pos + 1)));
          else {
            // CREATE SIMPLE RID
            return new ORecordId(fieldValueAsString);
          }
        }
        if (fieldTypes != null && !fieldTypes.isEmpty()) {
          final Character c = fieldTypes.get(fieldName);
          if (c != null) type = ORecordSerializerStringAbstract.getType(fieldValueAsString, c);
        }
        // Early out, instead of "if (type == null) type = OType.STRING;"
        if (type == null) {
          return parser.getValueAsString();
        }
      }
    }

    if (type != null)
      switch (type) {
        case DATE:
          if (fieldValueAsString == null || fieldValueAsString.equals("")) return null;
          try {
            // TRY TO PARSE AS LONG
            return Long.parseLong(fieldValueAsString);
          } catch (NumberFormatException e) {
            try {
              // TRY TO PARSE AS DATE
              return ODateHelper.getDateFormatInstance().parseObject(fieldValueAsString);
            } catch (ParseException ex) {
              OLogManager.instance()
                  .error(this, "Exception is suppressed, original exception is ", e);
              throw OException.wrapException(
                  new OSerializationException(
                      "Unable to unmarshall date (format="
                          + ODateHelper.getDateFormat()
                          + ") : "
                          + fieldValueAsString),
                  ex);
            }
          }

        case DATETIME:
          if (fieldValueAsString == null || fieldValueAsString.equals("")) return null;
          try {
            // TRY TO PARSE AS LONG
            return Long.parseLong(fieldValueAsString);
          } catch (NumberFormatException e) {
            try {
              // TRY TO PARSE AS DATETIME
              return ODateHelper.getDateTimeFormatInstance().parseObject(fieldValueAsString);
            } catch (ParseException ex) {
              OLogManager.instance()
                  .error(this, "Exception is suppressed, original exception is ", e);
              throw OException.wrapException(
                  new OSerializationException(
                      "Unable to unmarshall datetime (format="
                          + ODateHelper.getDateTimeFormat()
                          + ") : "
                          + fieldValueAsString),
                  ex);
            }
          }
        case BINARY:
          return OStringSerializerHelper.fieldTypeFromStream(iRecord, type, fieldValueAsString);
        case CUSTOM:
          {
            try {
              ByteArrayInputStream bais =
                  new ByteArrayInputStream(Base64.getDecoder().decode(fieldValueAsString));
              ObjectInputStream input = new ObjectInputStream(bais);
              return input.readObject();
            } catch (IOException e) {
              throw OException.wrapException(
                  new OSerializationException("Error on custom field deserialization"), e);
            } catch (ClassNotFoundException e) {
              throw OException.wrapException(
                  new OSerializationException("Error on custom field deserialization"), e);
            }
          }
        default:
          return OStringSerializerHelper.fieldTypeFromStream(iRecord, type, fieldValue);
      }
    return fieldValueAsString;
  }

  @Deprecated
  @SuppressWarnings("unchecked")
  private Object getValueV0(
      final ODocument iRecord,
      String iFieldName,
      String iFieldValue,
      String iFieldValueAsString,
      OType iType,
      OType iLinkedType,
      final Map<String, Character> iFieldTypes,
      final boolean iNoMap,
      final String iOptions) {
    if (iFieldValue.equals("null")) return null;

    if (iFieldName != null && ODocumentInternal.getImmutableSchemaClass(iRecord) != null) {
      final OProperty p =
          ODocumentInternal.getImmutableSchemaClass(iRecord).getProperty(iFieldName);
      if (p != null) {
        iType = p.getType();
        iLinkedType = p.getLinkedType();
      }
    }

    if (iType == null && iFieldTypes != null && iFieldTypes.containsKey(iFieldName))
      iType = ORecordSerializerStringAbstract.getType(iFieldValue, iFieldTypes.get(iFieldName));

    if (iFieldValue.startsWith("{") && iFieldValue.endsWith("}")) {
      // Json object
      return getValueAsObjectOrMapV0(
          iRecord, iFieldValue, iType, iLinkedType, iFieldTypes, iNoMap, iOptions);
    } else if (iFieldValue.startsWith("[") && iFieldValue.endsWith("]")) {
      // Json array
      return getValueAsCollectionV0(
          iRecord, iFieldValue, iType, iLinkedType, iFieldTypes, iNoMap, iOptions);
    }

    if (iType == null || iType == OType.ANY) {
      // TRY TO DETERMINE THE CONTAINED TYPE from THE FIRST VALUE
      if (iFieldValue.charAt(0) != '\"' && iFieldValue.charAt(0) != '\'') {
        if (iFieldValue.equalsIgnoreCase("false") || iFieldValue.equalsIgnoreCase("true"))
          iType = OType.BOOLEAN;
        else {
          Character c = null;
          if (iFieldTypes != null) {
            c = iFieldTypes.get(iFieldName);
            if (c != null) iType = ORecordSerializerStringAbstract.getType(iFieldValue + c);
          }

          if (c == null && !iFieldValue.isEmpty()) {
            // TRY TO AUTODETERMINE THE BEST TYPE
            if (ORecordId.isA(iFieldValue)) iType = OType.LINK;
            else if (iFieldValue.matches(".*[\\.Ee].*")) {
              // DECIMAL FORMAT: DETERMINE IF DOUBLE OR FLOAT
              final Double v = new Double(OIOUtils.getStringContent(iFieldValue));

              return v;
              // REMOVED TRUNK to float
              // if (canBeTrunkedToFloat(v))
              // return v.floatValue();
              // else
              // return v;
            } else {
              final Long v = new Long(OIOUtils.getStringContent(iFieldValue));
              // INTEGER FORMAT: DETERMINE IF DOUBLE OR FLOAT

              if (canBeTrunkedToInt(v)) return v.intValue();
              else return v;
            }
          }
        }
      } else if (iFieldValue.startsWith("{") && iFieldValue.endsWith("}")) iType = OType.EMBEDDED;
      else {
        if (ORecordId.isA(iFieldValueAsString)) iType = OType.LINK;

        if (iFieldTypes != null) {
          Character c = iFieldTypes.get(iFieldName);
          if (c != null) iType = ORecordSerializerStringAbstract.getType(iFieldValueAsString, c);
        }

        if (iType == null) iType = OType.STRING;
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
            return new ODocument(
                iFieldValueAsString.substring(1, pos),
                new ORecordId(iFieldValueAsString.substring(pos + 1)));
          else {
            // CREATE SIMPLE RID
            return new ORecordId(iFieldValueAsString);
          }
        case EMBEDDED:
          return fromString(iFieldValueAsString, new ODocumentEmbedded(), null);
        case DATE:
          if (iFieldValueAsString == null || iFieldValueAsString.equals("")) return null;
          try {
            // TRY TO PARSE AS LONG
            return Long.parseLong(iFieldValueAsString);
          } catch (NumberFormatException e) {
            try {
              // TRY TO PARSE AS DATE
              return ODateHelper.getDateFormatInstance().parseObject(iFieldValueAsString);
            } catch (ParseException ex) {
              OLogManager.instance()
                  .error(this, "Exception is suppressed, original exception is ", e);
              throw OException.wrapException(
                  new OSerializationException(
                      "Unable to unmarshall date (format="
                          + ODateHelper.getDateFormat()
                          + ") : "
                          + iFieldValueAsString),
                  ex);
            }
          }
        case DATETIME:
          if (iFieldValueAsString == null || iFieldValueAsString.equals("")) return null;
          try {
            // TRY TO PARSE AS LONG
            return Long.parseLong(iFieldValueAsString);
          } catch (NumberFormatException e) {
            try {
              // TRY TO PARSE AS DATETIME
              return ODateHelper.getDateTimeFormatInstance().parseObject(iFieldValueAsString);
            } catch (ParseException ex) {
              OLogManager.instance()
                  .error(this, "Exception is suppressed, original exception is ", e);
              throw OException.wrapException(
                  new OSerializationException(
                      "Unable to unmarshall datetime (format="
                          + ODateHelper.getDateTimeFormat()
                          + ") : "
                          + iFieldValueAsString),
                  ex);
            }
          }
        case BINARY:
          return OStringSerializerHelper.fieldTypeFromStream(iRecord, iType, iFieldValueAsString);
        case CUSTOM:
          {
            try {
              ByteArrayInputStream bais =
                  new ByteArrayInputStream(Base64.getDecoder().decode(iFieldValueAsString));
              ObjectInputStream input = new ObjectInputStream(bais);
              return input.readObject();
            } catch (IOException e) {
              throw OException.wrapException(
                  new OSerializationException("Error on custom field deserialization"), e);
            } catch (ClassNotFoundException e) {
              throw OException.wrapException(
                  new OSerializationException("Error on custom field deserialization"), e);
            }
          }
        default:
          return OStringSerializerHelper.fieldTypeFromStream(iRecord, iType, iFieldValue);
      }
    return iFieldValueAsString;
  }

  private boolean canBeTrunkedToInt(Long v) {
    return (v > 0) ? v.compareTo(MAX_INT) <= 0 : v.compareTo(MIN_INT) >= 0;
  }

  private boolean canBeTrunkedToFloat(Double v) {
    // TODO not really correct check. Small numbers with high precision will be trunked while they
    // shouldn't be

    return (v > 0) ? v.compareTo(MAX_FLOAT) <= 0 : v.compareTo(MIN_FLOAT) >= 0;
  }

  /** OBJECT OR MAP. CHECK THE TYPE ATTRIBUTE TO KNOW IT. */
  private Object getValueAsObjectOrMap(
      final JsonParser parser,
      final ODocument record,
      String fieldValue,
      OType type,
      OType linkedType,
      Map<String, Character> fieldTypes,
      boolean noMap,
      String options)
      throws IOException {
    final JsonToken inputToken = parser.currentToken();
    if (!JsonToken.START_OBJECT.equals(inputToken)) {
      throw new IllegalStateException("Expected JSON OBJECT, but was " + inputToken);
    }
    /*final ObjectMapper mapper = new ObjectMapper();
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    final Map<String, Object> map =
        mapper.readValue(parser, new TypeReference<Map<String,Object>>(){});*/

    final JsonToken currentToken = parser.nextToken();
    // JSON Object is empty { }
    // TODO: handle { null } case?
    if (JsonToken.END_OBJECT.equals(currentToken)) {
      if (noMap) {
        final ODocument doc = new ODocument();
        ODocumentInternal.addOwner(doc, record);
        return doc;
      } else {
        return new HashMap<String, Object>(0);
      }
    }
    fieldValue = parser.getValueAsString();
    if (noMap || "@type".equals(fieldValue)) {
      return getObjectValueAsRecord(parser, record, fieldValue, type, options);
    } else {
      return getObjectValueAsMap(parser, record, linkedType, fieldTypes, false, options);
    }
  }

  // FIXME: WIP
  private Object getObjectValueAsRecord(
      JsonParser parser, ODocument record, String fieldValue, OType iType, String iOptions)
      throws IOException {
    // FIXME: handle missing fields
    // ORID rid = new ORecordId(OIOUtils.getStringContent(getFieldValue("@rid", fields)));
    ORID rid = null;
    // ORID rid = new ORecordId(OIOUtils.getStringContent(getFieldValue("@rid", new String[0])));
    // boolean shouldReload = rid.isTemporary();

    final Map<String, Character> fieldTypes = new HashMap<>();
    JsonToken jsonToken = parser.currentToken();
    ODocument recordInternal = new ODocument();
    while (JsonToken.END_OBJECT != jsonToken) {
      // final ODocument recordInternal =
      //    (ODocument) fromString(fieldValue, new ODocument(), null, iOptions, shouldReload);
      processRecord(parser, recordInternal, fieldTypes, fieldValue, false, rid);

      if (ODocumentHelper.ATTRIBUTE_RID.equals(parser.getCurrentName())) {
        rid = new ORecordId(parser.getValueAsString());
        boolean shouldReload = rid.isTemporary();

        // if (shouldReload && record instanceof ODocument) {
        if (shouldReload && recordInternal instanceof ODocument) {
          final ORecord localRecord =
              ODatabaseRecordThreadLocal.instance()
                  .get()
                  .load(new ORecordId(parser.getValueAsString()));
          if (localRecord != null) {
            // record = (ODocument) localRecord;
            recordInternal = (ODocument) localRecord;
          }
        }
        // ORecordInternal.setIdentity(record, new ORecordId(parser.getValueAsString()));
      }
      jsonToken = parser.nextToken();
    }

    if (shouldBeDeserializedAsEmbedded(recordInternal, iType)) {
      ODocumentInternal.addOwner(recordInternal, record);
    } else {
      final ODatabaseDocument database = ODatabaseRecordThreadLocal.instance().getIfDefined();

      if (rid.isPersistent() && database != null) {
        ODocument documentToMerge = database.load(rid);
        documentToMerge.merge(recordInternal, false, false);
        return documentToMerge;
      }
    }
    return recordInternal;
  }

  // FIXME: WIP
  private Object getObjectValueAsMap(
      final JsonParser parser,
      ODocument record,
      OType linkedType,
      final Map<String, Character> fieldTypes,
      boolean noMap,
      String options)
      throws IOException {
    final Map<String, Object> embeddedMap = new LinkedHashMap<>();
    JsonToken jsonToken = parser.currentToken();

    while (JsonToken.END_OBJECT != jsonToken) {
      if (jsonToken.equals(JsonToken.FIELD_NAME)) {
        final String fieldName = parser.getValueAsString();
        final JsonToken currentToken = parser.nextToken();
        final String value = parser.getValueAsString();

        if (value != null) {
          embeddedMap.put(
              fieldName,
              getValue(
                  parser,
                  record,
                  fieldName,
                  value,
                  value,
                  linkedType,
                  null,
                  fieldTypes,
                  noMap,
                  options));
        } else if (value == null
            && (JsonToken.START_OBJECT.equals(currentToken)
                || JsonToken.START_ARRAY.equals(currentToken))) {
          embeddedMap.put(
              fieldName,
              getValue(
                  parser,
                  record,
                  fieldName,
                  value,
                  value,
                  linkedType,
                  null,
                  fieldTypes,
                  noMap,
                  options));
        } else {
          // early out
          embeddedMap.put(fieldName, null);
        }
      }
      jsonToken = parser.nextToken();
    }
    return embeddedMap;
  }

  /** OBJECT OR MAP. CHECK THE TYPE ATTRIBUTE TO KNOW IT. */
  @Deprecated
  private Object getValueAsObjectOrMapV0(
      ODocument iRecord,
      String iFieldValue,
      OType iType,
      OType iLinkedType,
      Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions) {
    final String[] fields =
        OStringParser.getWords(iFieldValue.substring(1, iFieldValue.length() - 1), ":,", true);

    if (fields == null || fields.length == 0)
      if (iNoMap) {
        ODocument res = new ODocument();
        ODocumentInternal.addOwner(res, iRecord);
        return res;
      } else return new HashMap<String, Object>();

    if (iNoMap || hasTypeField(fields)) {
      return getObjectValuesAsRecordV0(iRecord, iFieldValue, iType, iOptions, fields);
    } else {
      return getObjectValuesAsMapV0(
          iRecord, iFieldValue, iLinkedType, iFieldTypes, false, iOptions, fields);
    }
  }

  @Deprecated
  private Object getObjectValuesAsMapV0(
      ODocument iRecord,
      String iFieldValue,
      OType iLinkedType,
      Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions,
      String[] fields) {
    if (fields.length % 2 == 1) {
      throw new OSerializationException(
          "Bad JSON format on map. Expected pairs of field:value but received '"
              + iFieldValue
              + "'");
    }
    final Map<String, Object> embeddedMap = new LinkedHashMap<>();

    for (int i = 0; i < fields.length; i += 2) {
      String iFieldName = fields[i];
      if (iFieldName.length() >= 2) iFieldName = iFieldName.substring(1, iFieldName.length() - 1);
      iFieldValue = fields[i + 1];
      final String valueAsString = OIOUtils.getStringContent(iFieldValue);

      embeddedMap.put(
          iFieldName,
          getValueV0(
              iRecord,
              null,
              iFieldValue,
              valueAsString,
              iLinkedType,
              null,
              iFieldTypes,
              iNoMap,
              iOptions));
    }
    return embeddedMap;
  }

  @Deprecated
  private Object getObjectValuesAsRecordV0(
      ODocument iRecord, String iFieldValue, OType iType, String iOptions, String[] fields) {
    ORID rid = new ORecordId(OIOUtils.getStringContent(getFieldValue("@rid", fields)));
    boolean shouldReload = rid.isTemporary();

    final ODocument recordInternal =
        (ODocument) fromString(iFieldValue, new ODocument(), null, iOptions, shouldReload);

    if (shouldBeDeserializedAsEmbedded(recordInternal, iType))
      ODocumentInternal.addOwner(recordInternal, iRecord);
    else {
      ODatabaseDocument database = ODatabaseRecordThreadLocal.instance().getIfDefined();

      if (rid.isPersistent() && database != null) {
        ODocument documentToMerge = database.load(rid);
        documentToMerge.merge(recordInternal, false, false);
        return documentToMerge;
      }
    }
    return recordInternal;
  }

  private Object getValueAsCollection(
      final JsonParser parser,
      ODocument iRecord,
      String iFieldValue,
      OType iType,
      OType iLinkedType,
      // Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions)
      throws IOException {
    // remove square brackets
    // iFieldValue = iFieldValue.substring(1, iFieldValue.length() - 1);

    if (iType == OType.LINKBAG) {
      final ORidBag bag = new ORidBag();

      parseRidbag(
          iRecord,
          iFieldValue,
          iType,
          OType.LINK,
          null,
          iNoMap,
          iOptions,
          new CollectionItemVisitor() {
            @Override
            public void visitItem(Object item) {
              bag.add((OIdentifiable) item);
            }
          });

      return bag;
    } else if (iType == OType.LINKSET) {
      return getValueAsLinkedCollection(
          parser,
          new ORecordLazySet(iRecord),
          iRecord,
          iFieldValue,
          iType,
          iLinkedType,
          null,
          iNoMap,
          iOptions);
    } else if (iType == OType.LINKLIST) {
      return getValueAsLinkedCollection(
          parser,
          new ORecordLazyList(iRecord),
          iRecord,
          iFieldValue,
          iType,
          iLinkedType,
          null,
          iNoMap,
          iOptions);
    } else if (iType == OType.EMBEDDEDSET) {
      return getValueAsEmbeddedCollection(
          parser,
          new OTrackedSet<Object>(iRecord),
          iRecord,
          iFieldValue,
          iType,
          iLinkedType,
          null,
          iNoMap,
          iOptions);
    } else {
      return getValueAsEmbeddedCollection(
          parser,
          new OTrackedList<>(iRecord),
          iRecord,
          iFieldValue,
          iType,
          iLinkedType,
          null,
          iNoMap,
          iOptions);
    }
  }

  @Deprecated
  private Object getValueAsCollectionV0(
      ODocument iRecord,
      String iFieldValue,
      OType iType,
      OType iLinkedType,
      Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions) {
    // remove square brackets
    iFieldValue = iFieldValue.substring(1, iFieldValue.length() - 1);

    if (iType == OType.LINKBAG) {
      final ORidBag bag = new ORidBag();

      parseRidbag(
          iRecord,
          iFieldValue,
          iType,
          OType.LINK,
          iFieldTypes,
          iNoMap,
          iOptions,
          new CollectionItemVisitor() {
            @Override
            public void visitItem(Object item) {
              bag.add((OIdentifiable) item);
            }
          });

      return bag;
    } else if (iType == OType.LINKSET) {
      return getValueAsLinkedCollectionV0(
          new ORecordLazySet(iRecord),
          iRecord,
          iFieldValue,
          iType,
          iLinkedType,
          iFieldTypes,
          iNoMap,
          iOptions);
    } else if (iType == OType.LINKLIST) {
      return getValueAsLinkedCollectionV0(
          new ORecordLazyList(iRecord),
          iRecord,
          iFieldValue,
          iType,
          iLinkedType,
          iFieldTypes,
          iNoMap,
          iOptions);
    } else if (iType == OType.EMBEDDEDSET) {
      return getValueAsEmbeddedCollectionV0(
          new OTrackedSet<>(iRecord),
          iRecord,
          iFieldValue,
          iType,
          iLinkedType,
          iFieldTypes,
          iNoMap,
          iOptions);
    } else {
      return getValueAsEmbeddedCollectionV0(
          new OTrackedList<>(iRecord),
          iRecord,
          iFieldValue,
          iType,
          iLinkedType,
          iFieldTypes,
          iNoMap,
          iOptions);
    }
  }

  private Object getValueAsLinkedCollection(
      final JsonParser parser,
      final Collection<OIdentifiable> collection,
      ODocument iRecord,
      String iFieldValue,
      OType iType,
      OType iLinkedType,
      Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions)
      throws IOException {
    this.parseCollection(
        parser,
        iRecord,
        iFieldValue,
        iType,
        iLinkedType,
        iFieldTypes,
        iNoMap,
        iOptions,
        item -> collection.add((OIdentifiable) item));
    return collection;
  }

  @Deprecated
  private Object getValueAsLinkedCollectionV0(
      final Collection<OIdentifiable> collection,
      ODocument iRecord,
      String iFieldValue,
      OType iType,
      OType iLinkedType,
      Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions) {

    parseCollectionV0(
        iRecord,
        iFieldValue,
        iType,
        iLinkedType,
        iFieldTypes,
        iNoMap,
        iOptions,
        new CollectionItemVisitor() {
          @Override
          public void visitItem(Object item) {
            collection.add((OIdentifiable) item);
          }
        });

    return collection;
  }

  private Object getValueAsEmbeddedCollection(
      final JsonParser parser,
      final Collection<Object> collection,
      ODocument iRecord,
      String iFieldValue,
      OType iType,
      OType iLinkedType,
      Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions)
      throws IOException {

    parseCollection(
        parser,
        iRecord,
        iFieldValue,
        iType,
        iLinkedType,
        iFieldTypes,
        iNoMap,
        iOptions,
        new CollectionItemVisitor() {
          @Override
          public void visitItem(Object item) {
            collection.add(item);
          }
        });
    return collection;
  }

  @Deprecated
  private Object getValueAsEmbeddedCollectionV0(
      final Collection<Object> collection,
      ODocument iRecord,
      String iFieldValue,
      OType iType,
      OType iLinkedType,
      Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions) {

    parseCollectionV0(
        iRecord,
        iFieldValue,
        iType,
        iLinkedType,
        iFieldTypes,
        iNoMap,
        iOptions,
        new CollectionItemVisitor() {
          @Override
          public void visitItem(Object item) {
            collection.add(item);
          }
        });
    return collection;
  }

  private void parseRidbag(
      ODocument iRecord,
      String iFieldValue,
      OType iType,
      OType iLinkedType,
      Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions,
      CollectionItemVisitor visitor) {
    if (!iFieldValue.isEmpty()) {
      int lastCommaPosition = -1;
      for (int i = 1; i < iFieldValue.length(); i++) {
        if (iFieldValue.charAt(i) == ',' || i == iFieldValue.length() - 1) {
          String item;
          if (i == iFieldValue.length() - 1) {
            item = iFieldValue.substring(lastCommaPosition + 1);
          } else {
            item = iFieldValue.substring(lastCommaPosition + 1, i);
          }
          lastCommaPosition = i;
          final String itemValue = item.trim();
          if (itemValue.length() == 0) continue;

          final Object collectionItem =
              getValueV0(
                  iRecord,
                  null,
                  itemValue,
                  OIOUtils.getStringContent(itemValue),
                  iLinkedType,
                  null,
                  iFieldTypes,
                  iNoMap,
                  iOptions);

          // TODO: redundant in some cases, owner is already added by getValueV0 in some cases
          if (shouldBeDeserializedAsEmbedded(collectionItem, iType))
            ODocumentInternal.addOwner((ODocument) collectionItem, iRecord);

          visitor.visitItem(collectionItem);
        }
      }
    }
  }

  private void parseCollection(
      JsonParser parser,
      ODocument record,
      String fieldValue,
      OType type,
      OType linkedType,
      Map<String, Character> fieldTypes,
      boolean noMap,
      String options,
      CollectionItemVisitor visitor)
      throws IOException {
    JsonToken jsonToken = parser.currentToken();
    if (!JsonToken.START_ARRAY.equals(jsonToken)) {
      throw new IllegalStateException("Expected JSON ARRAY, but was " + jsonToken);
    }
    jsonToken = parser.nextToken();
    while (!JsonToken.END_ARRAY.equals(jsonToken)) {
      final String itemValue = parser.getValueAsString();

      // Object collectionItem = null;
      // if (itemValue != null) {
      //  if (itemValue.length() == 0) {
      //    continue;
      //  }
      final Object collectionItem =
          getValue(
              parser,
              record,
              null,
              itemValue,
              itemValue,
              linkedType,
              null,
              fieldTypes,
              noMap,
              options);
      // }

      // TODO: redundant in some cases, owner might have been already added by getValue before
      if (shouldBeDeserializedAsEmbedded(collectionItem, type)) {
        ODocumentInternal.addOwner((ODocument) collectionItem, record);
      }

      visitor.visitItem(collectionItem);
      jsonToken = parser.nextToken();
    }
  }

  @Deprecated
  private void parseCollectionV0(
      ODocument iRecord,
      String iFieldValue,
      OType iType,
      OType iLinkedType,
      Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions,
      CollectionItemVisitor visitor) {
    if (!iFieldValue.isEmpty()) {
      for (String item : OStringSerializerHelper.smartSplit(iFieldValue, ',')) {
        final String itemValue = item.trim();
        if (itemValue.length() == 0) continue;

        final Object collectionItem =
            getValueV0(
                iRecord,
                null,
                itemValue,
                OIOUtils.getStringContent(itemValue),
                iLinkedType,
                null,
                iFieldTypes,
                iNoMap,
                iOptions);

        // TODO redundant in some cases, owner is already added by getValueV0 in some cases
        if (shouldBeDeserializedAsEmbedded(collectionItem, iType))
          ODocumentInternal.addOwner((ODocument) collectionItem, iRecord);

        visitor.visitItem(collectionItem);
      }
    }
  }

  private boolean shouldBeDeserializedAsEmbedded(Object record, OType iType) {
    return record instanceof ODocument
        && !((ODocument) record).getIdentity().isTemporary()
        && !((ODocument) record).getIdentity().isPersistent()
        && (iType == null || !iType.isLink());
  }

  private String decodeJSON(String iFieldValueAsString) {
    if (iFieldValueAsString == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder(iFieldValueAsString.length());
    boolean quoting = false;
    for (char c : iFieldValueAsString.toCharArray()) {
      if (quoting) {
        if (c != '\\' && c != '\"' && c != '/') {
          builder.append('\\');
        }
        builder.append(c);
        quoting = false;
      } else {
        if (c == '\\') {
          quoting = true;
        } else {
          builder.append(c);
        }
      }
    }
    return builder.toString();
  }

  private boolean hasTypeField(final String[] fields) {
    return hasField("@type", fields);
  }

  /**
   * Checks if given collection of fields contain field with specified name.
   *
   * @param field to find
   * @param fields collection of fields where search
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

  @Override
  public String getName() {
    return NAME;
  }
}
