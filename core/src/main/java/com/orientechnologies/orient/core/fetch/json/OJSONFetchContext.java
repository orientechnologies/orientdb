/*
 *
 * Copyright 2012 Luca Molino (molino.luca--AT--gmail.com)
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
package com.orientechnologies.orient.core.fetch.json;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.Set;
import java.util.Stack;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON.FormatSettings;
import com.orientechnologies.orient.core.version.ODistributedVersion;

/**
 * @author luca.molino
 * 
 */
public class OJSONFetchContext implements OFetchContext {

  protected final OJSONWriter                  jsonWriter;
  protected final FormatSettings               settings;
  protected final Stack<StringBuilder>         typesStack      = new Stack<StringBuilder>();
  protected final Stack<ORecordSchemaAware<?>> collectionStack = new Stack<ORecordSchemaAware<?>>();

  public OJSONFetchContext(final OJSONWriter iJsonWriter, final FormatSettings iSettings) {
    jsonWriter = iJsonWriter;
    settings = iSettings;
  }

  public void onBeforeFetch(final ORecordSchemaAware<?> iRootRecord) {
    typesStack.add(new StringBuilder());
  }

  public void onAfterFetch(final ORecordSchemaAware<?> iRootRecord) {
    StringBuilder buffer = typesStack.pop();
    if (settings.keepTypes && buffer.length() > 0)
      try {
        jsonWriter.writeAttribute(settings.indentLevel > -1 ? settings.indentLevel + 1 : -1, true,
            ORecordSerializerJSON.ATTRIBUTE_FIELD_TYPES, buffer.toString());
      } catch (IOException e) {
        throw new OFetchException("Error writing field types", e);
      }
  }

  public void onBeforeStandardField(final Object iFieldValue, final String iFieldName, final Object iUserObject) {
    manageTypes(iFieldName, iFieldValue);
  }

  public void onAfterStandardField(Object iFieldValue, String iFieldName, Object iUserObject) {
  }

  public void onBeforeArray(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject,
      final OIdentifiable[] iArray) {
    onBeforeCollection(iRootRecord, iFieldName, iUserObject, null);
  }

  public void onAfterArray(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject) {
    onAfterCollection(iRootRecord, iFieldName, iUserObject);
  }

  public void onBeforeCollection(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject,
      final Collection<?> iCollection) {
    settings.indentLevel++;
    try {
      manageTypes(iFieldName, iCollection);
      jsonWriter.beginCollection(settings.indentLevel, true, iFieldName);
      collectionStack.add(iRootRecord);
    } catch (IOException e) {
      throw new OFetchException("Error writing collection field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
    }
  }

  public void onAfterCollection(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject) {
    try {
      jsonWriter.endCollection(settings.indentLevel, false);
      collectionStack.pop();
    } catch (IOException e) {
      throw new OFetchException("Error writing collection field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
    }
    settings.indentLevel--;
  }

  public void onBeforeMap(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject) {
    settings.indentLevel++;
    try {
      jsonWriter.beginObject(settings.indentLevel, true, iFieldName);
    } catch (IOException e) {
      throw new OFetchException("Error writing map field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
    }
  }

  public void onAfterMap(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject) {
    try {
      jsonWriter.endObject(settings.indentLevel, true);
    } catch (IOException e) {
      throw new OFetchException("Error writing map field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
    }
    settings.indentLevel--;
  }

  public void onBeforeDocument(final ORecordSchemaAware<?> iRootRecord, final ORecordSchemaAware<?> iDocument,
      final String iFieldName, final Object iUserObject) {
    settings.indentLevel++;
    try {
      final String fieldName;
      if (!collectionStack.isEmpty() && collectionStack.peek().equals(iRootRecord))
        fieldName = null;
      else
        fieldName = iFieldName;
      jsonWriter.beginObject(settings.indentLevel, false, fieldName);
      writeSignature(jsonWriter, iDocument);
    } catch (IOException e) {
      throw new OFetchException("Error writing link field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
    }
  }

  public void onAfterDocument(final ORecordSchemaAware<?> iRootRecord, final ORecordSchemaAware<?> iDocument,
      final String iFieldName, final Object iUserObject) {
    try {
      jsonWriter.endObject(settings.indentLevel--, true);
    } catch (IOException e) {
      throw new OFetchException("Error writing link field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
    }
  }

  public void writeLinkedValue(final OIdentifiable iRecord, final String iFieldName) throws IOException {
    jsonWriter.writeValue(settings.indentLevel, true, OJSONWriter.encode(iRecord.getIdentity()));
  }

  public void writeLinkedAttribute(final OIdentifiable iRecord, final String iFieldName) throws IOException {
    jsonWriter.writeAttribute(settings.indentLevel, true, iFieldName, OJSONWriter.encode(iRecord.getIdentity()));
  }

  public boolean isInCollection(ORecordSchemaAware<?> record) {
    return !collectionStack.isEmpty() && collectionStack.peek().equals(record);
  }

  public OJSONWriter getJsonWriter() {
    return jsonWriter;
  }

  public int getIndentLevel() {
    return settings.indentLevel;
  }

  private void appendType(final StringBuilder iBuffer, final String iFieldName, final char iType) {
    if (iBuffer.length() > 0)
      iBuffer.append(',');
    iBuffer.append(iFieldName);
    iBuffer.append('=');
    iBuffer.append(iType);
  }

  public void writeSignature(final OJSONWriter json, final ORecordInternal<?> record) throws IOException {
    boolean firstAttribute = true;

    if (settings.indentLevel > -1)
      settings.indentLevel++;

    if (settings.includeType) {
      json.writeAttribute(firstAttribute ? settings.indentLevel : 0, firstAttribute, ODocumentHelper.ATTRIBUTE_TYPE, ""
          + (char) record.getRecordType());
      if (settings.attribSameRow)
        firstAttribute = false;
    }
    if (settings.includeId && record.getIdentity() != null && record.getIdentity().isValid()) {
      json.writeAttribute(!firstAttribute ? settings.indentLevel : 0, firstAttribute, ODocumentHelper.ATTRIBUTE_RID, record
          .getIdentity().toString());
      if (settings.attribSameRow)
        firstAttribute = false;
    }
    if (settings.includeVer) {
      json.writeAttribute(firstAttribute ? settings.indentLevel : 0, firstAttribute, ODocumentHelper.ATTRIBUTE_VERSION, record
          .getRecordVersion().getCounter());
      if (settings.attribSameRow)
        firstAttribute = false;
      if (OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.getValueAsBoolean()) {
        final ODistributedVersion ver = (ODistributedVersion) record.getRecordVersion();
        json.writeAttribute(firstAttribute ? settings.indentLevel : 0, firstAttribute, ODocumentHelper.ATTRIBUTE_VERSION_TIMESTAMP,
            ver.getTimestamp());
        json.writeAttribute(firstAttribute ? settings.indentLevel : 0, firstAttribute,
            ODocumentHelper.ATTRIBUTE_VERSION_MACADDRESS, ver.getMacAddress());
      }
    }
    if (settings.includeClazz && record instanceof ORecordSchemaAware<?> && ((ORecordSchemaAware<?>) record).getClassName() != null) {
      json.writeAttribute(firstAttribute ? settings.indentLevel : 0, firstAttribute, ODocumentHelper.ATTRIBUTE_CLASS,
          ((ORecordSchemaAware<?>) record).getClassName());
      if (settings.attribSameRow)
        firstAttribute = false;
    }
  }

  public boolean fetchEmbeddedDocuments() {
    return settings.alwaysFetchEmbeddedDocuments;
  }

  protected void manageTypes(final String iFieldName, final Object iFieldValue) {
    if (settings.keepTypes) {
      if (iFieldValue instanceof Long)
        appendType(typesStack.peek(), iFieldName, 'l');
      else if (iFieldValue instanceof Float)
        appendType(typesStack.peek(), iFieldName, 'f');
      else if (iFieldValue instanceof Short)
        appendType(typesStack.peek(), iFieldName, 's');
      else if (iFieldValue instanceof Double)
        appendType(typesStack.peek(), iFieldName, 'd');
      else if (iFieldValue instanceof Date)
        appendType(typesStack.peek(), iFieldName, 't');
      else if (iFieldValue instanceof Byte || iFieldValue instanceof byte[])
        appendType(typesStack.peek(), iFieldName, 'b');
      else if (iFieldValue instanceof BigDecimal)
        appendType(typesStack.peek(), iFieldName, 'c');
      else if (iFieldValue instanceof Set<?>)
        appendType(typesStack.peek(), iFieldName, 'e');
    }
  }
}
