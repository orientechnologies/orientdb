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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON.FormatSettings;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Set;
import java.util.Stack;

/**
 * @author luca.molino
 */
public class OJSONFetchContext implements OFetchContext {

  protected final OJSONWriter    jsonWriter;
  protected final FormatSettings settings;
  protected final Stack<StringBuilder> typesStack      = new Stack<StringBuilder>();
  protected final Stack<ODocument>     collectionStack = new Stack<ODocument>();

  public OJSONFetchContext(final OJSONWriter iJsonWriter, final FormatSettings iSettings) {
    jsonWriter = iJsonWriter;
    settings = iSettings;
  }

  public void onBeforeFetch(final ODocument iRootRecord) {
    typesStack.add(new StringBuilder());
  }

  public void onAfterFetch(final ODocument iRootRecord) {
    StringBuilder buffer = typesStack.pop();
    if (settings.keepTypes && buffer.length()>0)
      try {
        jsonWriter.writeAttribute(settings.indentLevel>-1 ? settings.indentLevel : 1, true, ORecordSerializerJSON.ATTRIBUTE_FIELD_TYPES, buffer.toString());
      } catch (IOException e) {
        throw new OFetchException("Error writing field types", e);
      }
  }

  public void onBeforeStandardField(final Object iFieldValue, final String iFieldName, final Object iUserObject) {
    manageTypes(iFieldName, iFieldValue);
  }

  public void onAfterStandardField(Object iFieldValue, String iFieldName, Object iUserObject) {
  }

  public void onBeforeArray(final ODocument iRootRecord, final String iFieldName, final Object iUserObject, final OIdentifiable[] iArray) {
    onBeforeCollection(iRootRecord, iFieldName, iUserObject, null);
  }

  public void onAfterArray(final ODocument iRootRecord, final String iFieldName, final Object iUserObject) {
    onAfterCollection(iRootRecord, iFieldName, iUserObject);
  }

  public void onBeforeCollection(final ODocument iRootRecord, final String iFieldName, final Object iUserObject, final Iterable<?> iterable) {
    try {
      manageTypes(iFieldName, iterable);
      jsonWriter.beginCollection(++settings.indentLevel, true, iFieldName);
      collectionStack.add(iRootRecord);
    } catch (IOException e) {
      throw new OFetchException("Error writing collection field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
    }
  }

  public void onAfterCollection(final ODocument iRootRecord, final String iFieldName, final Object iUserObject) {
    try {
      jsonWriter.endCollection(settings.indentLevel--, true);
      collectionStack.pop();
    } catch (IOException e) {
      throw new OFetchException("Error writing collection field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
    }
  }

  public void onBeforeMap(final ODocument iRootRecord, final String iFieldName, final Object iUserObject) {
    try {
      jsonWriter.beginObject(++settings.indentLevel, true, iFieldName);
      if (!(iUserObject instanceof ODocument)) {
        collectionStack.add(new ODocument()); // <-- sorry for this... fixes #2845 but this mess should be rewritten...
      }
    } catch (IOException e) {
      throw new OFetchException("Error writing map field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
    }
  }

  public void onAfterMap(final ODocument iRootRecord, final String iFieldName, final Object iUserObject) {
    try {
      jsonWriter.endObject(--settings.indentLevel, true);
      if (!(iUserObject instanceof ODocument)) {
        collectionStack.pop();
      }
    } catch (IOException e) {
      throw new OFetchException("Error writing map field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
    }
  }

  public void onBeforeDocument(final ODocument iRootRecord, final ODocument iDocument, final String iFieldName, final Object iUserObject) {
    try {
      final String fieldName;
      if (!collectionStack.isEmpty() && collectionStack.peek().equals(iRootRecord))
        fieldName = null;
      else
        fieldName = iFieldName;
      jsonWriter.beginObject(++settings.indentLevel, true, fieldName);
      writeSignature(jsonWriter, iDocument);
    } catch (IOException e) {
      throw new OFetchException("Error writing link field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
    }
  }

  public void onAfterDocument(final ODocument iRootRecord, final ODocument iDocument, final String iFieldName, final Object iUserObject) {
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
    final Object link = iRecord.getIdentity().isValid() ? OJSONWriter.encode(iRecord.getIdentity()) : null;
    jsonWriter.writeAttribute(settings.indentLevel, true, iFieldName, link);
  }

  public boolean isInCollection(ODocument record) {
    return !collectionStack.isEmpty() && collectionStack.peek().equals(record);
  }

  public OJSONWriter getJsonWriter() {
    return jsonWriter;
  }

  public int getIndentLevel() {
    return settings.indentLevel;
  }

  public void writeSignature(final OJSONWriter json, final ORecord record) throws IOException {
    if (record == null) {
      json.write("null");
      return;
    }

    boolean firstAttribute = true;

    if (settings.includeType) {
      json.writeAttribute(firstAttribute ? settings.indentLevel : 1, firstAttribute, ODocumentHelper.ATTRIBUTE_TYPE, "" + (char) ORecordInternal.getRecordType(record));
      if (settings.attribSameRow)
        firstAttribute = false;
    }
    if (settings.includeId && record.getIdentity() != null && record.getIdentity().isValid()) {
      json.writeAttribute(!firstAttribute ? settings.indentLevel : 1, firstAttribute, ODocumentHelper.ATTRIBUTE_RID, record.getIdentity().toString());
      if (settings.attribSameRow)
        firstAttribute = false;
    }
    if (settings.includeVer) {
      json.writeAttribute(firstAttribute ? settings.indentLevel : 1, firstAttribute, ODocumentHelper.ATTRIBUTE_VERSION, record.getRecordVersion().getCounter());
      if (settings.attribSameRow)
        firstAttribute = false;
    }
    if (settings.includeClazz && record instanceof ODocument && ((ODocument) record).getClassName() != null) {
      json.writeAttribute(firstAttribute ? settings.indentLevel : 1, firstAttribute, ODocumentHelper.ATTRIBUTE_CLASS, ((ODocument) record).getClassName());
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
      else if (iFieldValue instanceof OIdentifiable)
        appendType(typesStack.peek(), iFieldName, 'x');
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
      else if (iFieldValue instanceof ORecordLazySet)
        appendType(typesStack.peek(), iFieldName, 'n');
      else if (iFieldValue instanceof Set<?>)
        appendType(typesStack.peek(), iFieldName, 'e');
      else if (iFieldValue instanceof ORidBag)
        appendType(typesStack.peek(), iFieldName, 'g');
      else {
        final OType t = OType.getTypeByValue(iFieldValue);
        if (t == OType.LINKLIST)
          appendType(typesStack.peek(), iFieldName, 'z');
        else if (t == OType.LINKMAP)
          appendType(typesStack.peek(), iFieldName, 'm');
        else if (t == OType.CUSTOM)
          appendType(typesStack.peek(), iFieldName, 'u');
      }
    }
  }

  private void appendType(final StringBuilder iBuffer, final String iFieldName, final char iType) {
    if (iBuffer.length()>0)
      iBuffer.append(',');
    iBuffer.append(iFieldName);
    iBuffer.append('=');
    iBuffer.append(iType);
  }
}
