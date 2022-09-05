/*
 *
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
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

/** @author Luca Molino (molino.luca--at--gmail.com) */
public class OJSONFetchContext implements OFetchContext {

  protected final OJSONWriter jsonWriter;
  protected final FormatSettings settings;
  protected final Stack<StringBuilder> typesStack = new Stack<>();
  protected final Stack<ODocument> collectionStack = new Stack<>();

  public OJSONFetchContext(final OJSONWriter jsonWriter, final FormatSettings settings) {
    this.jsonWriter = jsonWriter;
    this.settings = settings;
  }

  public void onBeforeFetch(final ODocument rootRecord) {
    typesStack.add(new StringBuilder());
  }

  public void onAfterFetch(final ODocument rootRecord) {
    final StringBuilder sb = typesStack.pop();
    if (settings.keepTypes && sb.length() > 0) {
      try {
        jsonWriter.writeAttribute(
            settings.indentLevel > -1 ? settings.indentLevel : 1,
            true,
            ORecordSerializerJSON.ATTRIBUTE_FIELD_TYPES,
            sb.toString());
      } catch (final IOException e) {
        throw OException.wrapException(new OFetchException("Error writing field types"), e);
      }
    }
  }

  public void onBeforeStandardField(
      final Object iFieldValue,
      final String iFieldName,
      final Object iUserObject,
      OType fieldType) {
    manageTypes(iFieldName, iFieldValue, fieldType);
  }

  public void onAfterStandardField(
      Object iFieldValue, String iFieldName, Object iUserObject, OType fieldType) {}

  public void onBeforeArray(
      final ODocument iRootRecord,
      final String iFieldName,
      final Object iUserObject,
      final OIdentifiable[] iArray) {
    onBeforeCollection(iRootRecord, iFieldName, iUserObject, null);
  }

  public void onAfterArray(
      final ODocument iRootRecord, final String iFieldName, final Object iUserObject) {
    onAfterCollection(iRootRecord, iFieldName, iUserObject);
  }

  public void onBeforeCollection(
      final ODocument rootRecord,
      final String fieldName,
      final Object userObject,
      final Iterable<?> iterable) {
    try {
      manageTypes(fieldName, iterable, null);
      jsonWriter.beginCollection(++settings.indentLevel, true, fieldName);
      collectionStack.add(rootRecord);
    } catch (final IOException e) {
      throw OException.wrapException(
          new OFetchException(
              "Error writing collection field "
                  + fieldName
                  + " of record "
                  + rootRecord.getIdentity()),
          e);
    }
  }

  public void onAfterCollection(
      final ODocument iRootRecord, final String iFieldName, final Object iUserObject) {
    try {
      jsonWriter.endCollection(settings.indentLevel--, true);
      collectionStack.pop();
    } catch (IOException e) {
      throw OException.wrapException(
          new OFetchException(
              "Error writing collection field "
                  + iFieldName
                  + " of record "
                  + iRootRecord.getIdentity()),
          e);
    }
  }

  public void onBeforeMap(
      final ODocument iRootRecord, final String iFieldName, final Object iUserObject) {
    try {
      jsonWriter.beginObject(++settings.indentLevel, true, iFieldName);
      if (!(iUserObject instanceof ODocument)) {
        collectionStack.add(
            new ODocument()); // <-- sorry for this... fixes #2845 but this mess should be
        // rewritten...
      }
    } catch (IOException e) {
      throw OException.wrapException(
          new OFetchException(
              "Error writing map field " + iFieldName + " of record " + iRootRecord.getIdentity()),
          e);
    }
  }

  public void onAfterMap(
      final ODocument iRootRecord, final String iFieldName, final Object iUserObject) {
    try {
      jsonWriter.endObject(--settings.indentLevel, true);
      if (!(iUserObject instanceof ODocument)) {
        collectionStack.pop();
      }
    } catch (IOException e) {
      throw OException.wrapException(
          new OFetchException(
              "Error writing map field " + iFieldName + " of record " + iRootRecord.getIdentity()),
          e);
    }
  }

  public void onBeforeDocument(
      final ODocument iRootRecord,
      final ODocument iDocument,
      final String iFieldName,
      final Object iUserObject) {
    try {
      final String fieldName;
      if (!collectionStack.isEmpty() && collectionStack.peek().equals(iRootRecord))
        fieldName = null;
      else fieldName = iFieldName;
      jsonWriter.beginObject(++settings.indentLevel, true, fieldName);
      writeSignature(jsonWriter, iDocument);
    } catch (IOException e) {
      throw OException.wrapException(
          new OFetchException(
              "Error writing link field " + iFieldName + " of record " + iRootRecord.getIdentity()),
          e);
    }
  }

  public void onAfterDocument(
      final ODocument iRootRecord,
      final ODocument iDocument,
      final String iFieldName,
      final Object iUserObject) {
    try {
      jsonWriter.endObject(settings.indentLevel--, true);
    } catch (IOException e) {
      throw OException.wrapException(
          new OFetchException(
              "Error writing link field " + iFieldName + " of record " + iRootRecord.getIdentity()),
          e);
    }
  }

  public void writeLinkedValue(final OIdentifiable iRecord, final String iFieldName)
      throws IOException {
    jsonWriter.writeValue(settings.indentLevel, true, OJSONWriter.encode(iRecord.getIdentity()));
  }

  public void writeLinkedAttribute(final OIdentifiable iRecord, final String iFieldName)
      throws IOException {
    final Object link =
        iRecord.getIdentity().isValid() ? OJSONWriter.encode(iRecord.getIdentity()) : null;
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
      json.writeAttribute(
          firstAttribute ? settings.indentLevel : 1,
          firstAttribute,
          ODocumentHelper.ATTRIBUTE_TYPE,
          "" + (char) ORecordInternal.getRecordType(record));
      if (settings.attribSameRow) firstAttribute = false;
    }
    if (settings.includeId && record.getIdentity() != null && record.getIdentity().isValid()) {
      json.writeAttribute(
          !firstAttribute ? settings.indentLevel : 1,
          firstAttribute,
          ODocumentHelper.ATTRIBUTE_RID,
          record.getIdentity().toString());
      if (settings.attribSameRow) firstAttribute = false;
    }
    if (settings.includeVer) {
      json.writeAttribute(
          firstAttribute ? settings.indentLevel : 1,
          firstAttribute,
          ODocumentHelper.ATTRIBUTE_VERSION,
          record.getVersion());
      if (settings.attribSameRow) firstAttribute = false;
    }
    if (settings.includeClazz
        && record instanceof ODocument
        && ((ODocument) record).getClassName() != null) {
      json.writeAttribute(
          firstAttribute ? settings.indentLevel : 1,
          firstAttribute,
          ODocumentHelper.ATTRIBUTE_CLASS,
          ((ODocument) record).getClassName());
      if (settings.attribSameRow) firstAttribute = false;
    }
  }

  public boolean fetchEmbeddedDocuments() {
    return settings.alwaysFetchEmbeddedDocuments;
  }

  protected void manageTypes(
      final String fieldName, final Object fieldValue, final OType fieldType) {
    // TODO: avoid `EmptyStackException`, but check root cause
    if (typesStack.empty()) {
      typesStack.push(new StringBuilder());
      OLogManager.instance()
          .debug(
              OJSONFetchContext.class,
              "Type stack in `manageTypes` null for `field` %s, `value` %s, and `type` %s.",
              fieldName,
              fieldValue,
              fieldType);
    }
    if (settings.keepTypes) {
      if (fieldValue instanceof Long) appendType(typesStack.peek(), fieldName, 'l');
      else if (fieldValue instanceof OIdentifiable) appendType(typesStack.peek(), fieldName, 'x');
      else if (fieldValue instanceof Float) appendType(typesStack.peek(), fieldName, 'f');
      else if (fieldValue instanceof Short) appendType(typesStack.peek(), fieldName, 's');
      else if (fieldValue instanceof Double) appendType(typesStack.peek(), fieldName, 'd');
      else if (fieldValue instanceof Date) appendType(typesStack.peek(), fieldName, 't');
      else if (fieldValue instanceof Byte || fieldValue instanceof byte[])
        appendType(typesStack.peek(), fieldName, 'b');
      else if (fieldValue instanceof BigDecimal) appendType(typesStack.peek(), fieldName, 'c');
      else if (fieldValue instanceof ORecordLazySet) appendType(typesStack.peek(), fieldName, 'n');
      else if (fieldValue instanceof Set<?>) appendType(typesStack.peek(), fieldName, 'e');
      else if (fieldValue instanceof ORidBag) appendType(typesStack.peek(), fieldName, 'g');
      else {
        OType t = fieldType;
        if (t == null) t = OType.getTypeByValue(fieldValue);
        if (t == OType.LINKLIST) appendType(typesStack.peek(), fieldName, 'z');
        else if (t == OType.LINKMAP) appendType(typesStack.peek(), fieldName, 'm');
        else if (t == OType.CUSTOM) appendType(typesStack.peek(), fieldName, 'u');
      }
    }
  }

  private void appendType(final StringBuilder iBuffer, final String iFieldName, final char iType) {
    if (iBuffer.length() > 0) iBuffer.append(',');
    iBuffer.append(iFieldName);
    iBuffer.append('=');
    iBuffer.append(iType);
  }
}
