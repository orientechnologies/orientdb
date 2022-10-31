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

package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OPropertyAccess;
import com.orientechnologies.orient.core.metadata.security.OPropertyEncryption;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class ODocumentInternal {

  public static void convertAllMultiValuesToTrackedVersions(ODocument document) {
    document.convertAllMultiValuesToTrackedVersions();
  }

  public static void addOwner(ODocument oDocument, ORecordElement iOwner) {
    oDocument.addOwner(iOwner);
  }

  public static void removeOwner(ODocument oDocument, ORecordElement iOwner) {
    oDocument.removeOwner(iOwner);
  }

  public static void rawField(
      final ODocument oDocument,
      final String iFieldName,
      final Object iFieldValue,
      final OType iFieldType) {
    oDocument.rawField(iFieldName, iFieldValue, iFieldType);
  }

  public static boolean rawContainsField(final ODocument oDocument, final String iFiledName) {
    return oDocument.rawContainsField(iFiledName);
  }

  public static OImmutableClass getImmutableSchemaClass(
      final ODatabaseDocumentInternal database, final ODocument oDocument) {
    if (oDocument == null) {
      return null;
    }
    return oDocument.getImmutableSchemaClass(database);
  }

  public static OImmutableClass getImmutableSchemaClass(final ODocument oDocument) {
    if (oDocument == null) {
      return null;
    }
    return oDocument.getImmutableSchemaClass();
  }

  public static OImmutableSchema getImmutableSchema(final ODocument oDocument) {
    if (oDocument == null) {
      return null;
    }
    return oDocument.getImmutableSchema();
  }

  public static OGlobalProperty getGlobalPropertyById(final ODocument oDocument, final int id) {
    return oDocument.getGlobalPropertyById(id);
  }

  public static void fillClassNameIfNeeded(final ODocument oDocument, String className) {
    oDocument.fillClassIfNeed(className);
  }

  public static Set<Entry<String, ODocumentEntry>> rawEntries(final ODocument document) {
    return document.getRawEntries();
  }

  public static ODocumentEntry rawEntry(final ODocument document, String propertyName) {
    return document.fields.get(propertyName);
  }

  public static List<Entry<String, ODocumentEntry>> filteredEntries(final ODocument document) {
    return document.getFilteredEntries();
  }

  public static void clearTrackData(final ODocument document) {
    document.clearTrackData();
  }

  public static void checkClass(ODocument doc, ODatabaseDocumentInternal database) {
    doc.checkClass(database);
  }

  public static void autoConvertValueToClass(ODatabaseDocumentInternal database, ODocument doc) {
    doc.autoConvertFieldsToClass(database);
  }

  public static Object getRawProperty(ODocument doc, String propertyName) {
    if (doc == null) {
      return null;
    }
    return doc.getRawProperty(propertyName);
  }

  public static void setPropertyAccess(ODocument doc, OPropertyAccess propertyAccess) {
    doc.propertyAccess = propertyAccess;
  }

  public static OPropertyAccess getPropertyAccess(ODocument doc) {
    return doc.propertyAccess;
  }

  public static void setPropertyEncryption(ODocument doc, OPropertyEncryption propertyEncryption) {
    doc.propertyEncryption = propertyEncryption;
  }

  public static OPropertyEncryption getPropertyEncryption(ODocument doc) {
    return doc.propertyEncryption;
  }

  public static void clearTransactionTrackData(ODocument doc) {
    doc.clearTransactionTrackData();
  }

  public static Iterator<String> iteratePropertieNames(ODocument doc) {
    return doc.calculatePropertyNames().iterator();
  }
}
