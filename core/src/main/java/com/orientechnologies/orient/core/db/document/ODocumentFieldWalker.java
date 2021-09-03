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
package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class allows to walk through all fields of single document using instance of {@link
 * ODocumentFieldVisitor} class.
 *
 * <p>Only current document and embedded documents will be walked. Which means that all embedded
 * collections will be visited too and all embedded documents which are contained in this
 * collections also will be visited.
 *
 * <p>Fields values can be updated/converted too. If method {@link
 * ODocumentFieldVisitor#visitField(OType, OType, Object)} will return new value original value will
 * be updated but returned result will not be visited by {@link ODocumentFieldVisitor} instance.
 *
 * <p>If currently processed value is collection or map of embedded documents or embedded document
 * itself then method {@link ODocumentFieldVisitor#goDeeper(OType, OType, Object)} is called, if it
 * returns false then this collection will not be visited by {@link ODocumentFieldVisitor} instance.
 *
 * <p>Fields will be visited till method {@link ODocumentFieldVisitor#goFurther(OType, OType,
 * Object, Object)} returns true.
 */
public class ODocumentFieldWalker {
  public void walkDocument(ODocument document, ODocumentFieldVisitor fieldWalker) {
    final Set<ODocument> walked =
        Collections.newSetFromMap(new IdentityHashMap<ODocument, Boolean>());
    walkDocument(document, fieldWalker, walked);
    walked.clear();
  }

  private void walkDocument(
      ODocument document, ODocumentFieldVisitor fieldWalker, Set<ODocument> walked) {
    if (walked.contains(document)) return;

    walked.add(document);
    boolean oldLazyLoad = document.isLazyLoad();
    document.setLazyLoad(false);

    final boolean updateMode = fieldWalker.updateMode();

    final OClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    for (String fieldName : document.fieldNames()) {

      final OType concreteType = document.fieldType(fieldName);
      OType fieldType = concreteType;

      OType linkedType = null;
      if (fieldType == null && clazz != null) {
        OProperty property = clazz.getProperty(fieldName);
        if (property != null) {
          fieldType = property.getType();
          linkedType = property.getLinkedType();
        }
      }

      Object fieldValue = document.field(fieldName, fieldType);
      Object newValue = fieldWalker.visitField(fieldType, linkedType, fieldValue);

      boolean updated;
      if (updateMode)
        updated =
            updateFieldValueIfChanged(document, fieldName, fieldValue, newValue, concreteType);
      else updated = false;

      // exclude cases when:
      // 1. value was updated.
      // 2. we use link types.
      // 3. document is not not embedded.
      if (!updated
          && fieldValue != null
          && !(OType.LINK.equals(fieldType)
              || OType.LINKBAG.equals(fieldType)
              || OType.LINKLIST.equals(fieldType)
              || OType.LINKSET.equals(fieldType)
              || (fieldValue instanceof ORecordLazyMultiValue))) {
        if (fieldWalker.goDeeper(fieldType, linkedType, fieldValue)) {
          if (fieldValue instanceof Map) walkMap((Map) fieldValue, fieldType, fieldWalker, walked);
          else if (fieldValue instanceof ODocument) {
            final ODocument doc = (ODocument) fieldValue;
            if (OType.EMBEDDED.equals(fieldType) || doc.isEmbedded())
              walkDocument((ODocument) fieldValue, fieldWalker);
          } else if (OMultiValue.isIterable(fieldValue))
            walkIterable(
                OMultiValue.getMultiValueIterable(fieldValue), fieldType, fieldWalker, walked);
        }
      }

      if (!fieldWalker.goFurther(fieldType, linkedType, fieldValue, newValue)) {
        document.setLazyLoad(oldLazyLoad);
        return;
      }
    }

    document.setLazyLoad(oldLazyLoad);
  }

  private void walkMap(
      Map map, OType fieldType, ODocumentFieldVisitor fieldWalker, Set<ODocument> walked) {
    for (Object value : map.values()) {
      if (value instanceof ODocument) {
        final ODocument doc = (ODocument) value;
        // only embedded documents are walked
        if (OType.EMBEDDEDMAP.equals(fieldType) || doc.isEmbedded())
          walkDocument((ODocument) value, fieldWalker, walked);
      }
    }
  }

  private void walkIterable(
      Iterable iterable,
      OType fieldType,
      ODocumentFieldVisitor fieldWalker,
      Set<ODocument> walked) {
    for (Object value : iterable) {
      if (value instanceof ODocument) {
        final ODocument doc = (ODocument) value;
        // only embedded documents are walked
        if (OType.EMBEDDEDLIST.equals(fieldType)
            || OType.EMBEDDEDSET.equals(fieldType)
            || doc.isEmbedded()) walkDocument((ODocument) value, fieldWalker, walked);
      }
    }
  }

  private boolean updateFieldValueIfChanged(
      ODocument document,
      String fieldName,
      Object fieldValue,
      Object newValue,
      OType concreteType) {
    if (fieldValue != newValue) {
      document.field(fieldName, newValue, concreteType);
      return true;
    }

    return false;
  }
}
