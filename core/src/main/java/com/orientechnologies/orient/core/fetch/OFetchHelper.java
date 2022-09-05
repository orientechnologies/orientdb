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
package com.orientechnologies.orient.core.fetch;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.fetch.json.OJSONFetchContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helper class for fetching.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @author Luca Molino
 * @author Claudio Tesoriero (giastfader @ github)
 */
public class OFetchHelper {
  public static final String DEFAULT = "*:0";
  public static final OFetchPlan DEFAULT_FETCHPLAN = new OFetchPlan(DEFAULT);

  public static OFetchPlan buildFetchPlan(final String iFetchPlan) {
    if (iFetchPlan == null) return null;

    if (DEFAULT.equals(iFetchPlan)) return DEFAULT_FETCHPLAN;

    return new OFetchPlan(iFetchPlan);
  }

  public static void fetch(
      final ORecord rootRecord,
      final Object userObject,
      final OFetchPlan fetchPlan,
      final OFetchListener listener,
      final OFetchContext context,
      final String format) {
    try {
      if (rootRecord instanceof ODocument) {
        // SCHEMA AWARE
        final ODocument record = (ODocument) rootRecord;
        final Map<ORID, Integer> parsedRecords = new HashMap<>();

        final boolean isEmbedded = record.isEmbedded() || !record.getIdentity().isPersistent();
        if (!isEmbedded) parsedRecords.put(rootRecord.getIdentity(), 0);

        if (!format.contains("shallow")) {
          processRecordRidMap(record, fetchPlan, 0, 0, -1, parsedRecords, "", context);
        }
        processRecord(
            record, userObject, fetchPlan, 0, 0, -1, parsedRecords, "", listener, context, format);
      }
    } catch (final Exception e) {
      OLogManager.instance()
          .error(null, "Fetching error on record %s", e, rootRecord.getIdentity());
    }
  }

  public static void checkFetchPlanValid(final String iFetchPlan) {

    if (iFetchPlan != null && !iFetchPlan.isEmpty()) {
      // CHECK IF THERE IS SOME FETCH-DEPTH
      final List<String> planParts = OStringSerializerHelper.split(iFetchPlan, ' ');
      if (!planParts.isEmpty()) {
        for (String planPart : planParts) {
          final List<String> parts = OStringSerializerHelper.split(planPart, ':');
          if (parts.size() != 2) {
            throw new IllegalArgumentException("Fetch plan '" + iFetchPlan + "' is invalid");
          }
        }
      } else {
        throw new IllegalArgumentException("Fetch plan '" + iFetchPlan + "' is invalid");
      }
    }
  }

  public static boolean isFetchPlanValid(final String iFetchPlan) {

    if (iFetchPlan != null && !iFetchPlan.isEmpty()) {
      // CHECK IF THERE IS SOME FETCH-DEPTH
      final List<String> planParts = OStringSerializerHelper.split(iFetchPlan, ' ');
      if (!planParts.isEmpty()) {
        for (String planPart : planParts) {
          final List<String> parts = OStringSerializerHelper.split(planPart, ':');
          if (parts.size() != 2) {
            return false;
          }
        }
      } else {
        return false;
      }
    }

    return true;
  }

  private static int getDepthLevel(
      final OFetchPlan iFetchPlan, final String iFieldPath, final int iCurrentLevel) {
    if (iFetchPlan == null) return 0;
    return iFetchPlan.getDepthLevel(iFieldPath, iCurrentLevel);
  }

  public static void processRecordRidMap(
      final ODocument record,
      final OFetchPlan iFetchPlan,
      final int iCurrentLevel,
      final int iLevelFromRoot,
      final int iFieldDepthLevel,
      final Map<ORID, Integer> parsedRecords,
      final String iFieldPathFromRoot,
      final OFetchContext iContext) {
    if (iFetchPlan == null) return;

    if (iFetchPlan == OFetchHelper.DEFAULT_FETCHPLAN) return;

    Object fieldValue;
    for (String fieldName : record.getPropertyNames()) {
      int depthLevel;
      final String fieldPath =
          !iFieldPathFromRoot.isEmpty() ? iFieldPathFromRoot + "." + fieldName : fieldName;

      depthLevel = getDepthLevel(iFetchPlan, fieldPath, iCurrentLevel);
      if (depthLevel == -2) continue;
      if (iFieldDepthLevel > -1) depthLevel = iFieldDepthLevel;

      fieldValue = ODocumentInternal.getRawProperty(record, fieldName);
      if (fieldValue == null
          || !(fieldValue instanceof OIdentifiable)
              && (!(fieldValue instanceof ORecordLazyMultiValue)
                  || !((ORecordLazyMultiValue) fieldValue).rawIterator().hasNext()
                  || !(((ORecordLazyMultiValue) fieldValue).rawIterator().next()
                      instanceof OIdentifiable))
              && (!(fieldValue instanceof Collection<?>)
                  || ((Collection<?>) fieldValue).size() == 0
                  || !(((Collection<?>) fieldValue).iterator().next() instanceof OIdentifiable))
              && (!(fieldValue.getClass().isArray())
                  || Array.getLength(fieldValue) == 0
                  || !(Array.get(fieldValue, 0) instanceof OIdentifiable))
              && (!(fieldValue instanceof OMultiCollectionIterator<?>))
              && (!(fieldValue instanceof Map<?, ?>)
                  || ((Map<?, ?>) fieldValue).size() == 0
                  || !(((Map<?, ?>) fieldValue).values().iterator().next()
                      instanceof OIdentifiable))) {
        continue;
      } else {
        try {
          final boolean isEmbedded = isEmbedded(fieldValue);
          if (iFetchPlan == null
              || (!(isEmbedded && iContext.fetchEmbeddedDocuments())
                  && !iFetchPlan.has(fieldPath, iCurrentLevel)
                  && depthLevel > -1
                  && iCurrentLevel >= depthLevel))
            // MAX DEPTH REACHED: STOP TO FETCH THIS FIELD
            continue;

          final int nextLevel = isEmbedded ? iLevelFromRoot : iLevelFromRoot + 1;

          if (fieldValue instanceof ORecordId) fieldValue = ((ORecordId) fieldValue).getRecord();

          fetchRidMap(
              record,
              iFetchPlan,
              fieldValue,
              fieldName,
              iCurrentLevel,
              nextLevel,
              iFieldDepthLevel,
              parsedRecords,
              fieldPath,
              iContext);
        } catch (Exception e) {
          OLogManager.instance()
              .error(null, "Fetching error on record %s", e, record.getIdentity());
        }
      }
    }
  }

  private static void fetchRidMap(
      final ODocument iRootRecord,
      final OFetchPlan iFetchPlan,
      final Object fieldValue,
      final String fieldName,
      final int iCurrentLevel,
      final int iLevelFromRoot,
      final int iFieldDepthLevel,
      final Map<ORID, Integer> parsedRecords,
      final String iFieldPathFromRoot,
      final OFetchContext iContext)
      throws IOException {
    if (fieldValue == null) {
      return;
    } else if (fieldValue instanceof ODocument) {
      fetchDocumentRidMap(
          iFetchPlan,
          fieldValue,
          fieldName,
          iCurrentLevel,
          iLevelFromRoot,
          iFieldDepthLevel,
          parsedRecords,
          iFieldPathFromRoot,
          iContext);
    } else if (fieldValue instanceof Iterable<?>) {
      fetchCollectionRidMap(
          iFetchPlan,
          fieldValue,
          fieldName,
          iCurrentLevel,
          iLevelFromRoot,
          iFieldDepthLevel,
          parsedRecords,
          iFieldPathFromRoot,
          iContext);
    } else if (fieldValue.getClass().isArray()) {
      fetchArrayRidMap(
          iFetchPlan,
          fieldValue,
          fieldName,
          iCurrentLevel,
          iLevelFromRoot,
          iFieldDepthLevel,
          parsedRecords,
          iFieldPathFromRoot,
          iContext);
    } else if (fieldValue instanceof Map<?, ?>) {
      fetchMapRidMap(
          iFetchPlan,
          fieldValue,
          fieldName,
          iCurrentLevel,
          iLevelFromRoot,
          iFieldDepthLevel,
          parsedRecords,
          iFieldPathFromRoot,
          iContext);
    }
  }

  private static void fetchDocumentRidMap(
      final OFetchPlan iFetchPlan,
      Object fieldValue,
      String fieldName,
      final int iCurrentLevel,
      final int iLevelFromRoot,
      final int iFieldDepthLevel,
      final Map<ORID, Integer> parsedRecords,
      final String iFieldPathFromRoot,
      final OFetchContext iContext)
      throws IOException {
    updateRidMap(
        iFetchPlan,
        (ODocument) fieldValue,
        iCurrentLevel,
        iLevelFromRoot,
        iFieldDepthLevel,
        parsedRecords,
        iFieldPathFromRoot,
        iContext);
  }

  @SuppressWarnings("unchecked")
  private static void fetchCollectionRidMap(
      final OFetchPlan iFetchPlan,
      final Object fieldValue,
      final String fieldName,
      final int iCurrentLevel,
      final int iLevelFromRoot,
      final int iFieldDepthLevel,
      final Map<ORID, Integer> parsedRecords,
      final String iFieldPathFromRoot,
      final OFetchContext iContext)
      throws IOException {
    final Iterable<OIdentifiable> linked = (Iterable<OIdentifiable>) fieldValue;
    for (OIdentifiable d : linked) {
      if (d != null) {
        // GO RECURSIVELY
        d = d.getRecord();

        updateRidMap(
            iFetchPlan,
            (ODocument) d,
            iCurrentLevel,
            iLevelFromRoot,
            iFieldDepthLevel,
            parsedRecords,
            iFieldPathFromRoot,
            iContext);
      }
    }
  }

  private static void fetchArrayRidMap(
      final OFetchPlan iFetchPlan,
      final Object fieldValue,
      final String fieldName,
      final int iCurrentLevel,
      final int iLevelFromRoot,
      final int iFieldDepthLevel,
      final Map<ORID, Integer> parsedRecords,
      final String iFieldPathFromRoot,
      final OFetchContext iContext)
      throws IOException {
    if (fieldValue instanceof ODocument[]) {
      final ODocument[] linked = (ODocument[]) fieldValue;
      for (ODocument d : linked)
        // GO RECURSIVELY
        updateRidMap(
            iFetchPlan,
            (ODocument) d,
            iCurrentLevel,
            iLevelFromRoot,
            iFieldDepthLevel,
            parsedRecords,
            iFieldPathFromRoot,
            iContext);
    }
  }

  @SuppressWarnings("unchecked")
  private static void fetchMapRidMap(
      final OFetchPlan iFetchPlan,
      Object fieldValue,
      String fieldName,
      final int iCurrentLevel,
      final int iLevelFromRoot,
      final int iFieldDepthLevel,
      final Map<ORID, Integer> parsedRecords,
      final String iFieldPathFromRoot,
      final OFetchContext iContext)
      throws IOException {
    final Map<String, ODocument> linked = (Map<String, ODocument>) fieldValue;
    for (ODocument d : (linked).values())
      // GO RECURSIVELY
      updateRidMap(
          iFetchPlan,
          (ODocument) d,
          iCurrentLevel,
          iLevelFromRoot,
          iFieldDepthLevel,
          parsedRecords,
          iFieldPathFromRoot,
          iContext);
  }

  private static void updateRidMap(
      final OFetchPlan iFetchPlan,
      final ODocument fieldValue,
      final int iCurrentLevel,
      final int iLevelFromRoot,
      final int iFieldDepthLevel,
      final Map<ORID, Integer> parsedRecords,
      final String iFieldPathFromRoot,
      final OFetchContext iContext)
      throws IOException {
    if (fieldValue == null) return;

    final Integer fetchedLevel = parsedRecords.get(fieldValue.getIdentity());
    int currentLevel = iCurrentLevel + 1;
    int fieldDepthLevel = iFieldDepthLevel;
    if (iFetchPlan != null && iFetchPlan.has(iFieldPathFromRoot, iCurrentLevel)) {
      currentLevel = 1;
      fieldDepthLevel = iFetchPlan.getDepthLevel(iFieldPathFromRoot, iCurrentLevel);
    }

    final boolean isEmbedded = isEmbedded(fieldValue);

    if (isEmbedded || fetchedLevel == null) {
      if (!isEmbedded) parsedRecords.put(fieldValue.getIdentity(), iLevelFromRoot);

      processRecordRidMap(
          fieldValue,
          iFetchPlan,
          currentLevel,
          iLevelFromRoot,
          fieldDepthLevel,
          parsedRecords,
          iFieldPathFromRoot,
          iContext);
    }
  }

  private static void processRecord(
      final ODocument record,
      final Object userObject,
      final OFetchPlan fetchPlan,
      final int currentLevel,
      final int levelFromRoot,
      final int fieldDepthLevel,
      final Map<ORID, Integer> parsedRecords,
      final String fieldPathFromRoot,
      final OFetchListener fetchListener,
      final OFetchContext fetchContext,
      final String format) {
    if (record == null) {
      return;
    }
    if (!fetchListener.requireFieldProcessing() && fetchPlan == OFetchHelper.DEFAULT_FETCHPLAN) {
      return;
    }
    final ORecordSerializerJSON.FormatSettings settings =
        new ORecordSerializerJSON.FormatSettings(format);

    // Pre-process to gather fieldTypes
    fetchContext.onBeforeFetch(record);
    if (settings.keepTypes) {
      for (final String fieldName : record.getPropertyNames()) {
        processFieldTypes(
            record,
            userObject,
            fetchPlan,
            currentLevel,
            fieldDepthLevel,
            fieldPathFromRoot,
            fetchContext,
            format,
            new HashSet<>(),
            fieldName);
      }
      fetchContext.onAfterFetch(record);
    }

    fetchContext.onBeforeFetch(record);
    final Set<String> toRemove = new HashSet<>();
    for (final String fieldName : record.getPropertyNames()) {
      process(
          record,
          userObject,
          fetchPlan,
          currentLevel,
          levelFromRoot,
          fieldDepthLevel,
          parsedRecords,
          fieldPathFromRoot,
          fetchListener,
          fetchContext,
          format,
          toRemove,
          fieldName);
    }
    for (final String fieldName : toRemove) {
      fetchListener.skipStandardField(record, fieldName, fetchContext, userObject, format);
    }
    if (settings.keepTypes) {
      fetchContext.onAfterFetch(record);
    }
  }

  private static void processFieldTypes(
      ODocument record,
      Object userObject,
      OFetchPlan fetchPlan,
      int currentLevel,
      int fieldDepthLevel,
      String fieldPathFromRoot,
      OFetchContext fetchContext,
      String format,
      Set<String> toRemove,
      String fieldName) {
    Object fieldValue;
    final String fieldPath =
        !fieldPathFromRoot.isEmpty() ? fieldPathFromRoot + "." + fieldName : fieldName;
    int depthLevel;
    depthLevel = getDepthLevel(fetchPlan, fieldPath, currentLevel);
    if (depthLevel == -2) {
      toRemove.add(fieldName);
      return;
    }
    if (fieldDepthLevel > -1) depthLevel = fieldDepthLevel;

    fieldValue = ODocumentInternal.getRawProperty(record, fieldName);
    final OType fieldType = record.fieldType(fieldName);
    boolean fetch =
        !format.contains("shallow")
            && (!(fieldValue instanceof OIdentifiable)
                || depthLevel == -1
                || currentLevel <= depthLevel
                || (fetchPlan != null && fetchPlan.has(fieldPath, currentLevel)));
    final boolean isEmbedded = isEmbedded(fieldValue);

    if (!fetch && isEmbedded && fetchContext.fetchEmbeddedDocuments()) {
      // EMBEDDED, GO DEEPER
      fetch = true;
    }

    if (format.contains("shallow")
        || fieldValue == null
        || (!fetch && fieldValue instanceof OIdentifiable)
        || !(fieldValue instanceof OIdentifiable)
            && (!(fieldValue instanceof ORecordLazyMultiValue)
                || !((ORecordLazyMultiValue) fieldValue).rawIterator().hasNext()
                || !(((ORecordLazyMultiValue) fieldValue).rawIterator().next()
                    instanceof OIdentifiable))
            && (!(fieldValue.getClass().isArray())
                || Array.getLength(fieldValue) == 0
                || !(Array.get(fieldValue, 0) instanceof OIdentifiable))
            && !containsIdentifiers(fieldValue)) {
      fetchContext.onBeforeStandardField(fieldValue, fieldName, userObject, fieldType);
    }
  }

  private static void process(
      final ODocument record,
      final Object userObject,
      final OFetchPlan fetchPlan,
      final int currentLevel,
      final int levelFromRoot,
      final int fieldDepthLevel,
      final Map<ORID, Integer> parsedRecords,
      final String fieldPathFromRoot,
      final OFetchListener fetchListener,
      final OFetchContext fetchContext,
      final String format,
      final Set<String> toRemove,
      final String fieldName) {
    final ORecordSerializerJSON.FormatSettings settings =
        new ORecordSerializerJSON.FormatSettings(format);

    Object fieldValue;
    final String fieldPath =
        !fieldPathFromRoot.isEmpty() ? fieldPathFromRoot + "." + fieldName : fieldName;
    int depthLevel;
    depthLevel = getDepthLevel(fetchPlan, fieldPath, currentLevel);
    if (depthLevel == -2) {
      toRemove.add(fieldName);
      return;
    }
    if (fieldDepthLevel > -1) depthLevel = fieldDepthLevel;

    fieldValue = ODocumentInternal.getRawProperty(record, fieldName);
    final OType fieldType = record.fieldType(fieldName);
    boolean fetch =
        !format.contains("shallow")
            && (!(fieldValue instanceof OIdentifiable)
                || depthLevel == -1
                || currentLevel <= depthLevel
                || (fetchPlan != null && fetchPlan.has(fieldPath, currentLevel)));
    final boolean isEmbedded = isEmbedded(fieldValue);

    if (!fetch && isEmbedded && fetchContext.fetchEmbeddedDocuments()) {
      // EMBEDDED, GO DEEPER
      fetch = true;
    }

    if (format.contains("shallow")
        || fieldValue == null
        || (!fetch && fieldValue instanceof OIdentifiable)
        || !(fieldValue instanceof OIdentifiable)
            && (!(fieldValue instanceof ORecordLazyMultiValue)
                || !((ORecordLazyMultiValue) fieldValue).rawIterator().hasNext()
                || !(((ORecordLazyMultiValue) fieldValue).rawIterator().next()
                    instanceof OIdentifiable))
            && (!(fieldValue.getClass().isArray())
                || Array.getLength(fieldValue) == 0
                || !(Array.get(fieldValue, 0) instanceof OIdentifiable))
            && !containsIdentifiers(fieldValue)) {
      fetchContext.onBeforeStandardField(fieldValue, fieldName, userObject, fieldType);
      fetchListener.processStandardField(
          record, fieldValue, fieldName, fetchContext, userObject, format, fieldType);
      fetchContext.onAfterStandardField(fieldValue, fieldName, userObject, fieldType);
    } else {
      try {
        if (fetch) {
          final int nextLevel = isEmbedded ? levelFromRoot : levelFromRoot + 1;
          fetch(
              record,
              userObject,
              fetchPlan,
              fieldValue,
              fieldName,
              currentLevel,
              nextLevel,
              fieldDepthLevel,
              parsedRecords,
              depthLevel,
              fieldPath,
              fetchListener,
              fetchContext,
              settings);
        }
      } catch (final Exception e) {
        OLogManager.instance().error(null, "Fetching error on record %s", e, record.getIdentity());
      }
    }
  }

  private static boolean containsIdentifiers(Object fieldValue) {
    if (!OMultiValue.isMultiValue(fieldValue)) {
      return false;
    }
    for (Object item : OMultiValue.getMultiValueIterable(fieldValue)) {
      if (item instanceof OIdentifiable) {
        return true;
      }
      if (containsIdentifiers(item)) {
        return true;
      }
    }
    return false;
  }

  public static boolean isEmbedded(Object fieldValue) {
    boolean isEmbedded =
        fieldValue instanceof ODocument
            && (((ODocument) fieldValue).isEmbedded()
                || !((ODocument) fieldValue).getIdentity().isPersistent());

    // ridbag can contain only edges no embedded documents are allowed.
    if (fieldValue instanceof ORidBag) return false;
    if (!isEmbedded) {
      try {
        final Object f = OMultiValue.getFirstValue(fieldValue);
        isEmbedded =
            f != null
                && (f instanceof ODocument
                    && (((ODocument) f).isEmbedded()
                        || !((ODocument) f).getIdentity().isPersistent()));
      } catch (Exception e) {
        OLogManager.instance().error(OFetchHelper.class, "", e);
        // IGNORE IT
      }
    }
    return isEmbedded;
  }

  private static void fetch(
      final ODocument iRootRecord,
      final Object iUserObject,
      final OFetchPlan iFetchPlan,
      final Object fieldValue,
      final String fieldName,
      final int iCurrentLevel,
      final int iLevelFromRoot,
      final int iFieldDepthLevel,
      final Map<ORID, Integer> parsedRecords,
      final int depthLevel,
      final String iFieldPathFromRoot,
      final OFetchListener iListener,
      final OFetchContext iContext,
      final ORecordSerializerJSON.FormatSettings settings)
      throws IOException {
    int currentLevel = iCurrentLevel + 1;
    int fieldDepthLevel = iFieldDepthLevel;
    if (iFetchPlan != null && iFetchPlan.has(iFieldPathFromRoot, iCurrentLevel)) {
      currentLevel = 0;
      fieldDepthLevel = iFetchPlan.getDepthLevel(iFieldPathFromRoot, iCurrentLevel);
    }

    if (fieldValue == null) {
      iListener.processStandardField(iRootRecord, null, fieldName, iContext, iUserObject, "", null);
    } else if (fieldValue instanceof OIdentifiable) {
      fetchDocument(
          iRootRecord,
          iUserObject,
          iFetchPlan,
          (OIdentifiable) fieldValue,
          fieldName,
          currentLevel,
          iLevelFromRoot,
          fieldDepthLevel,
          parsedRecords,
          iFieldPathFromRoot,
          iListener,
          iContext,
          settings);
    } else if (fieldValue instanceof Map<?, ?>) {
      fetchMap(
          iRootRecord,
          iUserObject,
          iFetchPlan,
          fieldValue,
          fieldName,
          currentLevel,
          iLevelFromRoot,
          fieldDepthLevel,
          parsedRecords,
          iFieldPathFromRoot,
          iListener,
          iContext,
          settings);
    } else if (OMultiValue.isMultiValue(fieldValue)) {
      fetchCollection(
          iRootRecord,
          iUserObject,
          iFetchPlan,
          fieldValue,
          fieldName,
          currentLevel,
          iLevelFromRoot,
          fieldDepthLevel,
          parsedRecords,
          iFieldPathFromRoot,
          iListener,
          iContext,
          settings);
    } else if (fieldValue.getClass().isArray()) {
      fetchArray(
          iRootRecord,
          iUserObject,
          iFetchPlan,
          fieldValue,
          fieldName,
          currentLevel,
          iLevelFromRoot,
          fieldDepthLevel,
          parsedRecords,
          iFieldPathFromRoot,
          iListener,
          iContext,
          settings);
    }
  }

  @SuppressWarnings("unchecked")
  private static void fetchMap(
      final ODocument iRootRecord,
      final Object iUserObject,
      final OFetchPlan iFetchPlan,
      Object fieldValue,
      String fieldName,
      final int iCurrentLevel,
      final int iLevelFromRoot,
      final int iFieldDepthLevel,
      final Map<ORID, Integer> parsedRecords,
      final String iFieldPathFromRoot,
      final OFetchListener iListener,
      final OFetchContext iContext,
      final ORecordSerializerJSON.FormatSettings settings)
      throws IOException {
    final Map<String, ODocument> linked = (Map<String, ODocument>) fieldValue;
    iContext.onBeforeMap(iRootRecord, fieldName, iUserObject);

    for (Object key : linked.keySet()) {
      final Object o = linked.get(key);

      if (o instanceof OIdentifiable) {
        ORecord r = null;
        try {
          r = ((OIdentifiable) o).getRecord();
        } catch (ORecordNotFoundException ignore) {
        }
        if (r != null) {
          if (r instanceof ODocument) {
            // GO RECURSIVELY
            final ODocument d = (ODocument) r;
            final Integer fieldDepthLevel = parsedRecords.get(d.getIdentity());
            if (!d.getIdentity().isValid()
                || (fieldDepthLevel != null && fieldDepthLevel.intValue() == iLevelFromRoot)) {
              removeParsedFromMap(parsedRecords, d);
              iContext.onBeforeDocument(iRootRecord, d, key.toString(), iUserObject);
              final Object userObject =
                  iListener.fetchLinkedMapEntry(
                      iRootRecord, iUserObject, fieldName, key.toString(), d, iContext);
              processRecord(
                  d,
                  userObject,
                  iFetchPlan,
                  iCurrentLevel,
                  iLevelFromRoot,
                  iFieldDepthLevel,
                  parsedRecords,
                  iFieldPathFromRoot,
                  iListener,
                  iContext,
                  getTypesFormat(settings.keepTypes)); // ""
              iContext.onAfterDocument(iRootRecord, d, key.toString(), iUserObject);
            } else {
              iListener.parseLinked(iRootRecord, d, iUserObject, key.toString(), iContext);
            }
          } else iListener.parseLinked(iRootRecord, r, iUserObject, key.toString(), iContext);

        } else {
          iListener.processStandardField(
              iRootRecord, o, key.toString(), iContext, iUserObject, "", null);
        }
      } else if (o instanceof Map) {
        fetchMap(
            iRootRecord,
            iUserObject,
            iFetchPlan,
            o,
            key.toString(),
            iCurrentLevel + 1,
            iLevelFromRoot,
            iFieldDepthLevel,
            parsedRecords,
            iFieldPathFromRoot,
            iListener,
            iContext,
            settings);
      } else if (OMultiValue.isMultiValue(o)) {
        fetchCollection(
            iRootRecord,
            iUserObject,
            iFetchPlan,
            o,
            key.toString(),
            iCurrentLevel + 1,
            iLevelFromRoot,
            iFieldDepthLevel,
            parsedRecords,
            iFieldPathFromRoot,
            iListener,
            iContext,
            settings);
      } else
        iListener.processStandardField(
            iRootRecord, o, key.toString(), iContext, iUserObject, "", null);
    }
    iContext.onAfterMap(iRootRecord, fieldName, iUserObject);
  }

  private static void fetchArray(
      final ODocument rootRecord,
      final Object iUserObject,
      final OFetchPlan iFetchPlan,
      Object fieldValue,
      String fieldName,
      final int iCurrentLevel,
      final int iLevelFromRoot,
      final int iFieldDepthLevel,
      final Map<ORID, Integer> parsedRecords,
      final String iFieldPathFromRoot,
      final OFetchListener iListener,
      final OFetchContext context,
      ORecordSerializerJSON.FormatSettings settings) {
    if (fieldValue instanceof ODocument[]) {
      final ODocument[] linked = (ODocument[]) fieldValue;
      context.onBeforeArray(rootRecord, fieldName, iUserObject, linked);
      for (final ODocument document : linked) {
        // GO RECURSIVELY
        final Integer fieldDepthLevel = parsedRecords.get(document.getIdentity());
        if (!document.getIdentity().isValid()
            || (fieldDepthLevel != null && fieldDepthLevel.intValue() == iLevelFromRoot)) {
          removeParsedFromMap(parsedRecords, document);
          context.onBeforeDocument(rootRecord, document, fieldName, iUserObject);
          final Object userObject =
              iListener.fetchLinked(rootRecord, iUserObject, fieldName, document, context);
          processRecord(
              document,
              userObject,
              iFetchPlan,
              iCurrentLevel,
              iLevelFromRoot,
              iFieldDepthLevel,
              parsedRecords,
              iFieldPathFromRoot,
              iListener,
              context,
              getTypesFormat(settings.keepTypes)); // ""
          context.onAfterDocument(rootRecord, document, fieldName, iUserObject);
        } else {
          iListener.parseLinkedCollectionValue(
              rootRecord, document, iUserObject, fieldName, context);
        }
      }
      context.onAfterArray(rootRecord, fieldName, iUserObject);
    } else {
      iListener.processStandardField(
          rootRecord, fieldValue, fieldName, context, iUserObject, "", null);
    }
  }

  @SuppressWarnings("unchecked")
  private static void fetchCollection(
      final ODocument iRootRecord,
      final Object iUserObject,
      final OFetchPlan iFetchPlan,
      final Object fieldValue,
      final String fieldName,
      final int iCurrentLevel,
      final int iLevelFromRoot,
      final int iFieldDepthLevel,
      final Map<ORID, Integer> parsedRecords,
      final String iFieldPathFromRoot,
      final OFetchListener iListener,
      final OFetchContext context,
      final ORecordSerializerJSON.FormatSettings settings)
      throws IOException {
    final Iterable<?> linked;
    if (fieldValue instanceof Iterable<?> || fieldValue instanceof ORidBag) {
      linked = (Iterable<OIdentifiable>) fieldValue;
      context.onBeforeCollection(iRootRecord, fieldName, iUserObject, linked);
    } else if (fieldValue.getClass().isArray()) {
      linked = OMultiValue.getMultiValueIterable(fieldValue, false);
      context.onBeforeCollection(iRootRecord, fieldName, iUserObject, linked);
    } else if (fieldValue instanceof Map<?, ?>) {
      linked = ((Map<?, ?>) fieldValue).values();
      context.onBeforeMap(iRootRecord, fieldName, iUserObject);
    } else throw new IllegalStateException("Unrecognized type: " + fieldValue.getClass());

    final Iterator<?> iter;
    if (linked instanceof ORecordLazyMultiValue) {
      iter = ((ORecordLazyMultiValue) linked).rawIterator();
    } else {
      iter = linked.iterator();
    }

    try {
      while (iter.hasNext()) {
        final Object recordLazyMultiValue = iter.next();
        if (recordLazyMultiValue == null) continue;

        if (recordLazyMultiValue instanceof OIdentifiable) {
          OIdentifiable identifiable = (OIdentifiable) recordLazyMultiValue;

          // GO RECURSIVELY
          final Integer fieldDepthLevel = parsedRecords.get(identifiable.getIdentity());
          if (!identifiable.getIdentity().isPersistent()
              || (fieldDepthLevel != null && fieldDepthLevel.intValue() == iLevelFromRoot)) {
            removeParsedFromMap(parsedRecords, identifiable);
            identifiable = identifiable.getRecord();

            if (identifiable == null)
              iListener.processStandardField(
                  null, identifiable, null, context, iUserObject, "", null);
            else if (!(identifiable instanceof ODocument)) {
              iListener.processStandardField(
                  null, identifiable, fieldName, context, iUserObject, "", null);
            } else {
              context.onBeforeDocument(
                  iRootRecord, (ODocument) identifiable, fieldName, iUserObject);
              final Object userObject =
                  iListener.fetchLinkedCollectionValue(
                      iRootRecord, iUserObject, fieldName, (ODocument) identifiable, context);
              processRecord(
                  (ODocument) identifiable,
                  userObject,
                  iFetchPlan,
                  iCurrentLevel,
                  iLevelFromRoot,
                  iFieldDepthLevel,
                  parsedRecords,
                  iFieldPathFromRoot,
                  iListener,
                  context,
                  getTypesFormat(settings.keepTypes)); // ""
              context.onAfterDocument(
                  iRootRecord, (ODocument) identifiable, fieldName, iUserObject);
            }
          } else {
            iListener.parseLinkedCollectionValue(
                iRootRecord, identifiable, iUserObject, fieldName, context);
          }
        } else if (recordLazyMultiValue instanceof Map<?, ?>) {
          fetchMap(
              iRootRecord,
              iUserObject,
              iFetchPlan,
              recordLazyMultiValue,
              null,
              iCurrentLevel + 1,
              iLevelFromRoot,
              iFieldDepthLevel,
              parsedRecords,
              iFieldPathFromRoot,
              iListener,
              context,
              settings);
        } else if (OMultiValue.isMultiValue(recordLazyMultiValue)) {
          fetchCollection(
              iRootRecord,
              iUserObject,
              iFetchPlan,
              recordLazyMultiValue,
              null,
              iCurrentLevel + 1,
              iLevelFromRoot,
              iFieldDepthLevel,
              parsedRecords,
              iFieldPathFromRoot,
              iListener,
              context,
              settings);
        } else if ((recordLazyMultiValue instanceof String
                || recordLazyMultiValue instanceof Number
                || recordLazyMultiValue instanceof Boolean)
            && context instanceof OJSONFetchContext) {
          ((OJSONFetchContext) context).getJsonWriter().writeValue(0, false, recordLazyMultiValue);
        }
      }
    } finally {
      if (fieldValue instanceof Iterable<?> || fieldValue instanceof ORidBag)
        context.onAfterCollection(iRootRecord, fieldName, iUserObject);
      else if (fieldValue.getClass().isArray())
        context.onAfterCollection(iRootRecord, fieldName, iUserObject);
      else if (fieldValue instanceof Map<?, ?>)
        context.onAfterMap(iRootRecord, fieldName, iUserObject);
    }
  }

  private static void fetchDocument(
      final ODocument iRootRecord,
      final Object iUserObject,
      final OFetchPlan iFetchPlan,
      final OIdentifiable fieldValue,
      final String fieldName,
      final int iCurrentLevel,
      final int iLevelFromRoot,
      final int iFieldDepthLevel,
      final Map<ORID, Integer> parsedRecords,
      final String iFieldPathFromRoot,
      final OFetchListener iListener,
      final OFetchContext iContext,
      final ORecordSerializerJSON.FormatSettings settings) {
    if (fieldValue instanceof ORID && !((ORID) fieldValue).isValid()) {
      // RID NULL: TREAT AS "NULL" VALUE
      iContext.onBeforeStandardField(fieldValue, fieldName, iRootRecord, null);
      iListener.parseLinked(iRootRecord, fieldValue, iUserObject, fieldName, iContext);
      iContext.onAfterStandardField(fieldValue, fieldName, iRootRecord, null);
      return;
    }

    final Integer fieldDepthLevel = parsedRecords.get(fieldValue.getIdentity());
    if (!fieldValue.getIdentity().isValid()
        || (fieldDepthLevel != null && fieldDepthLevel.intValue() == iLevelFromRoot)) {
      removeParsedFromMap(parsedRecords, fieldValue);
      final ODocument linked = fieldValue.getRecord();
      if (linked == null) return;

      iContext.onBeforeDocument(iRootRecord, linked, fieldName, iUserObject);
      Object userObject =
          iListener.fetchLinked(iRootRecord, iUserObject, fieldName, linked, iContext);
      processRecord(
          linked,
          userObject,
          iFetchPlan,
          iCurrentLevel,
          iLevelFromRoot,
          iFieldDepthLevel,
          parsedRecords,
          iFieldPathFromRoot,
          iListener,
          iContext,
          getTypesFormat(settings.keepTypes)); // ""
      iContext.onAfterDocument(iRootRecord, linked, fieldName, iUserObject);
    } else {
      iContext.onBeforeStandardField(fieldValue, fieldName, iRootRecord, null);
      iListener.parseLinked(iRootRecord, fieldValue, iUserObject, fieldName, iContext);
      iContext.onAfterStandardField(fieldValue, fieldName, iRootRecord, null);
    }
  }

  private static String getTypesFormat(final boolean keepTypes) {
    final StringBuilder sb = new StringBuilder();
    if (keepTypes) sb.append("keepTypes");
    return sb.toString();
  }

  protected static void removeParsedFromMap(
      final Map<ORID, Integer> parsedRecords, OIdentifiable d) {
    parsedRecords.remove(d.getIdentity());
  }
}
