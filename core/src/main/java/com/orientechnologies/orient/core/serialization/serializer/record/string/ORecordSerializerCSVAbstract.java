/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.serialization.serializer.record.string;

import com.orientechnologies.common.collection.OLazyIterator;
import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.annotation.OAfterSerialization;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.object.OLazyObjectMapInterface;
import com.orientechnologies.orient.core.db.record.*;
import com.orientechnologies.orient.core.db.record.ORecordElement.STATUS;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.entity.OEntityManagerInternal;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.serializer.ONetworkThreadLocalSerializer;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializerHelperManager;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringBuilderSerializable;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringSerializerEmbedded;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@SuppressWarnings({ "unchecked", "serial" })
public abstract class ORecordSerializerCSVAbstract extends ORecordSerializerStringAbstract {
  public static final char FIELD_VALUE_SEPARATOR = ':';
  private final boolean    preferSBTreeRIDSet    = OGlobalConfiguration.PREFER_SBTREE_SET.getValueAsBoolean();

  /**
   * Serialize the link.
   * 
   * @param buffer
   * @param iParentRecord
   * @param iLinked
   *          Can be an instance of ORID or a Record<?>
   * @return
   */
  private static OIdentifiable linkToStream(final StringBuilder buffer, final ODocument iParentRecord, Object iLinked) {
    if (iLinked == null)
      // NULL REFERENCE
      return null;

    OIdentifiable resultRid = null;
    ORID rid;

    if (iLinked instanceof ORID) {
      // JUST THE REFERENCE
      rid = (ORID) iLinked;

      if (rid.isValid() && rid.isNew()) {
        // SAVE AT THE FLY AND STORE THE NEW RID
        final ORecord record = rid.getRecord();
        if (ONetworkThreadLocalSerializer.getNetworkSerializer() != null)
          throw new ODatabaseException("Impossible save a record during network serialization");

        final ODatabaseDocument database = ODatabaseRecordThreadLocal.INSTANCE.get();
        if (record != null) {
          database.save((ORecord) record);
          rid = record.getIdentity();
        }

        resultRid = rid;
      }
    } else {
      if (iLinked instanceof String)
        iLinked = new ORecordId((String) iLinked);
      else if (!(iLinked instanceof ORecord)) {
        // NOT RECORD: TRY TO EXTRACT THE DOCUMENT IF ANY
        final String boundDocumentField = OObjectSerializerHelperManager.getInstance().getDocumentBoundField(iLinked.getClass());
        if (boundDocumentField != null)
          iLinked = OObjectSerializerHelperManager.getInstance().getFieldValue(iLinked, boundDocumentField);
      }

      if (!(iLinked instanceof OIdentifiable))
        throw new IllegalArgumentException("Invalid object received. Expected a OIdentifiable but received type="
            + iLinked.getClass().getName() + " and value=" + iLinked);

      // RECORD
      ORecord iLinkedRecord = ((OIdentifiable) iLinked).getRecord();
      rid = iLinkedRecord.getIdentity();

      if ((rid.isNew() && !rid.isTemporary()) || iLinkedRecord.isDirty()) {
        if (ONetworkThreadLocalSerializer.getNetworkSerializer() != null)
          throw new ODatabaseException("Impossible save a record during network serialization");

        final ODatabaseDocumentInternal database = ODatabaseRecordThreadLocal.INSTANCE.get();
        if (iLinkedRecord instanceof ODocument) {
          final OClass schemaClass = ODocumentInternal.getImmutableSchemaClass(((ODocument) iLinkedRecord));
          database.save(
              iLinkedRecord,
              schemaClass != null && database.getStorage().isAssigningClusterIds() ? database.getClusterNameById(schemaClass
                  .getClusterForNewInstance((ODocument) iLinkedRecord)) : null);
        } else
          // STORE THE TRAVERSED OBJECT TO KNOW THE RECORD ID. CALL THIS VERSION TO AVOID CLEAR OF STACK IN THREAD-LOCAL
          database.save(iLinkedRecord);

        final ODatabase<?> dbOwner = database.getDatabaseOwner();
        dbOwner.registerUserObjectAfterLinkSave(iLinkedRecord);

        resultRid = iLinkedRecord;
      }

      final ODatabaseDocument database = ODatabaseRecordThreadLocal.INSTANCE.get();
      if (iParentRecord != null) {
        if (!database.isRetainRecords())
          // REPLACE CURRENT RECORD WITH ITS ID: THIS SAVES A LOT OF MEMORY
          resultRid = iLinkedRecord.getIdentity();
      }
    }

    if (rid.isValid())
      rid.toString(buffer);

    return resultRid;
  }

  public Object fieldFromStream(final ORecord iSourceRecord, final OType iType, OClass iLinkedClass, OType iLinkedType,
      final String iName, final String iValue) {

    if (iValue == null)
      return null;

    switch (iType) {
    case EMBEDDEDLIST:
    case EMBEDDEDSET:
      return embeddedCollectionFromStream((ODocument) iSourceRecord, iType, iLinkedClass, iLinkedType, iValue);

    case LINKSET:
    case LINKLIST: {
      if (iValue.length() == 0)
        return null;

      // REMOVE BEGIN & END COLLECTIONS CHARACTERS IF IT'S A COLLECTION
      final String value = iValue.startsWith("[") || iValue.startsWith("<") ? iValue.substring(1, iValue.length() - 1) : iValue;

      return iType == OType.LINKLIST ? new ORecordLazyList((ODocument) iSourceRecord).setStreamedContent(new StringBuilder(value))
          : new OMVRBTreeRIDSet(iSourceRecord).fromStream(new StringBuilder(iValue));
    }

    case LINKMAP: {
      if (iValue.length() == 0)
        return null;

      // REMOVE BEGIN & END MAP CHARACTERS
      String value = iValue.substring(1, iValue.length() - 1);

      @SuppressWarnings("rawtypes")
      final Map map = new ORecordLazyMap((ODocument) iSourceRecord, ODocument.RECORD_TYPE);

      if (value.length() == 0)
        return map;

      final List<String> items = OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR, true, false);

      // EMBEDDED LITERALS
      for (String item : items) {
        if (item != null && !item.isEmpty()) {
          final List<String> entry = OStringSerializerHelper.smartSplit(item, OStringSerializerHelper.ENTRY_SEPARATOR);
          if (!entry.isEmpty()) {
            String mapValue = entry.get(1);
            if (mapValue != null && !mapValue.isEmpty())
              mapValue = mapValue.substring(1);
            map.put(fieldTypeFromStream((ODocument) iSourceRecord, OType.STRING, entry.get(0)), new ORecordId(mapValue));
          }

        }
      }
      return map;
    }

    case EMBEDDEDMAP:
      return embeddedMapFromStream((ODocument) iSourceRecord, iLinkedType, iValue, iName);

    case LINK:
      if (iValue.length() > 1) {
        int pos = iValue.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
        if (pos > -1)
          ((OMetadataInternal) ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata()).getImmutableSchemaSnapshot().getClass(
              iValue.substring(1, pos));
        else
          pos = 0;

        final String linkAsString = iValue.substring(pos + 1);
        try {
          return new ORecordId(linkAsString);
        } catch (IllegalArgumentException e) {
          OLogManager.instance().error(this, "Error on unmarshalling field '%s' of record '%s': value '%s' is not a link", iName,
              iSourceRecord, linkAsString);
          return new ORecordId();
        }
      } else
        return null;

    case EMBEDDED:
      if (iValue.length() > 2) {
        // REMOVE BEGIN & END EMBEDDED CHARACTERS
        final String value = iValue.substring(1, iValue.length() - 1);

        final Object embeddedObject = OStringSerializerEmbedded.INSTANCE.fromStream(value);
        if (embeddedObject instanceof ODocument)
          ODocumentInternal.addOwner((ODocument) embeddedObject, iSourceRecord);

        // RECORD
        return embeddedObject;
      } else
        return null;
    case LINKBAG:
      final String value = iValue.charAt(0) == OStringSerializerHelper.BAG_BEGIN ? iValue.substring(1, iValue.length() - 1)
          : iValue;
      return ORidBag.fromStream(value);
    default:
      return fieldTypeFromStream((ODocument) iSourceRecord, iType, iValue);
    }
  }

  public Map<String, Object> embeddedMapFromStream(final ODocument iSourceDocument, final OType iLinkedType, final String iValue,
      final String iName) {
    if (iValue.length() == 0)
      return null;

    // REMOVE BEGIN & END MAP CHARACTERS
    String value = iValue.substring(1, iValue.length() - 1);

    @SuppressWarnings("rawtypes")
    Map map;
    if (iLinkedType == OType.LINK || iLinkedType == OType.EMBEDDED)
      map = new ORecordLazyMap(iSourceDocument, ODocument.RECORD_TYPE);
    else
      map = new OTrackedMap<Object>(iSourceDocument);

    if (value.length() == 0)
      return map;

    final List<String> items = OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR, true, false);

    // EMBEDDED LITERALS

    if (map instanceof ORecordElement)
      ((ORecordElement) map).setInternalStatus(STATUS.UNMARSHALLING);

    for (String item : items) {
      if (item != null && !item.isEmpty()) {
        final List<String> entries = OStringSerializerHelper.smartSplit(item, OStringSerializerHelper.ENTRY_SEPARATOR, true, false);
        if (!entries.isEmpty()) {
          final Object mapValueObject;
          if (entries.size() > 1) {
            String mapValue = entries.get(1);

            final OType linkedType;

            if (iLinkedType == null)
              if (!mapValue.isEmpty()) {
                linkedType = getType(mapValue);
                if ((iName == null || iSourceDocument.fieldType(iName) == null || iSourceDocument.fieldType(iName) != OType.EMBEDDEDMAP)
                    && isConvertToLinkedMap(map, linkedType)) {
                  // CONVERT IT TO A LAZY MAP
                  map = new ORecordLazyMap(iSourceDocument, ODocument.RECORD_TYPE);
                  ((ORecordElement) map).setInternalStatus(STATUS.UNMARSHALLING);
                } else if (map instanceof ORecordLazyMap && linkedType != OType.LINK) {
                  map = new OTrackedMap<Object>(iSourceDocument, map, null);
                }
              } else
                linkedType = OType.EMBEDDED;
            else
              linkedType = iLinkedType;

            if (linkedType == OType.EMBEDDED && mapValue.length() >= 2)
              mapValue = mapValue.substring(1, mapValue.length() - 1);

            mapValueObject = fieldTypeFromStream(iSourceDocument, linkedType, mapValue);

            if (mapValueObject != null && mapValueObject instanceof ODocument)
              ODocumentInternal.addOwner((ODocument) mapValueObject, iSourceDocument);
          } else
            mapValueObject = null;

          final Object key = fieldTypeFromStream(iSourceDocument, OType.STRING, entries.get(0));
          try {
            map.put(key, mapValueObject);
          } catch (ClassCastException e) {
            throw new OSerializationException("Cannot load map because the type was not the expected: key=" + key + "(type "
                + key.getClass().toString() + "), value=" + mapValueObject + "(type " + key.getClass() + ")", e);
          }
        }

      }
    }

    if (map instanceof ORecordElement)
      ((ORecordElement) map).setInternalStatus(STATUS.LOADED);

    return map;
  }

  public void fieldToStream(final ODocument iRecord, final StringBuilder iOutput, OUserObject2RecordHandler iObjHandler,
      final OType iType, final OClass iLinkedClass, final OType iLinkedType, final String iName, final Object iValue,
      final Map<ODocument, Boolean> iMarshalledRecords, final boolean iSaveOnlyDirty) {
    if (iValue == null)
      return;

    final long timer = PROFILER.startChrono();

    switch (iType) {

    case LINK: {
      if (!(iValue instanceof OIdentifiable))
        throw new OSerializationException(
            "Found an unexpected type during marshalling of a LINK where a OIdentifiable (ORID or any Record) was expected. The string representation of the object is: "
                + iValue);

      if (!((OIdentifiable) iValue).getIdentity().isValid() && iValue instanceof ODocument && ((ODocument) iValue).isEmbedded()) {
        // WRONG: IT'S EMBEDDED!
        fieldToStream(iRecord, iOutput, iObjHandler, OType.EMBEDDED, iLinkedClass, iLinkedType, iName, iValue, iMarshalledRecords,
            iSaveOnlyDirty);
      } else {
        final Object link = linkToStream(iOutput, iRecord, iValue);
        if (link != null)
          // OVERWRITE CONTENT
          iRecord.field(iName, link);
        PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.link2string"), "Serialize link to string", timer);
      }
      break;
    }

    case LINKLIST: {
      iOutput.append(OStringSerializerHelper.LIST_BEGIN);

      if (iValue instanceof ORecordLazyList && ((ORecordLazyList) iValue).getStreamedContent() != null) {
        iOutput.append(((ORecordLazyList) iValue).getStreamedContent());
        PROFILER.updateCounter(PROFILER.getProcessMetric("serializer.record.string.linkList2string.cached"),
            "Serialize linklist to string in stream mode", +1);
      } else {
        final ORecordLazyList coll;
        final Iterator<OIdentifiable> it;
        if (iValue instanceof OMultiCollectionIterator<?>) {
          final OMultiCollectionIterator<OIdentifiable> iterator = (OMultiCollectionIterator<OIdentifiable>) iValue;
          iterator.reset();
          it = iterator;
          coll = null;
        } else if (!(iValue instanceof ORecordLazyList)) {
          // FIRST TIME: CONVERT THE ENTIRE COLLECTION
          coll = new ORecordLazyList(iRecord);

          if (iValue.getClass().isArray()) {
            Iterable<Object> iterab = OMultiValue.getMultiValueIterable(iValue);
            for (Object i : iterab) {
              coll.add((OIdentifiable) i);
            }
          } else {
            coll.addAll((Collection<? extends OIdentifiable>) iValue);
            ((Collection<? extends OIdentifiable>) iValue).clear();
          }

          iRecord.field(iName, coll);
          it = coll.rawIterator();
        } else {
          // LAZY LIST
          coll = (ORecordLazyList) iValue;
          if (coll.getStreamedContent() != null) {
            // APPEND STREAMED CONTENT
            iOutput.append(coll.getStreamedContent());
            PROFILER.updateCounter(PROFILER.getProcessMetric("serializer.record.string.linkList2string.cached"),
                "Serialize linklist to string in stream mode", +1);
            it = coll.newItemsIterator();
          } else
            it = coll.rawIterator();
        }

        if (it != null && it.hasNext()) {
          final StringBuilder buffer = new StringBuilder(128);
          for (int items = 0; it.hasNext(); items++) {
            if (items > 0)
              buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

            final OIdentifiable item = it.next();

            final OIdentifiable newRid = linkToStream(buffer, iRecord, item);
            if (newRid != null)
              ((OLazyIterator<OIdentifiable>) it).update(newRid);
          }

          if (coll != null)
            coll.convertRecords2Links();

          iOutput.append(buffer);

          // UPDATE THE STREAM
          if (coll != null)
            coll.setStreamedContent(buffer);
        }
      }

      iOutput.append(OStringSerializerHelper.LIST_END);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.linkList2string"), "Serialize linklist to string",
          timer);
      break;
    }

    case LINKSET: {
      final OStringBuilderSerializable coll;

      if (!(iValue instanceof OMVRBTreeRIDSet)) {
        if(iValue instanceof OAutoConvertToRecord)
          ((OAutoConvertToRecord)iValue).setAutoConvertToRecord(false);
        // FIRST TIME: CONVERT THE ENTIRE COLLECTION
        coll = new OMVRBTreeRIDSet(iRecord, (Collection<OIdentifiable>) iValue);

        if (!(iValue instanceof ORecordLazySet))
          iRecord.field(iName, coll);
      } else
        // LAZY SET
        coll = (OStringBuilderSerializable) iValue;

      coll.toStream(iOutput);

      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.linkSet2string"), "Serialize linkset to string",
          timer);
      break;
    }

    case LINKMAP: {
      iOutput.append(OStringSerializerHelper.MAP_BEGIN);

      Map<Object, Object> map = (Map<Object, Object>) iValue;

      // LINKED MAP
      if (map instanceof OLazyObjectMapInterface<?>)
        ((OLazyObjectMapInterface<?>) map).setConvertToRecord(false);

      boolean invalidMap = false;
      try {
        int items = 0;
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
          if (items++ > 0)
            iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

          fieldTypeToString(iOutput, OType.STRING, entry.getKey());
          iOutput.append(OStringSerializerHelper.ENTRY_SEPARATOR);
          final Object link = linkToStream(iOutput, iRecord, entry.getValue());

          if (link != null && !invalidMap)
            // IDENTITY IS CHANGED, RE-SET INTO THE COLLECTION TO RECOMPUTE THE HASH
            invalidMap = true;
        }
      } finally {
        if (map instanceof OLazyObjectMapInterface<?>) {
          ((OLazyObjectMapInterface<?>) map).setConvertToRecord(true);
        }
      }

      if (invalidMap) {
        final ORecordLazyMap newMap = new ORecordLazyMap(iRecord, ODocument.RECORD_TYPE);

        // REPLACE ALL CHANGED ITEMS
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
          newMap.put(entry.getKey(), (OIdentifiable) entry.getValue());
        }
        map.clear();
        iRecord.field(iName, newMap);
      }

      iOutput.append(OStringSerializerHelper.MAP_END);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.linkMap2string"), "Serialize linkmap to string",
          timer);
      break;
    }

    case EMBEDDED:
      if (iValue instanceof ORecord) {
        iOutput.append(OStringSerializerHelper.EMBEDDED_BEGIN);
        toString((ORecord) iValue, iOutput, null, iObjHandler, iMarshalledRecords, false, true);
        iOutput.append(OStringSerializerHelper.EMBEDDED_END);
      } else if (iValue instanceof ODocumentSerializable) {
        final ODocument doc = ((ODocumentSerializable) iValue).toDocument();
        doc.field(ODocumentSerializable.CLASS_NAME, iValue.getClass().getName());

        iOutput.append(OStringSerializerHelper.EMBEDDED_BEGIN);
        toString(doc, iOutput, null, iObjHandler, iMarshalledRecords, false, true);
        iOutput.append(OStringSerializerHelper.EMBEDDED_END);

      } else if (iValue != null)
        iOutput.append(iValue.toString());
      PROFILER
          .stopChrono(PROFILER.getProcessMetric("serializer.record.string.embed2string"), "Serialize embedded to string", timer);
      break;

    case EMBEDDEDLIST:
      embeddedCollectionToStream(null, iObjHandler, iOutput, iLinkedClass, iLinkedType, iValue, iMarshalledRecords, iSaveOnlyDirty,
          false);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.embedList2string"),
          "Serialize embeddedlist to string", timer);
      break;

    case EMBEDDEDSET:
      embeddedCollectionToStream(null, iObjHandler, iOutput, iLinkedClass, iLinkedType, iValue, iMarshalledRecords, iSaveOnlyDirty,
          true);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.embedSet2string"), "Serialize embeddedset to string",
          timer);
      break;

    case EMBEDDEDMAP: {
      embeddedMapToStream(null, iObjHandler, iOutput, iLinkedClass, iLinkedType, iValue, iMarshalledRecords, iSaveOnlyDirty);
      PROFILER.stopChrono(PROFILER.getProcessMetric("serializer.record.string.embedMap2string"), "Serialize embeddedmap to string",
          timer);
      break;
    }

    case LINKBAG: {
      iOutput.append(OStringSerializerHelper.BAG_BEGIN);
      ((ORidBag) iValue).toStream(iOutput);
      iOutput.append(OStringSerializerHelper.BAG_END);
      break;
    }

    default:
      fieldTypeToString(iOutput, iType, iValue);
    }
  }

  public void embeddedMapToStream(ODatabase<?> iDatabase, final OUserObject2RecordHandler iObjHandler, final StringBuilder iOutput,
      final OClass iLinkedClass, OType iLinkedType, final Object iValue, final Map<ODocument, Boolean> iMarshalledRecords,
      final boolean iSaveOnlyDirty) {
    iOutput.append(OStringSerializerHelper.MAP_BEGIN);

    if (iValue != null) {
      int items = 0;
      // EMBEDDED OBJECTS
      for (Entry<String, Object> o : ((Map<String, Object>) iValue).entrySet()) {
        if (items > 0)
          iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

        if (o != null) {
          fieldTypeToString(iOutput, OType.STRING, o.getKey());
          iOutput.append(OStringSerializerHelper.ENTRY_SEPARATOR);

          if (o.getValue() instanceof ODocument && ((ODocument) o.getValue()).getIdentity().isValid()) {
            fieldTypeToString(iOutput, OType.LINK, o.getValue());
          } else if (o.getValue() instanceof ORecord || o.getValue() instanceof ODocumentSerializable) {
            final ODocument record;
            if (o.getValue() instanceof ODocument)
              record = (ODocument) o.getValue();
            else if (o.getValue() instanceof ODocumentSerializable) {
              record = ((ODocumentSerializable) o.getValue()).toDocument();
              record.field(ODocumentSerializable.CLASS_NAME, o.getValue().getClass().getName());
            } else {
              if (iDatabase == null && ODatabaseRecordThreadLocal.INSTANCE.isDefined())
                iDatabase = ODatabaseRecordThreadLocal.INSTANCE.get();

              record = OObjectSerializerHelperManager.getInstance().toStream(
                  o.getValue(),
                  new ODocument(o.getValue().getClass().getSimpleName()),
                  iDatabase instanceof ODatabaseObject ? ((ODatabaseObject) iDatabase).getEntityManager()
                      : OEntityManagerInternal.INSTANCE, iLinkedClass,
                  iObjHandler != null ? iObjHandler : new OUserObject2RecordHandler() {

                    public Object getUserObjectByRecord(OIdentifiable iRecord, final String iFetchPlan) {
                      return iRecord;
                    }

                    public ORecord getRecordByUserObject(Object iPojo, boolean iCreateIfNotAvailable) {
                      return new ODocument(iLinkedClass);
                    }

                    public boolean existsUserObjectByRID(ORID iRID) {
                      return false;
                    }

                    public void registerUserObject(Object iObject, ORecord iRecord) {
                    }

                    public void registerUserObjectAfterLinkSave(ORecord iRecord) {
                    }
                  }, null, iSaveOnlyDirty);
            }
            iOutput.append(OStringSerializerHelper.EMBEDDED_BEGIN);
            toString(record, iOutput, null, iObjHandler, iMarshalledRecords, false, true);
            iOutput.append(OStringSerializerHelper.EMBEDDED_END);
          } else if (o.getValue() instanceof Set<?>) {
            // SUB SET
            fieldTypeToString(iOutput, OType.EMBEDDEDSET, o.getValue());
          } else if (o.getValue() instanceof Collection<?>) {
            // SUB LIST
            fieldTypeToString(iOutput, OType.EMBEDDEDLIST, o.getValue());
          } else if (o.getValue() instanceof Map<?, ?>) {
            // SUB MAP
            fieldTypeToString(iOutput, OType.EMBEDDEDMAP, o.getValue());
          } else {
            // EMBEDDED LITERALS
            if (iLinkedType == null && o.getValue() != null) {
              fieldTypeToString(iOutput, OType.getTypeByClass(o.getValue().getClass()), o.getValue());
            } else {
              fieldTypeToString(iOutput, iLinkedType, o.getValue());
            }
          }
        }
        items++;
      }
    }

    iOutput.append(OStringSerializerHelper.MAP_END);
  }

  public Object embeddedCollectionFromStream(final ODocument iDocument, final OType iType, OClass iLinkedClass,
      final OType iLinkedType, final String iValue) {
    if (iValue.length() == 0)
      return null;

    // REMOVE BEGIN & END COLLECTIONS CHARACTERS IF IT'S A COLLECTION
    final String value = iValue.charAt(0) == OStringSerializerHelper.LIST_BEGIN
        || iValue.charAt(0) == OStringSerializerHelper.SET_BEGIN ? iValue.substring(1, iValue.length() - 1) : iValue;

    Collection<?> coll;
    if (iLinkedType == OType.LINK) {
      if (iDocument != null)
        coll = (Collection<?>) (iType == OType.EMBEDDEDLIST ? new ORecordLazyList(iDocument).setStreamedContent(new StringBuilder(
            value)) : new OMVRBTreeRIDSet(iDocument).fromStream(new StringBuilder(value)));
      else {
        if (iType == OType.EMBEDDEDLIST)
          coll = (Collection<?>) new ORecordLazyList().setStreamedContent(new StringBuilder(value));
        else {
          final OMVRBTreeRIDSet set = new OMVRBTreeRIDSet();
          set.setAutoConvertToRecord(false);
          set.fromStream(new StringBuilder(value));
          return set;
        }
      }
    } else
      coll = iType == OType.EMBEDDEDLIST ? new OTrackedList<Object>(iDocument) : new OTrackedSet<Object>(iDocument);

    if (value.length() == 0)
      return coll;

    OType linkedType;

    if (coll instanceof ORecordElement)
      ((ORecordElement) coll).setInternalStatus(STATUS.UNMARSHALLING);

    final List<String> items = OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR, true, false);
    for (String item : items) {
      Object objectToAdd = null;
      linkedType = null;

      if (item.equals("null"))
        // NULL VALUE
        objectToAdd = null;
      else if (item.length() > 2 && item.charAt(0) == OStringSerializerHelper.EMBEDDED_BEGIN) {
        // REMOVE EMBEDDED BEGIN/END CHARS
        item = item.substring(1, item.length() - 1);

        if (!item.isEmpty()) {
          // EMBEDDED RECORD, EXTRACT THE CLASS NAME IF DIFFERENT BY THE PASSED (SUB-CLASS OR IT WAS PASSED NULL)
          iLinkedClass = OStringSerializerHelper.getRecordClassName(item, iLinkedClass);

          if (iLinkedClass != null)
            objectToAdd = fromString(item, new ODocument(iLinkedClass.getName()), null);
          else
            // EMBEDDED OBJECT
            objectToAdd = fieldTypeFromStream(iDocument, OType.EMBEDDED, item);
        }
      } else {
        if (linkedType == null) {
          final char begin = item.length() > 0 ? item.charAt(0) : OStringSerializerHelper.LINK;

          // AUTO-DETERMINE LINKED TYPE
          if (begin == OStringSerializerHelper.LINK)
            linkedType = OType.LINK;
          else
            linkedType = getType(item);

          if (linkedType == null)
            throw new IllegalArgumentException(
                "Linked type cannot be null. Probably the serialized type has not stored the type along with data");
        }

        if (iLinkedType == OType.CUSTOM)
          item = item.substring(1, item.length() - 1);

        objectToAdd = fieldTypeFromStream(iDocument, linkedType, item);
      }

      if (objectToAdd != null && objectToAdd instanceof ODocument && coll instanceof ORecordElement)
        ODocumentInternal.addOwner((ODocument) objectToAdd, (ORecordElement) coll);

      ((Collection<Object>) coll).add(objectToAdd);
    }

    if (coll instanceof ORecordElement)
      ((ORecordElement) coll).setInternalStatus(STATUS.LOADED);

    return coll;
  }

  public StringBuilder embeddedCollectionToStream(ODatabase<?> iDatabase, final OUserObject2RecordHandler iObjHandler,
      final StringBuilder iOutput, final OClass iLinkedClass, final OType iLinkedType, final Object iValue,
      final Map<ODocument, Boolean> iMarshalledRecords, final boolean iSaveOnlyDirty, final boolean iSet) {
    iOutput.append(iSet ? OStringSerializerHelper.SET_BEGIN : OStringSerializerHelper.LIST_BEGIN);

    final Iterator<Object> iterator = OMultiValue.getMultiValueIterator(iValue);

    OType linkedType = iLinkedType;

    for (int i = 0; iterator.hasNext(); ++i) {
      final Object o = iterator.next();

      if (i > 0)
        iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

      if (o == null) {
        iOutput.append("null");
        continue;
      }

      OIdentifiable id = null;
      ODocument doc = null;

      final OClass linkedClass;
      if (!(o instanceof OIdentifiable)) {
        final String fieldBound = OObjectSerializerHelperManager.getInstance().getDocumentBoundField(o.getClass());
        if (fieldBound != null) {
          OObjectSerializerHelperManager.getInstance().invokeCallback(o, null, OBeforeSerialization.class);
          doc = (ODocument) OObjectSerializerHelperManager.getInstance().getFieldValue(o, fieldBound);
          OObjectSerializerHelperManager.getInstance().invokeCallback(o, doc, OAfterSerialization.class);
          id = doc;
        } else if (iLinkedType == null)
          linkedType = OType.getTypeByClass(o.getClass());

        linkedClass = iLinkedClass;
      } else {
        id = (OIdentifiable) o;

        if (iLinkedType == null)
          // AUTO-DETERMINE LINKED TYPE
          if (id.getIdentity().isValid())
            linkedType = OType.LINK;
          else
            linkedType = OType.EMBEDDED;

        if (id instanceof ODocument) {
          doc = (ODocument) id;

          if (id.getIdentity().isTemporary()) {
            if (ONetworkThreadLocalSerializer.getNetworkSerializer() != null)
              throw new ODatabaseException("Impossible save a record during network serialization");

            doc.save();
          }

          linkedClass = ODocumentInternal.getImmutableSchemaClass(doc);
        } else
          linkedClass = null;
      }

      if (id != null && linkedType != OType.LINK)
        iOutput.append(OStringSerializerHelper.EMBEDDED_BEGIN);

      if (linkedType == OType.EMBEDDED && o instanceof OIdentifiable)
        toString((ORecord) ((OIdentifiable) o).getRecord(), iOutput, null);
      else if (linkedType != OType.LINK && (linkedClass != null || doc != null)) {
        if (id == null) {
          // EMBEDDED OBJECTS
          if (iDatabase == null && ODatabaseRecordThreadLocal.INSTANCE.isDefined())
            iDatabase = ODatabaseRecordThreadLocal.INSTANCE.get();

          id = OObjectSerializerHelperManager.getInstance().toStream(
              o,
              new ODocument(o.getClass().getSimpleName()),
              iDatabase instanceof ODatabaseObject ? ((ODatabaseObject) iDatabase).getEntityManager()
                  : OEntityManagerInternal.INSTANCE, iLinkedClass,
              iObjHandler != null ? iObjHandler : new OUserObject2RecordHandler() {
                public Object getUserObjectByRecord(OIdentifiable iRecord, final String iFetchPlan) {
                  return iRecord;
                }

                public ORecord getRecordByUserObject(Object iPojo, boolean iCreateIfNotAvailable) {
                  return new ODocument(linkedClass);
                }

                public boolean existsUserObjectByRID(ORID iRID) {
                  return false;
                }

                public void registerUserObject(Object iObject, ORecord iRecord) {
                }

                public void registerUserObjectAfterLinkSave(ORecord iRecord) {
                }
              }, null, iSaveOnlyDirty);
        }
        toString(doc, iOutput, null, iObjHandler, iMarshalledRecords, false, true);
      } else {
        // EMBEDDED LITERALS
        if (iLinkedType == null) {
          if (o != null)
            linkedType = OType.getTypeByClass(o.getClass());
        } else if (iLinkedType == OType.CUSTOM)
          iOutput.append(OStringSerializerHelper.CUSTOM_TYPE);
        fieldTypeToString(iOutput, linkedType, o);
      }

      if (id != null && linkedType != OType.LINK)
        iOutput.append(OStringSerializerHelper.EMBEDDED_END);
    }

    iOutput.append(iSet ? OStringSerializerHelper.SET_END : OStringSerializerHelper.LIST_END);
    return iOutput;
  }

  protected abstract ODocument newObject(final String iClassName);

  protected boolean isConvertToLinkedMap(Map<?, ?> map, final OType linkedType) {
    boolean convert = (linkedType == OType.LINK && !(map instanceof ORecordLazyMap));
    if (convert) {
      for (Object value : map.values())
        if (!(value instanceof OIdentifiable))
          return false;
    }
    return convert;
  }
}
