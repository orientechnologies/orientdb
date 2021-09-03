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

import com.orientechnologies.common.collection.OLazyIterator;
import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OAutoConvertToRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
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
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringBuilderSerializable;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringSerializerEmbedded;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@SuppressWarnings({"unchecked", "serial"})
public abstract class ORecordSerializerCSVAbstract extends ORecordSerializerStringAbstract {
  public static final char FIELD_VALUE_SEPARATOR = ':';

  /**
   * Serialize the link.
   *
   * @param buffer
   * @param iParentRecord
   * @param iLinked Can be an instance of ORID or a Record<?>
   * @return
   */
  private static OIdentifiable linkToStream(
      final StringBuilder buffer, final ODocument iParentRecord, Object iLinked) {
    if (iLinked == null)
      // NULL REFERENCE
      return null;

    OIdentifiable resultRid = null;
    ORID rid;

    if (iLinked instanceof ORID) {
      // JUST THE REFERENCE
      rid = (ORID) iLinked;

      assert rid.getIdentity().isValid() || ODatabaseRecordThreadLocal.instance().get().isRemote()
          : "Impossible to serialize invalid link " + rid.getIdentity();
      resultRid = rid;
    } else {
      if (iLinked instanceof String) iLinked = new ORecordId((String) iLinked);

      if (!(iLinked instanceof OIdentifiable))
        throw new IllegalArgumentException(
            "Invalid object received. Expected a OIdentifiable but received type="
                + iLinked.getClass().getName()
                + " and value="
                + iLinked);

      // RECORD
      ORecord iLinkedRecord = ((OIdentifiable) iLinked).getRecord();
      rid = iLinkedRecord.getIdentity();

      assert rid.getIdentity().isValid() || ODatabaseRecordThreadLocal.instance().get().isRemote()
          : "Impossible to serialize invalid link " + rid.getIdentity();

      final ODatabaseDocument database = ODatabaseRecordThreadLocal.instance().get();
      if (iParentRecord != null) {
        if (!database.isRetainRecords())
          // REPLACE CURRENT RECORD WITH ITS ID: THIS SAVES A LOT OF MEMORY
          resultRid = iLinkedRecord.getIdentity();
      }
    }

    if (rid.isValid()) rid.toString(buffer);

    return resultRid;
  }

  public Object fieldFromStream(
      final ORecord iSourceRecord,
      final OType iType,
      OClass iLinkedClass,
      OType iLinkedType,
      final String iName,
      final String iValue) {

    if (iValue == null) return null;

    switch (iType) {
      case EMBEDDEDLIST:
      case EMBEDDEDSET:
        return embeddedCollectionFromStream(
            (ODocument) iSourceRecord, iType, iLinkedClass, iLinkedType, iValue);

      case LINKSET:
      case LINKLIST:
        {
          if (iValue.length() == 0) return null;

          // REMOVE BEGIN & END COLLECTIONS CHARACTERS IF IT'S A COLLECTION
          final String value =
              iValue.startsWith("[") || iValue.startsWith("<")
                  ? iValue.substring(1, iValue.length() - 1)
                  : iValue;

          if (iType == OType.LINKLIST) {
            return unserializeList((ODocument) iSourceRecord, value);
          } else {
            return unserializeSet((ODocument) iSourceRecord, value);
          }
        }

      case LINKMAP:
        {
          if (iValue.length() == 0) return null;

          // REMOVE BEGIN & END MAP CHARACTERS
          String value = iValue.substring(1, iValue.length() - 1);

          @SuppressWarnings("rawtypes")
          final Map map = new ORecordLazyMap((ODocument) iSourceRecord, ODocument.RECORD_TYPE);

          if (value.length() == 0) return map;

          final List<String> items =
              OStringSerializerHelper.smartSplit(
                  value, OStringSerializerHelper.RECORD_SEPARATOR, true, false);

          // EMBEDDED LITERALS
          for (String item : items) {
            if (item != null && !item.isEmpty()) {
              final List<String> entry =
                  OStringSerializerHelper.smartSplit(item, OStringSerializerHelper.ENTRY_SEPARATOR);
              if (!entry.isEmpty()) {
                String mapValue = entry.get(1);
                if (mapValue != null && !mapValue.isEmpty()) mapValue = mapValue.substring(1);
                map.put(
                    fieldTypeFromStream((ODocument) iSourceRecord, OType.STRING, entry.get(0)),
                    new ORecordId(mapValue));
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
            ((OMetadataInternal) ODatabaseRecordThreadLocal.instance().get().getMetadata())
                .getImmutableSchemaSnapshot()
                .getClass(iValue.substring(1, pos));
          else pos = 0;

          final String linkAsString = iValue.substring(pos + 1);
          try {
            return new ORecordId(linkAsString);
          } catch (IllegalArgumentException e) {
            OLogManager.instance()
                .error(
                    this,
                    "Error on unmarshalling field '%s' of record '%s': value '%s' is not a link",
                    e,
                    iName,
                    iSourceRecord,
                    linkAsString);
            return new ORecordId();
          }
        } else return null;

      case EMBEDDED:
        if (iValue.length() > 2) {
          // REMOVE BEGIN & END EMBEDDED CHARACTERS
          final String value = iValue.substring(1, iValue.length() - 1);

          final Object embeddedObject = OStringSerializerEmbedded.INSTANCE.fromStream(value);
          if (embeddedObject instanceof ODocument)
            ODocumentInternal.addOwner((ODocument) embeddedObject, iSourceRecord);

          // RECORD
          return embeddedObject;
        } else return null;
      case LINKBAG:
        final String value =
            iValue.charAt(0) == OStringSerializerHelper.BAG_BEGIN
                ? iValue.substring(1, iValue.length() - 1)
                : iValue;
        return ORidBag.fromStream(value);
      default:
        return fieldTypeFromStream((ODocument) iSourceRecord, iType, iValue);
    }
  }

  public Map<String, Object> embeddedMapFromStream(
      final ODocument iSourceDocument,
      final OType iLinkedType,
      final String iValue,
      final String iName) {
    if (iValue.length() == 0) return null;

    // REMOVE BEGIN & END MAP CHARACTERS
    String value = iValue.substring(1, iValue.length() - 1);

    @SuppressWarnings("rawtypes")
    Map map;
    if (iLinkedType == OType.LINK || iLinkedType == OType.EMBEDDED)
      map = new ORecordLazyMap(iSourceDocument, ODocument.RECORD_TYPE);
    else map = new OTrackedMap<Object>(iSourceDocument);

    if (value.length() == 0) return map;

    final List<String> items =
        OStringSerializerHelper.smartSplit(
            value, OStringSerializerHelper.RECORD_SEPARATOR, true, false);

    // EMBEDDED LITERALS

    for (String item : items) {
      if (item != null && !item.isEmpty()) {
        final List<String> entries =
            OStringSerializerHelper.smartSplit(
                item, OStringSerializerHelper.ENTRY_SEPARATOR, true, false);
        if (!entries.isEmpty()) {
          final Object mapValueObject;
          if (entries.size() > 1) {
            String mapValue = entries.get(1);

            final OType linkedType;

            if (iLinkedType == null)
              if (!mapValue.isEmpty()) {
                linkedType = getType(mapValue);
                if ((iName == null
                        || iSourceDocument.fieldType(iName) == null
                        || iSourceDocument.fieldType(iName) != OType.EMBEDDEDMAP)
                    && isConvertToLinkedMap(map, linkedType)) {
                  // CONVERT IT TO A LAZY MAP
                  map = new ORecordLazyMap(iSourceDocument, ODocument.RECORD_TYPE);
                } else if (map instanceof ORecordLazyMap && linkedType != OType.LINK) {
                  map = new OTrackedMap<Object>(iSourceDocument, map, null);
                }
              } else linkedType = OType.EMBEDDED;
            else linkedType = iLinkedType;

            if (linkedType == OType.EMBEDDED && mapValue.length() >= 2)
              mapValue = mapValue.substring(1, mapValue.length() - 1);

            mapValueObject = fieldTypeFromStream(iSourceDocument, linkedType, mapValue);

            if (mapValueObject != null && mapValueObject instanceof ODocument)
              ODocumentInternal.addOwner((ODocument) mapValueObject, iSourceDocument);
          } else mapValueObject = null;

          final Object key = fieldTypeFromStream(iSourceDocument, OType.STRING, entries.get(0));
          try {
            map.put(key, mapValueObject);
          } catch (ClassCastException e) {
            throw OException.wrapException(
                new OSerializationException(
                    "Cannot load map because the type was not the expected: key="
                        + key
                        + "(type "
                        + key.getClass().toString()
                        + "), value="
                        + mapValueObject
                        + "(type "
                        + key.getClass()
                        + ")"),
                e);
          }
        }
      }
    }

    return map;
  }

  public void fieldToStream(
      final ODocument iRecord,
      final StringBuilder iOutput,
      final OType iType,
      final OClass iLinkedClass,
      final OType iLinkedType,
      final String iName,
      final Object iValue,
      final boolean iSaveOnlyDirty) {
    if (iValue == null) return;

    final long timer = PROFILER.startChrono();

    switch (iType) {
      case LINK:
        {
          if (!(iValue instanceof OIdentifiable))
            throw new OSerializationException(
                "Found an unexpected type during marshalling of a LINK where a OIdentifiable (ORID or any Record) was expected. The string representation of the object is: "
                    + iValue);

          if (!((OIdentifiable) iValue).getIdentity().isValid()
              && iValue instanceof ODocument
              && ((ODocument) iValue).isEmbedded()) {
            // WRONG: IT'S EMBEDDED!
            fieldToStream(
                iRecord,
                iOutput,
                OType.EMBEDDED,
                iLinkedClass,
                iLinkedType,
                iName,
                iValue,
                iSaveOnlyDirty);
          } else {
            final Object link = linkToStream(iOutput, iRecord, iValue);
            if (link != null)
              // OVERWRITE CONTENT
              iRecord.field(iName, link);
            PROFILER.stopChrono(
                PROFILER.getProcessMetric("serializer.record.string.link2string"),
                "Serialize link to string",
                timer);
          }
          break;
        }

      case LINKLIST:
        {
          iOutput.append(OStringSerializerHelper.LIST_BEGIN);
          final ORecordLazyList coll;
          final Iterator<OIdentifiable> it;
          if (iValue instanceof OMultiCollectionIterator<?>) {
            final OMultiCollectionIterator<OIdentifiable> iterator =
                (OMultiCollectionIterator<OIdentifiable>) iValue;
            iterator.reset();
            it = iterator;
            coll = null;
          } else if (!(iValue instanceof ORecordLazyList)) {
            // FIRST TIME: CONVERT THE ENTIRE COLLECTION
            coll = new ORecordLazyList(iRecord);

            if (iValue.getClass().isArray()) {
              Iterable<Object> iterab = OMultiValue.getMultiValueIterable(iValue, false);
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
            it = coll.rawIterator();
          }

          if (it != null && it.hasNext()) {
            final StringBuilder buffer = new StringBuilder(128);
            for (int items = 0; it.hasNext(); items++) {
              if (items > 0) buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

              final OIdentifiable item = it.next();

              final OIdentifiable newRid = linkToStream(buffer, iRecord, item);
              if (newRid != null) ((OLazyIterator<OIdentifiable>) it).update(newRid);
            }

            if (coll != null) coll.convertRecords2Links();

            iOutput.append(buffer);
          }

          iOutput.append(OStringSerializerHelper.LIST_END);
          PROFILER.stopChrono(
              PROFILER.getProcessMetric("serializer.record.string.linkList2string"),
              "Serialize linklist to string",
              timer);
          break;
        }

      case LINKSET:
        {
          if (!(iValue instanceof OStringBuilderSerializable)) {
            if (iValue instanceof OAutoConvertToRecord)
              ((OAutoConvertToRecord) iValue).setAutoConvertToRecord(false);

            final Collection<OIdentifiable> coll;

            // FIRST TIME: CONVERT THE ENTIRE COLLECTION
            if (!(iValue instanceof ORecordLazySet)) {
              final ORecordLazySet set = new ORecordLazySet(iRecord);
              set.addAll((Collection<OIdentifiable>) iValue);
              iRecord.field(iName, set);
              coll = set;
            } else coll = (Collection<OIdentifiable>) iValue;

            serializeSet(coll, iOutput);

          } else {
            // LAZY SET
            final OStringBuilderSerializable coll = (OStringBuilderSerializable) iValue;
            coll.toStream(iOutput);
          }

          PROFILER.stopChrono(
              PROFILER.getProcessMetric("serializer.record.string.linkSet2string"),
              "Serialize linkset to string",
              timer);
          break;
        }

      case LINKMAP:
        {
          iOutput.append(OStringSerializerHelper.MAP_BEGIN);

          Map<Object, Object> map = (Map<Object, Object>) iValue;

          boolean invalidMap = false;
          int items = 0;
          for (Map.Entry<Object, Object> entry : map.entrySet()) {
            if (items++ > 0) iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

            fieldTypeToString(iOutput, OType.STRING, entry.getKey());
            iOutput.append(OStringSerializerHelper.ENTRY_SEPARATOR);
            final Object link = linkToStream(iOutput, iRecord, entry.getValue());

            if (link != null && !invalidMap)
              // IDENTITY IS CHANGED, RE-SET INTO THE COLLECTION TO RECOMPUTE THE HASH
              invalidMap = true;
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
          PROFILER.stopChrono(
              PROFILER.getProcessMetric("serializer.record.string.linkMap2string"),
              "Serialize linkmap to string",
              timer);
          break;
        }

      case EMBEDDED:
        if (iValue instanceof ORecord) {
          iOutput.append(OStringSerializerHelper.EMBEDDED_BEGIN);
          toString((ORecord) iValue, iOutput, null, true);
          iOutput.append(OStringSerializerHelper.EMBEDDED_END);
        } else if (iValue instanceof ODocumentSerializable) {
          final ODocument doc = ((ODocumentSerializable) iValue).toDocument();
          doc.field(ODocumentSerializable.CLASS_NAME, iValue.getClass().getName());

          iOutput.append(OStringSerializerHelper.EMBEDDED_BEGIN);
          toString(doc, iOutput, null, true);
          iOutput.append(OStringSerializerHelper.EMBEDDED_END);

        } else if (iValue != null) iOutput.append(iValue.toString());
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.embed2string"),
            "Serialize embedded to string",
            timer);
        break;

      case EMBEDDEDLIST:
        embeddedCollectionToStream(
            null, iOutput, iLinkedClass, iLinkedType, iValue, iSaveOnlyDirty, false);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.embedList2string"),
            "Serialize embeddedlist to string",
            timer);
        break;

      case EMBEDDEDSET:
        embeddedCollectionToStream(
            null, iOutput, iLinkedClass, iLinkedType, iValue, iSaveOnlyDirty, true);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.embedSet2string"),
            "Serialize embeddedset to string",
            timer);
        break;

      case EMBEDDEDMAP:
        {
          embeddedMapToStream(null, iOutput, iLinkedClass, iLinkedType, iValue, iSaveOnlyDirty);
          PROFILER.stopChrono(
              PROFILER.getProcessMetric("serializer.record.string.embedMap2string"),
              "Serialize embeddedmap to string",
              timer);
          break;
        }

      case LINKBAG:
        {
          iOutput.append(OStringSerializerHelper.BAG_BEGIN);
          ((ORidBag) iValue).toStream(iOutput);
          iOutput.append(OStringSerializerHelper.BAG_END);
          break;
        }

      default:
        fieldTypeToString(iOutput, iType, iValue);
    }
  }

  public void embeddedMapToStream(
      ODatabase<?> iDatabase,
      final StringBuilder iOutput,
      final OClass iLinkedClass,
      OType iLinkedType,
      final Object iValue,
      final boolean iSaveOnlyDirty) {
    iOutput.append(OStringSerializerHelper.MAP_BEGIN);

    if (iValue != null) {
      int items = 0;
      // EMBEDDED OBJECTS
      for (Entry<String, Object> o : ((Map<String, Object>) iValue).entrySet()) {
        if (items > 0) iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

        if (o != null) {
          fieldTypeToString(iOutput, OType.STRING, o.getKey());
          iOutput.append(OStringSerializerHelper.ENTRY_SEPARATOR);

          if (o.getValue() instanceof ODocument
              && ((ODocument) o.getValue()).getIdentity().isValid()) {
            fieldTypeToString(iOutput, OType.LINK, o.getValue());
          } else if (o.getValue() instanceof ORecord
              || o.getValue() instanceof ODocumentSerializable) {
            final ODocument record;
            if (o.getValue() instanceof ODocument) record = (ODocument) o.getValue();
            else if (o.getValue() instanceof ODocumentSerializable) {
              record = ((ODocumentSerializable) o.getValue()).toDocument();
              record.field(ODocumentSerializable.CLASS_NAME, o.getValue().getClass().getName());
            } else {
              record = null;
            }
            iOutput.append(OStringSerializerHelper.EMBEDDED_BEGIN);
            toString(record, iOutput, null, true);
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
              fieldTypeToString(
                  iOutput, OType.getTypeByClass(o.getValue().getClass()), o.getValue());
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

  public Object embeddedCollectionFromStream(
      final ODocument iDocument,
      final OType iType,
      OClass iLinkedClass,
      final OType iLinkedType,
      final String iValue) {
    if (iValue.length() == 0) return null;

    // REMOVE BEGIN & END COLLECTIONS CHARACTERS IF IT'S A COLLECTION
    final String value;
    if (iValue.charAt(0) == OStringSerializerHelper.LIST_BEGIN
        || iValue.charAt(0) == OStringSerializerHelper.SET_BEGIN) {
      value = iValue.substring(1, iValue.length() - 1);
    } else {
      value = iValue;
    }

    Collection<?> coll;
    if (iLinkedType == OType.LINK) {
      if (iDocument != null)
        coll =
            (iType == OType.EMBEDDEDLIST
                ? unserializeList(iDocument, value)
                : unserializeSet(iDocument, value));
      else {
        if (iType == OType.EMBEDDEDLIST) coll = unserializeList(iDocument, value);
        else {
          return unserializeSet(iDocument, value);
        }
      }
    } else
      coll =
          iType == OType.EMBEDDEDLIST
              ? new OTrackedList<Object>(iDocument)
              : new OTrackedSet<Object>(iDocument);

    if (value.length() == 0) return coll;

    OType linkedType;

    final List<String> items =
        OStringSerializerHelper.smartSplit(
            value, OStringSerializerHelper.RECORD_SEPARATOR, true, false);
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
          // EMBEDDED RECORD, EXTRACT THE CLASS NAME IF DIFFERENT BY THE PASSED (SUB-CLASS OR IT WAS
          // PASSED NULL)
          iLinkedClass = OStringSerializerHelper.getRecordClassName(item, iLinkedClass);

          if (iLinkedClass != null) {
            ODocument doc = new ODocument();
            objectToAdd = fromString(item, doc, null);
            ODocumentInternal.fillClassNameIfNeeded(doc, iLinkedClass.getName());
          } else
            // EMBEDDED OBJECT
            objectToAdd = fieldTypeFromStream(iDocument, OType.EMBEDDED, item);
        }
      } else {
        if (linkedType == null) {
          final char begin = item.length() > 0 ? item.charAt(0) : OStringSerializerHelper.LINK;

          // AUTO-DETERMINE LINKED TYPE
          if (begin == OStringSerializerHelper.LINK) linkedType = OType.LINK;
          else linkedType = getType(item);

          if (linkedType == null)
            throw new IllegalArgumentException(
                "Linked type cannot be null. Probably the serialized type has not stored the type along with data");
        }

        if (iLinkedType == OType.CUSTOM) item = item.substring(1, item.length() - 1);

        objectToAdd = fieldTypeFromStream(iDocument, linkedType, item);
      }

      if (objectToAdd != null && objectToAdd instanceof ODocument && coll instanceof ORecordElement)
        ODocumentInternal.addOwner((ODocument) objectToAdd, (ORecordElement) coll);

      ((Collection<Object>) coll).add(objectToAdd);
    }

    return coll;
  }

  public StringBuilder embeddedCollectionToStream(
      ODatabase<?> iDatabase,
      final StringBuilder iOutput,
      final OClass iLinkedClass,
      final OType iLinkedType,
      final Object iValue,
      final boolean iSaveOnlyDirty,
      final boolean iSet) {
    iOutput.append(iSet ? OStringSerializerHelper.SET_BEGIN : OStringSerializerHelper.LIST_BEGIN);

    final Iterator<Object> iterator = OMultiValue.getMultiValueIterator(iValue, false);

    OType linkedType = iLinkedType;

    for (int i = 0; iterator.hasNext(); ++i) {
      final Object o = iterator.next();

      if (i > 0) iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

      if (o == null) {
        iOutput.append("null");
        continue;
      }

      OIdentifiable id = null;
      ODocument doc = null;

      final OClass linkedClass;
      if (!(o instanceof OIdentifiable)) {
        if (iLinkedType == null) linkedType = OType.getTypeByClass(o.getClass());

        linkedClass = iLinkedClass;
      } else {
        id = (OIdentifiable) o;

        if (iLinkedType == null)
          // AUTO-DETERMINE LINKED TYPE
          if (id.getIdentity().isValid()) linkedType = OType.LINK;
          else linkedType = OType.EMBEDDED;

        if (id instanceof ODocument) {
          doc = (ODocument) id;

          if (doc.hasOwners()) linkedType = OType.EMBEDDED;

          assert linkedType == OType.EMBEDDED
                  || id.getIdentity().isValid()
                  || ODatabaseRecordThreadLocal.instance().get().isRemote()
              : "Impossible to serialize invalid link " + id.getIdentity();

          linkedClass = ODocumentInternal.getImmutableSchemaClass(doc);
        } else linkedClass = null;
      }

      if (id != null && linkedType != OType.LINK)
        iOutput.append(OStringSerializerHelper.EMBEDDED_BEGIN);

      if (linkedType == OType.EMBEDDED && o instanceof OIdentifiable)
        toString((ORecord) ((OIdentifiable) o).getRecord(), iOutput, null);
      else if (linkedType != OType.LINK && (linkedClass != null || doc != null)) {
        toString(doc, iOutput, null, true);
      } else {
        // EMBEDDED LITERALS
        if (iLinkedType == null) {
          if (o != null) linkedType = OType.getTypeByClass(o.getClass());
        } else if (iLinkedType == OType.CUSTOM) iOutput.append(OStringSerializerHelper.CUSTOM_TYPE);
        fieldTypeToString(iOutput, linkedType, o);
      }

      if (id != null && linkedType != OType.LINK)
        iOutput.append(OStringSerializerHelper.EMBEDDED_END);
    }

    iOutput.append(iSet ? OStringSerializerHelper.SET_END : OStringSerializerHelper.LIST_END);
    return iOutput;
  }

  protected boolean isConvertToLinkedMap(Map<?, ?> map, final OType linkedType) {
    boolean convert = (linkedType == OType.LINK && !(map instanceof ORecordLazyMap));
    if (convert) {
      for (Object value : map.values()) if (!(value instanceof OIdentifiable)) return false;
    }
    return convert;
  }

  private void serializeSet(final Collection<OIdentifiable> coll, final StringBuilder iOutput) {
    iOutput.append(OStringSerializerHelper.SET_BEGIN);
    int i = 0;
    for (OIdentifiable rid : coll) {
      if (i++ > 0) iOutput.append(',');

      iOutput.append(rid.getIdentity().toString());
    }
    iOutput.append(OStringSerializerHelper.SET_END);
  }

  private ORecordLazyList unserializeList(final ODocument iSourceRecord, final String value) {
    final ORecordLazyList coll = new ORecordLazyList(iSourceRecord);
    final List<String> items =
        OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR);
    for (String item : items) {
      if (item.length() == 0) coll.add(new ORecordId());
      else {
        if (item.startsWith("#")) coll.add(new ORecordId(item));
        else {
          final ORecord doc = fromString(item);
          if (doc instanceof ODocument) ODocumentInternal.addOwner((ODocument) doc, iSourceRecord);

          coll.add(doc);
        }
      }
    }
    return coll;
  }

  private ORecordLazySet unserializeSet(final ODocument iSourceRecord, final String value) {
    final ORecordLazySet coll = new ORecordLazySet(iSourceRecord);
    final List<String> items =
        OStringSerializerHelper.smartSplit(value, OStringSerializerHelper.RECORD_SEPARATOR);
    for (String item : items) {
      if (item.length() == 0) coll.add(new ORecordId());
      else {
        if (item.startsWith("#")) coll.add(new ORecordId(item));
        else {
          final ORecord doc = fromString(item);
          if (doc instanceof ODocument) ODocumentInternal.addOwner((ODocument) doc, iSourceRecord);

          coll.add(doc);
        }
      }
    }
    return coll;
  }
}
