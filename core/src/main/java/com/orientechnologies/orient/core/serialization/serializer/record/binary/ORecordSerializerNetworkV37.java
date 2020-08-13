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

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OUUIDSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEmbedded;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.ChangeSerializationHelper;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TimeZone;
import java.util.UUID;

public class ORecordSerializerNetworkV37 implements ORecordSerializer {

  public static final String NAME = "onet_ser_v37";
  private static final String CHARSET_UTF_8 = "UTF-8";
  protected static final ORecordId NULL_RECORD_ID = new ORecordId(-2, ORID.CLUSTER_POS_INVALID);
  private static final long MILLISEC_PER_DAY = 86400000;
  public static final ORecordSerializerNetworkV37 INSTANCE = new ORecordSerializerNetworkV37();

  public ORecordSerializerNetworkV37() {}

  public void deserializePartial(
      final ODocument document, final BytesContainer bytes, final String[] iFields) {
    final String className = readString(bytes);
    if (className.length() != 0) ODocumentInternal.fillClassNameIfNeeded(document, className);

    String fieldName;
    OType type;
    int size = OVarIntSerializer.readAsInteger(bytes);

    int matched = 0;
    while ((size--) > 0) {
      fieldName = readString(bytes);
      type = readOType(bytes);
      Object value;
      if (type == null) {
        value = null;
      } else {
        value = deserializeValue(bytes, type, document);
      }
      if (ODocumentInternal.rawContainsField(document, fieldName)) {
        continue;
      }
      ODocumentInternal.rawField(document, fieldName, value, type);

      for (String field : iFields) {
        if (field.equals(fieldName)) {
          matched++;
        }
      }
      if (matched == iFields.length) {
        break;
      }
    }
  }

  public void deserialize(final ODocument document, final BytesContainer bytes) {
    final String className = readString(bytes);
    if (className.length() != 0) ODocumentInternal.fillClassNameIfNeeded(document, className);

    String fieldName;
    OType type;
    Object value;
    int size = OVarIntSerializer.readAsInteger(bytes);
    while ((size--) > 0) {
      // PARSE FIELD NAME
      fieldName = readString(bytes);
      type = readOType(bytes);
      if (type == null) {
        value = null;
      } else {
        value = deserializeValue(bytes, type, document);
      }
      if (ODocumentInternal.rawContainsField(document, fieldName)) {
        continue;
      }
      ODocumentInternal.rawField(document, fieldName, value, type);
    }

    ORecordInternal.clearSource(document);
  }

  public void serialize(final ODocument document, final BytesContainer bytes) {
    serializeClass(document, bytes);
    final Collection<Entry<String, ODocumentEntry>> fields = fetchEntries(document);
    OVarIntSerializer.write(bytes, fields.size());
    for (Entry<String, ODocumentEntry> entry : fields) {
      ODocumentEntry docEntry = entry.getValue();
      writeString(bytes, entry.getKey());
      final Object value = docEntry.value;
      if (value != null) {
        final OType type = getFieldType(docEntry);
        if (type == null) {
          throw new OSerializationException(
              "Impossible serialize value of type "
                  + value.getClass()
                  + " with the Result binary serializer");
        }
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(bytes, value, type, getLinkedType(document, type, entry.getKey()));
      } else {
        writeOType(bytes, bytes.alloc(1), null);
      }
    }
  }

  protected Collection<Entry<String, ODocumentEntry>> fetchEntries(ODocument document) {
    return ODocumentInternal.filteredEntries(document);
  }

  public String[] getFieldNames(ODocument reference, final BytesContainer bytes) {
    // SKIP CLASS NAME
    final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
    bytes.skip(classNameLen);

    final List<String> result = new ArrayList<String>();

    int size = OVarIntSerializer.readAsInteger(bytes);
    String fieldName;
    OType type;
    while ((size--) > 0) {
      fieldName = readString(bytes);
      type = readOType(bytes);
      if (type != null) {
        deserializeValue(bytes, type, new ODocument());
      }
      result.add(fieldName);
    }

    return result.toArray(new String[result.size()]);
  }

  protected OClass serializeClass(final ODocument document, final BytesContainer bytes) {
    final OClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    String name = null;
    if (clazz != null) name = clazz.getName();
    if (name == null) name = document.getClassName();

    if (name != null) writeString(bytes, name);
    else writeEmptyString(bytes);
    return clazz;
  }

  protected OType readOType(final BytesContainer bytes) {
    return HelperClasses.readType(bytes);
  }

  private void writeOType(BytesContainer bytes, int pos, OType type) {
    if (type == null) {
      bytes.bytes[pos] = (byte) -1;
    } else {
      bytes.bytes[pos] = (byte) type.getId();
    }
  }

  public byte[] serializeValue(Object value, OType type) {
    BytesContainer bytes = new BytesContainer();
    serializeValue(bytes, value, type, null);
    return bytes.fitBytes();
  }

  public Object deserializeValue(byte[] val, OType type) {
    BytesContainer bytes = new BytesContainer(val);
    return deserializeValue(bytes, type, null);
  }

  public Object deserializeValue(BytesContainer bytes, OType type, ORecordElement owner) {
    Object value = null;
    switch (type) {
      case INTEGER:
        value = OVarIntSerializer.readAsInteger(bytes);
        break;
      case LONG:
        value = OVarIntSerializer.readAsLong(bytes);
        break;
      case SHORT:
        value = OVarIntSerializer.readAsShort(bytes);
        break;
      case STRING:
        value = readString(bytes);
        break;
      case DOUBLE:
        value = Double.longBitsToDouble(readLong(bytes));
        break;
      case FLOAT:
        value = Float.intBitsToFloat(readInteger(bytes));
        break;
      case BYTE:
        value = readByte(bytes);
        break;
      case BOOLEAN:
        value = readByte(bytes) == 1;
        break;
      case DATETIME:
        value = new Date(OVarIntSerializer.readAsLong(bytes));
        break;
      case DATE:
        long savedTime = OVarIntSerializer.readAsLong(bytes) * MILLISEC_PER_DAY;
        savedTime =
            convertDayToTimezone(
                TimeZone.getTimeZone("GMT"), ODateHelper.getDatabaseTimeZone(), savedTime);
        value = new Date(savedTime);
        break;
      case EMBEDDED:
        value = new ODocumentEmbedded();
        deserialize((ODocument) value, bytes);
        if (((ODocument) value).containsField(ODocumentSerializable.CLASS_NAME)) {
          String className = ((ODocument) value).field(ODocumentSerializable.CLASS_NAME);
          try {
            Class<?> clazz = Class.forName(className);
            ODocumentSerializable newValue = (ODocumentSerializable) clazz.newInstance();
            newValue.fromDocument((ODocument) value);
            value = newValue;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        } else ODocumentInternal.addOwner((ODocument) value, owner);

        break;
      case EMBEDDEDSET:
        value = readEmbeddedSet(bytes, owner);
        break;
      case EMBEDDEDLIST:
        value = readEmbeddedList(bytes, owner);
        break;
      case LINKSET:
        value = readLinkSet(bytes, owner);
        break;
      case LINKLIST:
        value = readLinkList(bytes, owner);
        break;
      case BINARY:
        value = readBinary(bytes);
        break;
      case LINK:
        value = readOptimizedLink(bytes);
        break;
      case LINKMAP:
        value = readLinkMap(bytes, owner);
        break;
      case EMBEDDEDMAP:
        value = readEmbeddedMap(bytes, owner);
        break;
      case DECIMAL:
        value = ODecimalSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
        bytes.skip(ODecimalSerializer.INSTANCE.getObjectSize(bytes.bytes, bytes.offset));
        break;
      case LINKBAG:
        ORidBag bag = readRidBag(bytes);
        bag.setOwner(owner);
        value = bag;
        break;
      case TRANSIENT:
        break;
      case CUSTOM:
        try {
          String className = readString(bytes);
          Class<?> clazz = Class.forName(className);
          OSerializableStream stream = (OSerializableStream) clazz.newInstance();
          stream.fromStream(readBinary(bytes));
          if (stream instanceof OSerializableWrapper)
            value = ((OSerializableWrapper) stream).getSerializable();
          else value = stream;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        break;
      case ANY:
        break;
    }
    return value;
  }

  private void writeRidBag(BytesContainer bytes, ORidBag bag) {
    final OSBTreeCollectionManager sbTreeCollectionManager =
        ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager();
    UUID uuid = null;
    if (sbTreeCollectionManager != null) uuid = sbTreeCollectionManager.listenForChanges(bag);
    if (uuid == null) uuid = new UUID(-1, -1);
    int uuidPos = bytes.alloc(OUUIDSerializer.UUID_SIZE);
    OUUIDSerializer.INSTANCE.serialize(uuid, bytes.bytes, uuidPos);
    if (bag.isToSerializeEmbedded()) {
      int pos = bytes.alloc(1);
      bytes.bytes[pos] = 1;
      OVarIntSerializer.write(bytes, bag.size());
      Iterator<OIdentifiable> iterator = bag.rawIterator();
      while (iterator.hasNext()) {
        OIdentifiable itemValue = iterator.next();
        if (itemValue == null) writeNullLink(bytes);
        else writeOptimizedLink(bytes, itemValue);
      }
    } else {
      int pos = bytes.alloc(1);
      bytes.bytes[pos] = 2;
      OBonsaiCollectionPointer pointer = bag.getPointer();
      if (pointer == null) pointer = OBonsaiCollectionPointer.INVALID;
      OVarIntSerializer.write(bytes, pointer.getFileId());
      OVarIntSerializer.write(bytes, pointer.getRootPointer().getPageIndex());
      OVarIntSerializer.write(bytes, pointer.getRootPointer().getPageOffset());
      OVarIntSerializer.write(bytes, -1);
      NavigableMap<OIdentifiable, Change> changes = bag.getChanges();
      if (changes != null) {
        OVarIntSerializer.write(bytes, changes.size());
        for (Map.Entry<OIdentifiable, Change> change : changes.entrySet()) {
          writeOptimizedLink(bytes, change.getKey());
          int posAll = bytes.alloc(1);
          bytes.bytes[posAll] = change.getValue().getType();
          OVarIntSerializer.write(bytes, change.getValue().getValue());
        }
      } else {
        OVarIntSerializer.write(bytes, 0);
      }
    }
  }

  protected ORidBag readRidBag(BytesContainer bytes) {
    UUID uuid = OUUIDSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.skip(OUUIDSerializer.UUID_SIZE);
    if (uuid.getMostSignificantBits() == -1 && uuid.getLeastSignificantBits() == -1) uuid = null;
    byte b = bytes.bytes[bytes.offset];
    bytes.skip(1);
    if (b == 1) {
      ORidBag bag = new ORidBag(uuid);
      // enable tracking due to timeline issue, which must not be NULL (i.e. tracker.isEnabled()).
      bag.enableTracking(null);
      int size = OVarIntSerializer.readAsInteger(bytes);
      for (int i = 0; i < size; i++) {
        OIdentifiable id = readOptimizedLink(bytes);
        if (id.equals(NULL_RECORD_ID)) bag.add(null);
        else bag.add(id);
      }
      bag.disableTracking(null);
      bag.transactionClear();
      return bag;
    } else {
      long fileId = OVarIntSerializer.readAsLong(bytes);
      long pageIndex = OVarIntSerializer.readAsLong(bytes);
      int pageOffset = OVarIntSerializer.readAsInteger(bytes);
      int bagSize = OVarIntSerializer.readAsInteger(bytes);

      Map<OIdentifiable, Change> changes = new HashMap<>();
      int size = OVarIntSerializer.readAsInteger(bytes);
      while (size-- > 0) {
        OIdentifiable link = readOptimizedLink(bytes);
        byte type = bytes.bytes[bytes.offset];
        bytes.skip(1);
        int change = OVarIntSerializer.readAsInteger(bytes);
        changes.put(link, ChangeSerializationHelper.createChangeInstance(type, change));
      }

      OBonsaiCollectionPointer pointer = null;
      if (fileId != -1)
        pointer =
            new OBonsaiCollectionPointer(fileId, new OBonsaiBucketPointer(pageIndex, pageOffset));
      return new ORidBag(pointer, changes, uuid);
    }
  }

  private byte[] readBinary(BytesContainer bytes) {
    int n = OVarIntSerializer.readAsInteger(bytes);
    byte[] newValue = new byte[n];
    System.arraycopy(bytes.bytes, bytes.offset, newValue, 0, newValue.length);
    bytes.skip(n);
    return newValue;
  }

  private Map<Object, OIdentifiable> readLinkMap(
      final BytesContainer bytes, final ORecordElement owner) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    ORecordLazyMap result = new ORecordLazyMap(owner);
    while ((size--) > 0) {
      OType keyType = readOType(bytes);
      Object key = deserializeValue(bytes, keyType, result);
      OIdentifiable value = readOptimizedLink(bytes);
      if (value.equals(NULL_RECORD_ID)) result.putInternal(key, null);
      else result.putInternal(key, value);
    }
    return result;
  }

  private Object readEmbeddedMap(final BytesContainer bytes, final ORecordElement owner) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    final OTrackedMap result = new OTrackedMap<Object>(owner);
    while ((size--) > 0) {
      String key = readString(bytes);
      OType valType = readOType(bytes);
      Object value = null;
      if (valType != null) {
        value = deserializeValue(bytes, valType, result);
      }
      result.putInternal(key, value);
    }
    return result;
  }

  private Collection<OIdentifiable> readLinkList(BytesContainer bytes, ORecordElement owner) {
    ORecordLazyList found = new ORecordLazyList(owner);
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      OIdentifiable id = readOptimizedLink(bytes);
      if (id.equals(NULL_RECORD_ID)) found.addInternal(null);
      else found.addInternal(id);
    }
    return found;
  }

  private Collection<OIdentifiable> readLinkSet(BytesContainer bytes, ORecordElement owner) {
    ORecordLazySet found = new ORecordLazySet(owner);
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      OIdentifiable id = readOptimizedLink(bytes);
      if (id.equals(NULL_RECORD_ID)) found.addInternal(null);
      else found.addInternal(id);
    }
    return found;
  }

  protected OIdentifiable readOptimizedLink(final BytesContainer bytes) {
    ORecordId id =
        new ORecordId(OVarIntSerializer.readAsInteger(bytes), OVarIntSerializer.readAsLong(bytes));
    if (id.isTemporary()) {
      OIdentifiable persRef = id.getRecord();
      if (persRef != null) return persRef;
    }
    return id;
  }

  private Collection<?> readEmbeddedList(final BytesContainer bytes, final ORecordElement owner) {
    OTrackedList<Object> found = new OTrackedList<>(owner);
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      OType itemType = readOType(bytes);
      if (itemType == null) found.addInternal(null);
      else found.addInternal(deserializeValue(bytes, itemType, found));
    }
    return found;
  }

  private Collection<?> readEmbeddedSet(final BytesContainer bytes, final ORecordElement owner) {
    OTrackedSet<Object> found = new OTrackedSet<>(owner);
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      OType itemType = readOType(bytes);
      if (itemType == null) found.addInternal(null);
      else found.addInternal(deserializeValue(bytes, itemType, found));
    }
    return found;
  }

  private OType getLinkedType(ODocument document, OType type, String key) {
    if (type != OType.EMBEDDEDLIST && type != OType.EMBEDDEDSET && type != OType.EMBEDDEDMAP)
      return null;
    OClass immutableClass = ODocumentInternal.getImmutableSchemaClass(document);
    if (immutableClass != null) {
      OProperty prop = immutableClass.getProperty(key);
      if (prop != null) {
        return prop.getLinkedType();
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public void serializeValue(
      final BytesContainer bytes, Object value, final OType type, final OType linkedType) {
    int pointer = 0;
    switch (type) {
      case INTEGER:
      case LONG:
      case SHORT:
        OVarIntSerializer.write(bytes, ((Number) value).longValue());
        break;
      case STRING:
        writeString(bytes, value.toString());
        break;
      case DOUBLE:
        long dg = Double.doubleToLongBits((Double) value);
        pointer = bytes.alloc(OLongSerializer.LONG_SIZE);
        OLongSerializer.INSTANCE.serializeLiteral(dg, bytes.bytes, pointer);
        break;
      case FLOAT:
        int fg = Float.floatToIntBits((Float) value);
        pointer = bytes.alloc(OIntegerSerializer.INT_SIZE);
        OIntegerSerializer.INSTANCE.serializeLiteral(fg, bytes.bytes, pointer);
        break;
      case BYTE:
        pointer = bytes.alloc(1);
        bytes.bytes[pointer] = (Byte) value;
        break;
      case BOOLEAN:
        pointer = bytes.alloc(1);
        bytes.bytes[pointer] = ((Boolean) value) ? (byte) 1 : (byte) 0;
        break;
      case DATETIME:
        if (value instanceof Long) {
          OVarIntSerializer.write(bytes, (Long) value);
        } else OVarIntSerializer.write(bytes, ((Date) value).getTime());
        break;
      case DATE:
        long dateValue;
        if (value instanceof Long) {
          dateValue = (Long) value;
        } else dateValue = ((Date) value).getTime();
        dateValue =
            convertDayToTimezone(
                ODateHelper.getDatabaseTimeZone(), TimeZone.getTimeZone("GMT"), dateValue);
        OVarIntSerializer.write(bytes, dateValue / MILLISEC_PER_DAY);
        break;
      case EMBEDDED:
        if (value instanceof ODocumentSerializable) {
          ODocument cur = ((ODocumentSerializable) value).toDocument();
          cur.field(ODocumentSerializable.CLASS_NAME, value.getClass().getName());
          serialize(cur, bytes);
        } else {
          serialize((ODocument) value, bytes);
        }
        break;
      case EMBEDDEDSET:
      case EMBEDDEDLIST:
        if (value.getClass().isArray())
          writeEmbeddedCollection(bytes, Arrays.asList(OMultiValue.array(value)), linkedType);
        else writeEmbeddedCollection(bytes, (Collection<?>) value, linkedType);
        break;
      case DECIMAL:
        BigDecimal decimalValue = (BigDecimal) value;
        pointer = bytes.alloc(ODecimalSerializer.INSTANCE.getObjectSize(decimalValue));
        ODecimalSerializer.INSTANCE.serialize(decimalValue, bytes.bytes, pointer);
        break;
      case BINARY:
        pointer = writeBinary(bytes, (byte[]) (value));
        break;
      case LINKSET:
      case LINKLIST:
        Collection<OIdentifiable> ridCollection = (Collection<OIdentifiable>) value;
        writeLinkCollection(bytes, ridCollection);
        break;
      case LINK:
        if (!(value instanceof OIdentifiable))
          throw new OValidationException("Value '" + value + "' is not a OIdentifiable");

        writeOptimizedLink(bytes, (OIdentifiable) value);
        break;
      case LINKMAP:
        writeLinkMap(bytes, (Map<Object, OIdentifiable>) value);
        break;
      case EMBEDDEDMAP:
        writeEmbeddedMap(bytes, (Map<Object, Object>) value);
        break;
      case LINKBAG:
        writeRidBag(bytes, (ORidBag) value);
        break;
      case CUSTOM:
        if (!(value instanceof OSerializableStream))
          value = new OSerializableWrapper((Serializable) value);
        writeString(bytes, value.getClass().getName());
        writeBinary(bytes, ((OSerializableStream) value).toStream());
        break;
      case TRANSIENT:
        break;
      case ANY:
        break;
    }
  }

  private int writeBinary(final BytesContainer bytes, final byte[] valueBytes) {
    final int pointer = OVarIntSerializer.write(bytes, valueBytes.length);
    final int start = bytes.alloc(valueBytes.length);
    System.arraycopy(valueBytes, 0, bytes.bytes, start, valueBytes.length);
    return pointer;
  }

  private int writeLinkMap(final BytesContainer bytes, final Map<Object, OIdentifiable> map) {
    final boolean disabledAutoConversion =
        map instanceof ORecordLazyMultiValue
            && ((ORecordLazyMultiValue) map).isAutoConvertToRecord();

    if (disabledAutoConversion)
      // AVOID TO FETCH RECORD
      ((ORecordLazyMultiValue) map).setAutoConvertToRecord(false);

    try {
      final int fullPos = OVarIntSerializer.write(bytes, map.size());
      for (Entry<Object, OIdentifiable> entry : map.entrySet()) {
        // TODO:check skip of complex types
        // FIXME: changed to support only string key on map
        final OType type = OType.STRING;
        writeOType(bytes, bytes.alloc(1), type);
        writeString(bytes, entry.getKey().toString());
        if (entry.getValue() == null) writeNullLink(bytes);
        else writeOptimizedLink(bytes, entry.getValue());
      }
      return fullPos;

    } finally {
      if (disabledAutoConversion) ((ORecordLazyMultiValue) map).setAutoConvertToRecord(true);
    }
  }

  private int writeEmbeddedMap(BytesContainer bytes, Map<Object, Object> map) {
    final int fullPos = OVarIntSerializer.write(bytes, map.size());
    for (Entry<Object, Object> entry : map.entrySet()) {
      writeString(bytes, entry.getKey().toString());
      final Object value = entry.getValue();
      if (value != null) {
        final OType type = getTypeFromValueEmbedded(value);
        if (type == null) {
          throw new OSerializationException(
              "Impossible serialize value of type "
                  + value.getClass()
                  + " with the Result binary serializer");
        }
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(bytes, value, type, null);
      } else {
        writeOType(bytes, bytes.alloc(1), null);
      }
    }
    return fullPos;
  }

  private int writeNullLink(final BytesContainer bytes) {
    final int pos = OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterPosition());
    return pos;
  }

  protected int writeOptimizedLink(final BytesContainer bytes, OIdentifiable link) {
    if (!link.getIdentity().isPersistent()) {
      final ORecord real = link.getRecord();
      if (real != null) link = real;
    }
    final int pos = OVarIntSerializer.write(bytes, link.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, link.getIdentity().getClusterPosition());
    return pos;
  }

  private int writeLinkCollection(
      final BytesContainer bytes, final Collection<OIdentifiable> value) {
    final int pos = OVarIntSerializer.write(bytes, value.size());

    final boolean disabledAutoConversion =
        value instanceof ORecordLazyMultiValue
            && ((ORecordLazyMultiValue) value).isAutoConvertToRecord();

    if (disabledAutoConversion)
      // AVOID TO FETCH RECORD
      ((ORecordLazyMultiValue) value).setAutoConvertToRecord(false);

    try {
      for (OIdentifiable itemValue : value) {
        // TODO: handle the null links
        if (itemValue == null) writeNullLink(bytes);
        else writeOptimizedLink(bytes, itemValue);
      }

    } finally {
      if (disabledAutoConversion) ((ORecordLazyMultiValue) value).setAutoConvertToRecord(true);
    }

    return pos;
  }

  private int writeEmbeddedCollection(
      final BytesContainer bytes, final Collection<?> value, final OType linkedType) {
    final int pos = OVarIntSerializer.write(bytes, value.size());
    // TODO manage embedded type from schema and auto-determined.
    for (Object itemValue : value) {
      // TODO:manage in a better way null entry
      if (itemValue == null) {
        writeOType(bytes, bytes.alloc(1), null);
        continue;
      }
      OType type;
      if (linkedType == null) type = getTypeFromValueEmbedded(itemValue);
      else type = linkedType;
      if (type != null) {
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(bytes, itemValue, type, null);
      } else {
        throw new OSerializationException(
            "Impossible serialize value of type "
                + value.getClass()
                + " with the ODocument binary serializer");
      }
    }
    return pos;
  }

  private OType getFieldType(final ODocumentEntry entry) {
    OType type = entry.type;
    if (type == null) {
      final OProperty prop = entry.property;
      if (prop != null) type = prop.getType();
    }
    if (type == null || OType.ANY == type) type = OType.getTypeByValue(entry.value);
    return type;
  }

  private OType getTypeFromValueEmbedded(final Object fieldValue) {
    OType type = OType.getTypeByValue(fieldValue);
    if (type == OType.LINK
        && fieldValue instanceof ODocument
        && !((ODocument) fieldValue).getIdentity().isValid()) type = OType.EMBEDDED;
    return type;
  }

  protected String readString(final BytesContainer bytes) {
    final int len = OVarIntSerializer.readAsInteger(bytes);
    final String res = stringFromBytes(bytes.bytes, bytes.offset, len);
    bytes.skip(len);
    return res;
  }

  protected int readInteger(final BytesContainer container) {
    final int value =
        OIntegerSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OIntegerSerializer.INT_SIZE;
    return value;
  }

  private byte readByte(final BytesContainer container) {
    return container.bytes[container.offset++];
  }

  private long readLong(final BytesContainer container) {
    final long value =
        OLongSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OLongSerializer.LONG_SIZE;
    return value;
  }

  private int writeEmptyString(final BytesContainer bytes) {
    return OVarIntSerializer.write(bytes, 0);
  }

  private int writeString(final BytesContainer bytes, final String toWrite) {
    final byte[] nameBytes = bytesFromString(toWrite);
    final int pointer = OVarIntSerializer.write(bytes, nameBytes.length);
    final int start = bytes.alloc(nameBytes.length);
    System.arraycopy(nameBytes, 0, bytes.bytes, start, nameBytes.length);
    return pointer;
  }

  private byte[] bytesFromString(final String toWrite) {
    try {
      return toWrite.getBytes(CHARSET_UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSerializationException("Error on string encoding"), e);
    }
  }

  protected String stringFromBytes(final byte[] bytes, final int offset, final int len) {
    try {
      return new String(bytes, offset, len, CHARSET_UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSerializationException("Error on string decoding"), e);
    }
  }

  public OBinaryField deserializeField(
      final BytesContainer bytes, final OClass iClass, final String iFieldName) {
    // TODO: check if integrate the binary disc binary comparator here
    throw new UnsupportedOperationException("network serializer doesn't support comparators");
  }

  private long convertDayToTimezone(TimeZone from, TimeZone to, long time) {
    Calendar fromCalendar = Calendar.getInstance(from);
    fromCalendar.setTimeInMillis(time);
    Calendar toCalendar = Calendar.getInstance(to);
    toCalendar.setTimeInMillis(0);
    toCalendar.set(Calendar.ERA, fromCalendar.get(Calendar.ERA));
    toCalendar.set(Calendar.YEAR, fromCalendar.get(Calendar.YEAR));
    toCalendar.set(Calendar.MONTH, fromCalendar.get(Calendar.MONTH));
    toCalendar.set(Calendar.DAY_OF_MONTH, fromCalendar.get(Calendar.DAY_OF_MONTH));
    toCalendar.set(Calendar.HOUR_OF_DAY, 0);
    toCalendar.set(Calendar.MINUTE, 0);
    toCalendar.set(Calendar.SECOND, 0);
    toCalendar.set(Calendar.MILLISECOND, 0);
    return toCalendar.getTimeInMillis();
  }

  public ORecord fromStream(byte[] iSource, ORecord record) {
    return fromStream(iSource, record, null);
  }

  @Override
  public ORecord fromStream(byte[] iSource, ORecord iRecord, String[] iFields) {
    if (iSource == null || iSource.length == 0) return iRecord;
    if (iRecord == null) {
      iRecord = new ODocument();
    } else if (iRecord instanceof OBlob) {
      iRecord.fromStream(iSource);
      return iRecord;
    } else if (iRecord instanceof ORecordFlat) {
      iRecord.fromStream(iSource);
      return iRecord;
    }
    ORecordInternal.setRecordSerializer(iRecord, this);
    BytesContainer container = new BytesContainer(iSource);

    try {
      if (iFields != null && iFields.length > 0)
        deserializePartial((ODocument) iRecord, container, iFields);
      else deserialize((ODocument) iRecord, container);
    } catch (RuntimeException e) {
      OLogManager.instance()
          .warn(
              this,
              "Error deserializing record with id %s send this data for debugging: %s ",
              iRecord.getIdentity().toString(),
              Base64.getEncoder().encodeToString(iSource));
      throw e;
    }
    return iRecord;
  }

  @Override
  public byte[] toStream(ORecord iSource) {
    if (iSource instanceof OBlob) {
      return iSource.toStream();
    } else if (iSource instanceof ORecordFlat) {
      return iSource.toStream();
    } else {
      final BytesContainer container = new BytesContainer();

      ODocument doc = (ODocument) iSource;
      // SERIALIZE RECORD
      serialize(doc, container);
      return container.fitBytes();
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

  @Override
  public boolean getSupportBinaryEvaluate() {
    return false;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String[] getFieldNames(ODocument reference, byte[] iSource) {
    if (iSource == null || iSource.length == 0) return new String[0];

    final BytesContainer container = new BytesContainer(iSource);

    try {
      return getFieldNames(reference, container);
    } catch (RuntimeException e) {
      OLogManager.instance()
          .warn(
              this,
              "Error deserializing record to get field-names, send this data for debugging: %s ",
              Base64.getEncoder().encodeToString(iSource));
      throw e;
    }
  }
}
