package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

public class ORecordSerializerBinaryV0 implements ODocumentSerializer {

  private Charset           utf8;
  private static final long MILLISEC_PER_DAY = 86400000;
  private static final long ONE_HOUR         = 3600000;

  public ORecordSerializerBinaryV0() {
    utf8 = Charset.forName("UTF-8");
  }

  @Override
  public void deserialize(ODocument document, BytesContainer bytes) {
    String className = readString(bytes);
    if (className.length() != 0)
      document.setClassNameIfExists(className);
    int last = 0;
    String field;
    while ((field = readString(bytes)).length() != 0) {
      int valuePos = OIntegerSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
      bytes.read(OIntegerSerializer.INT_SIZE);
      OType type = readOType(bytes);
      // TODO:This is wrong should not stay here
      if (document.containsField(field))
        continue;
      if (valuePos != 0) {
        int headerCursor = bytes.offset;
        bytes.offset = valuePos;
        Object value = readSingleValue(bytes, type, document);
        if (bytes.offset > last)
          last = bytes.offset;
        bytes.offset = headerCursor;
        // TODO:This is wrong should not stay here
        if (document.fieldType(field) != null || OType.LINK == type)
          document.field(field, value);
        else
          document.field(field, value, type);
      } else
        document.field(field, (Object) null);
    }
    if (last > bytes.offset)
      bytes.offset = last;
  }

  private OType readOType(BytesContainer bytes) {
    OType type = OType.values()[bytes.bytes[bytes.offset]];
    bytes.read(1);
    return type;
  }

  private void writeOType(BytesContainer bytes, int pos, OType type) {
    bytes.bytes[pos] = (byte) type.ordinal();
  }

  private Object readSingleValue(BytesContainer bytes, OType type, ODocument document) {
    Object value = null;
    switch (type) {
    case INTEGER:
      value = OVarIntSerializer.read(bytes).intValue();
      break;
    case LONG:
      value = OVarIntSerializer.read(bytes).longValue();
      break;
    case SHORT:
      value = OVarIntSerializer.read(bytes).shortValue();
      break;
    case STRING:
      value = readString(bytes);
      break;
    case DOUBLE:
      long parsedd = OLongSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
      value = Double.longBitsToDouble(parsedd);
      bytes.read(OLongSerializer.LONG_SIZE);
      break;
    case FLOAT:
      int parsedf = OIntegerSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
      value = Float.intBitsToFloat(parsedf);
      bytes.read(OIntegerSerializer.INT_SIZE);
      break;
    case BYTE:
      value = bytes.bytes[bytes.offset];
      bytes.read(1);
      break;
    case BOOLEAN:
      value = bytes.bytes[bytes.offset] == 1 ? true : false;
      bytes.read(1);
      break;
    case DATETIME:
      value = new Date(OVarIntSerializer.read(bytes).longValue());
      break;
    case DATE:
      value = new Date((OVarIntSerializer.read(bytes).longValue() * MILLISEC_PER_DAY) - ONE_HOUR);
      break;
    case EMBEDDED:
      value = new ODocument();
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
      }
      break;
    case EMBEDDEDSET:
      value = readEmbeddedCollection(bytes, new OTrackedSet<Object>(document), document);
      break;
    case EMBEDDEDLIST:
      value = readEmbeddedCollection(bytes, new OTrackedList<Object>(document), document);
      break;
    case LINKSET:
      value = readLinkCollection(bytes, new OMVRBTreeRIDSet(document));
      break;
    case LINKLIST:
      value = readLinkCollection(bytes, new ORecordLazyList(document));
      break;
    case BINARY:
      value = readBinary(bytes);
      break;
    case LINK:
      value = readOptimizedLink(bytes);
      break;
    case LINKMAP:
      value = readLinkMap(bytes, document);
      break;
    case EMBEDDEDMAP:
      value = readEmbeddedMap(bytes, document);
      break;
    case DECIMAL:
      value = ODecimalSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
      bytes.read(ODecimalSerializer.INSTANCE.getObjectSize(bytes.bytes, bytes.offset));
      break;
    case LINKBAG:
      ORidBag bag = new ORidBag();
      bag.fromStream(bytes);
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
        value = stream;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      break;
    case ANY:
      break;
    }
    return value;
  }

  private byte[] readBinary(BytesContainer bytes) {
    Number n = OVarIntSerializer.read(bytes);
    byte[] newValue = new byte[n.intValue()];
    System.arraycopy(bytes.bytes, bytes.offset, newValue, 0, newValue.length);
    bytes.read(n.intValue());
    return newValue;
  }

  private Map<Object, OIdentifiable> readLinkMap(BytesContainer bytes, ODocument document) {
    int size = OVarIntSerializer.read(bytes).intValue();
    Map<Object, OIdentifiable> result = new ORecordLazyMap(document);
    while ((size--) > 0) {
      OType keyType = readOType(bytes);
      Object key = readSingleValue(bytes, keyType, document);
      OIdentifiable value = readOptimizedLink(bytes);
      result.put(key, value);
    }
    return result;
  }

  private Object readEmbeddedMap(BytesContainer bytes, ODocument document) {
    int size = OVarIntSerializer.read(bytes).intValue();
    Map<Object, Object> result = new OTrackedMap<Object>(document);
    int last = 0;
    while ((size--) > 0) {
      OType keyType = readOType(bytes);
      Object key = readSingleValue(bytes, keyType, document);
      int valuePos = OIntegerSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
      bytes.read(OIntegerSerializer.INT_SIZE);
      OType type = readOType(bytes);
      if (valuePos != 0) {
        int headerCursor = bytes.offset;
        bytes.offset = valuePos;
        Object value = readSingleValue(bytes, type, document);
        if (bytes.offset > last)
          last = bytes.offset;
        bytes.offset = headerCursor;
        result.put(key, value);
      } else
        result.put(key, null);
    }
    if (last > bytes.offset)
      bytes.offset = last;
    return result;
  }

  private Collection<OIdentifiable> readLinkCollection(BytesContainer bytes, Collection<OIdentifiable> found) {
    int items = OVarIntSerializer.read(bytes).intValue();
    for (int i = 0; i < items; i++) {
      OIdentifiable identifiable = readLink(bytes);
      if (ORecordId.EMPTY_RECORD_ID.equals(identifiable))
        found.add(null);
      else
        found.add(identifiable);
    }
    return found;
  }

  private OIdentifiable readLink(BytesContainer bytes) {
    int cluster = OIntegerSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.read(OIntegerSerializer.INT_SIZE);
    long record = OLongSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.read(OLongSerializer.LONG_SIZE);
    return new ORecordId(cluster, new OClusterPositionLong(record));
  }

  private OIdentifiable readOptimizedLink(BytesContainer bytes) {
    int cluster = OVarIntSerializer.read(bytes).intValue();
    long record = OVarIntSerializer.read(bytes).longValue();
    return new ORecordId(cluster, new OClusterPositionLong(record));
  }

  private Collection<?> readEmbeddedCollection(BytesContainer bytes, Collection<Object> found, ODocument document) {
    int items = OVarIntSerializer.read(bytes).intValue();
    OType type = readOType(bytes);
    if (type == OType.ANY) {
      for (int i = 0; i < items; i++) {
        OType itemType = readOType(bytes);
        if (itemType == OType.ANY)
          found.add(null);
        else
          found.add(readSingleValue(bytes, itemType, document));
      }
      return found;
    }
    // TODO: manage case where type is known
    return null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void serialize(ODocument document, BytesContainer bytes) {
    if (document.getClassName() != null)
      writeString(bytes, document.getClassName());
    else
      writeEmptyString(bytes);
    int[] pos = new int[document.fields()];
    int i = 0;
    Entry<String, ?> values[] = new Entry[document.fields()];
    for (Entry<String, Object> entry : document) {
      writeString(bytes, entry.getKey());
      pos[i] = bytes.alloc(OIntegerSerializer.INT_SIZE + 1);
      values[i] = entry;
      i++;
    }
    writeEmptyString(bytes);

    for (i = 0; i < values.length; i++) {
      int pointer = 0;
      Object value = values[i].getValue();
      if (value != null) {
        OType type = getFieldType(document, values[i].getKey(), value);
        // temporary skip serialization skip of unknown types
        if (type == null)
          continue;
        pointer = writeSingleValue(bytes, value, type, getLinkedType(document, values[i].getKey()));
        OIntegerSerializer.INSTANCE.serialize(pointer, bytes.bytes, pos[i]);
        writeOType(bytes, (pos[i] + OIntegerSerializer.INT_SIZE), type);
      }
    }

  }

  private OType getLinkedType(ODocument document, String key) {
    OClass clazz = document.getSchemaClass();
    if (clazz != null) {
      OProperty prop = clazz.getProperty(key);
      if (prop != null) {
        return prop.getLinkedType();
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private int writeSingleValue(BytesContainer bytes, Object value, OType type, OType linkedType) {
    int pointer = 0;
    switch (type) {
    case INTEGER:
    case LONG:
    case SHORT:
      pointer = OVarIntSerializer.write(bytes, ((Number) value).longValue());
      break;
    case STRING:
      pointer = writeString(bytes, (String) value);
      break;
    case DOUBLE:
      long dg = Double.doubleToLongBits((Double) value);
      pointer = bytes.alloc(OLongSerializer.LONG_SIZE);
      OLongSerializer.INSTANCE.serialize(dg, bytes.bytes, pointer);
      break;
    case FLOAT:
      int fg = Float.floatToIntBits((Float) value);
      pointer = bytes.alloc(OIntegerSerializer.INT_SIZE);
      OIntegerSerializer.INSTANCE.serialize(fg, bytes.bytes, pointer);
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
        pointer = OVarIntSerializer.write(bytes, (Long) value);
      } else
        pointer = OVarIntSerializer.write(bytes, ((Date) value).getTime());
      break;
    case DATE:
      long dateValue;
      if (value instanceof Long) {
        dateValue = (Long) value;
      } else
        dateValue = ((Date) value).getTime();
      pointer = OVarIntSerializer.write(bytes, (dateValue + ONE_HOUR) / MILLISEC_PER_DAY);
      break;
    case EMBEDDED:
      pointer = bytes.offset;
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
        pointer = writeEmbeddedCollection(bytes, Arrays.asList(OMultiValue.array(value)), linkedType);
      else
        pointer = writeEmbeddedCollection(bytes, (Collection<?>) value, linkedType);
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
      pointer = writeLinkCollection(bytes, ridCollection);
      break;
    case LINK:
      pointer = writeOptimizedLink(bytes, (OIdentifiable) value);
      break;
    case LINKMAP:
      pointer = writeLinkMap(bytes, (Map<Object, OIdentifiable>) value);
      break;
    case EMBEDDEDMAP:
      pointer = writeEmbeddedMap(bytes, (Map<Object, Object>) value);
      break;
    case LINKBAG:
      pointer = ((ORidBag) value).toStream(bytes);
      break;
    case CUSTOM:
      pointer = writeString(bytes, value.getClass().getName());
      writeBinary(bytes, ((OSerializableStream) value).toStream());
      break;
    case TRANSIENT:
      break;
    case ANY:
      break;
    }
    return pointer;
  }

  private int writeBinary(BytesContainer bytes, byte[] valueBytes) {
    int pointer;
    pointer = OVarIntSerializer.write(bytes, valueBytes.length);
    int start = bytes.alloc(valueBytes.length);
    System.arraycopy(valueBytes, 0, bytes.bytes, start, valueBytes.length);
    return pointer;
  }

  private int writeLinkMap(BytesContainer bytes, Map<Object, OIdentifiable> map) {
    int fullPos = OVarIntSerializer.write(bytes, map.size());
    for (Entry<Object, OIdentifiable> entry : map.entrySet()) {
      // TODO:check skip of complex types
      // FIXME: changed to support only string key on map
      OType type = OType.STRING;
      writeOType(bytes, bytes.alloc(1), type);
      writeString(bytes, entry.getKey().toString());
      writeOptimizedLink(bytes, entry.getValue());
    }
    return fullPos;
  }

  @SuppressWarnings("unchecked")
  private int writeEmbeddedMap(BytesContainer bytes, Map<Object, Object> map) {
    int[] pos = new int[map.size()];
    int i = 0;
    Entry<Object, Object> values[] = new Entry[map.size()];
    int fullPos = OVarIntSerializer.write(bytes, map.size());
    for (Entry<Object, Object> entry : map.entrySet()) {
      // TODO:check skip of complex types
      // FIXME: changed to support only string key on map
      OType type = OType.STRING;
      writeOType(bytes, bytes.alloc(1), type);
      writeString(bytes, entry.getKey().toString());
      pos[i] = bytes.alloc(OIntegerSerializer.INT_SIZE + 1);
      values[i] = entry;
      i++;
    }

    for (i = 0; i < values.length; i++) {
      int pointer = 0;
      Object value = values[i].getValue();
      if (value != null) {
        OType type = getTypeFromValue(value, true);
        // temporary skip serialization of unknown types
        if (type == null)
          continue;
        pointer = writeSingleValue(bytes, value, type, null);
        OIntegerSerializer.INSTANCE.serialize(pointer, bytes.bytes, pos[i]);
        writeOType(bytes, (pos[i] + OIntegerSerializer.INT_SIZE), type);
      }
    }
    return fullPos;
  }

  private OIdentifiable recursiveLinkSave(OIdentifiable link) {

    if (link instanceof ORID) {
      if (((ORID) link).isValid() && ((ORID) link).isNew()) {
        final ODatabaseRecord database = ODatabaseRecordThreadLocal.INSTANCE.get();
        ORecordInternal<?> record = link.getRecord();
        database.save(record);
        return record;
      }
    } else if (link instanceof ORecordInternal<?>) {
      ORID rid = link.getIdentity();
      if (((ORecordInternal<?>) link).isDirty() || (rid.isTemporary())) {
        final ODatabaseRecord database = ODatabaseRecordThreadLocal.INSTANCE.get();
        if (database != null) {
          if (link instanceof ODocument) {
            final OClass schemaClass = ((ODocument) link).getSchemaClass();
            database.save((ORecordInternal<?>) link,
                schemaClass != null ? database.getClusterNameById(schemaClass.getClusterForNewInstance()) : null);
          } else
            database.save((ORecordInternal<?>) link);
        }
      }
    }
    return link;
  }

  private int writeOptimizedLink(BytesContainer bytes, OIdentifiable link) {
    link = recursiveLinkSave(link);
    int pos = OVarIntSerializer.write(bytes, link.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, link.getIdentity().getClusterPosition().longValue());
    return pos;
  }

  private int writeLink(BytesContainer bytes, OIdentifiable link) {
    link = recursiveLinkSave(link);
    int pos = bytes.alloc(OIntegerSerializer.INT_SIZE);
    OIntegerSerializer.INSTANCE.serialize(link.getIdentity().getClusterId(), bytes.bytes, pos);
    int posR = bytes.alloc(OLongSerializer.LONG_SIZE);
    OLongSerializer.INSTANCE.serialize(link.getIdentity().getClusterPosition().longValue(), bytes.bytes, posR);
    return pos;
  }

  private int writeLinkCollection(BytesContainer bytes, Collection<OIdentifiable> value) {
    if (value instanceof OMVRBTreeRIDSet) {
      ((OMVRBTreeRIDSet) value).toStream();
    }
    int pos = OVarIntSerializer.write(bytes, value.size());
    for (OIdentifiable itemValue : value) {
      // TODO: handle the null links
      if (itemValue == null)
        writeLink(bytes, ORecordId.EMPTY_RECORD_ID);
      else
        writeLink(bytes, itemValue);
    }
    return pos;
  }

  private int writeEmbeddedCollection(BytesContainer bytes, Collection<?> value, OType linkedType) {
    int pos = OVarIntSerializer.write(bytes, value.size());
    // TODO manage embedded type from schema and autodeterminated.
    writeOType(bytes, bytes.alloc(1), OType.ANY);
    for (Object itemValue : value) {
      // TODO:manage in a better way null entry
      if (itemValue == null) {
        writeOType(bytes, bytes.alloc(1), OType.ANY);
        continue;
      }
      OType type;
      if (linkedType == null)
        type = getTypeFromValue(itemValue, true);
      else
        type = linkedType;
      if (type != null) {
        writeOType(bytes, bytes.alloc(1), type);
        writeSingleValue(bytes, itemValue, type, null);
      }
    }
    return pos;
  }

  private OType getFieldType(ODocument document, String key, Object fieldValue) {
    OType type = document.fieldType(key);
    if (type == null) {
      OClass clazz = document.getSchemaClass();
      if (clazz != null) {
        OProperty prop = clazz.getProperty(key);
        if (prop != null) {
          type = prop.getType();
        }
      }
      if (type == null || OType.ANY == type)
        type = getTypeFromValue(fieldValue, false);
    }
    return type;
  }

  private OType getTypeFromValue(Object fieldValue, boolean forceEmbedded) {
    OType type = null;
    if (fieldValue.getClass() == byte[].class)
      type = OType.BINARY;
    else if (fieldValue instanceof ORecord<?>) {
      if (fieldValue instanceof ODocument && (((ODocument) fieldValue).hasOwners() || forceEmbedded)) {
        type = OType.EMBEDDED;
      } else
        type = OType.LINK;
    } else if (fieldValue instanceof ODocumentSerializable)
      type = OType.EMBEDDED;
    else if (fieldValue instanceof ORID)
      type = OType.LINK;
    else if (fieldValue instanceof Date)
      type = OType.DATETIME;
    else if (fieldValue instanceof String)
      type = OType.STRING;
    else if (fieldValue instanceof Integer || fieldValue instanceof BigInteger)
      type = OType.INTEGER;
    else if (fieldValue instanceof Long)
      type = OType.LONG;
    else if (fieldValue instanceof Float)
      type = OType.FLOAT;
    else if (fieldValue instanceof Short)
      type = OType.SHORT;
    else if (fieldValue instanceof Byte)
      type = OType.BYTE;
    else if (fieldValue instanceof Double)
      type = OType.DOUBLE;
    else if (fieldValue instanceof BigDecimal)
      type = OType.DECIMAL;
    else if (fieldValue instanceof Boolean)
      type = OType.BOOLEAN;
    else if (fieldValue instanceof ORidBag)
      type = OType.LINKBAG;
    else if (fieldValue instanceof Map<?, ?>) {

      boolean link = true;
      if (((Map<?, ?>) fieldValue).size() > 0) {
        final Iterable<Object> firstValue = OMultiValue.getMultiValueIterable(fieldValue);
        for (Object object : firstValue) {
          if (!(object instanceof OIdentifiable))
            link = false;
        }
      } else
        link = false;
      if (link)
        type = OType.LINKMAP;
      else
        type = OType.EMBEDDEDMAP;
    } else if (fieldValue instanceof OMultiCollectionIterator<?>)
      type = ((OMultiCollectionIterator<?>) fieldValue).isEmbedded() ? OType.EMBEDDEDLIST : OType.LINKLIST;
    else if (fieldValue.getClass().isArray()) {
      type = OType.EMBEDDEDLIST;
    } else if (fieldValue instanceof Collection<?>) {
      final Object firstValue = OMultiValue.getFirstValue(fieldValue);
      if (firstValue instanceof OIdentifiable) {
        if (fieldValue instanceof Set<?>) {
          type = OType.LINKSET;
        } else {
          type = OType.LINKLIST;
        }
      } else {
        if (fieldValue instanceof Set<?>) {
          type = OType.EMBEDDEDSET;
        } else {
          type = OType.EMBEDDEDLIST;
        }
      }
    } else if (fieldValue instanceof OSerializableStream)
      type = OType.CUSTOM;
    return type;
  }

  private String readString(BytesContainer bytes) {
    Number n = OVarIntSerializer.read(bytes);
    int len = n.intValue();
    String res = new String(bytes.bytes, bytes.offset, len, utf8);
    bytes.read(len);
    return res;
  }

  private int writeEmptyString(BytesContainer bytes) {
    return OVarIntSerializer.write(bytes, 0);
  }

  private int writeString(BytesContainer bytes, String toWrite) {
    byte[] nameBytes = toWrite.getBytes(utf8);
    int pointer = OVarIntSerializer.write(bytes, nameBytes.length);
    int start = bytes.alloc(nameBytes.length);
    System.arraycopy(nameBytes, 0, bytes.bytes, start, nameBytes.length);
    return pointer;
  }

}
