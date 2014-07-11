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
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
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
import com.orientechnologies.orient.core.util.ODateHelper;

public class ORecordSerializerBinaryV0 implements ODocumentSerializer {

  private static final long MILLISEC_PER_DAY = 86400000;
  private Charset           utf8;

  public ORecordSerializerBinaryV0() {
    utf8 = Charset.forName("UTF-8");
  }

  @Override
  public void deserialize(final ODocument document, final BytesContainer bytes) {
    String className = readString(bytes);
    if (className.length() != 0)
      document.setClassNameIfExists(className);
    int last = 0;
    String field;
    while ((field = readString(bytes)).length() != 0) {
      if (document.containsField(field)) {
        // SKIP FIELD
        bytes.skip(OIntegerSerializer.INT_SIZE + 1);
        continue;
      }

      final int valuePos = readInteger(bytes);
      final OType type = readOType(bytes);

      if (valuePos != 0) {
        int headerCursor = bytes.offset;
        bytes.offset = valuePos;
        final Object value = readSingleValue(bytes, type, document);
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

  protected boolean compareFieldName(final BytesContainer container, final byte[] fieldBytes, final int size) {
    for (int i = 0; i < size; ++i) {
      if (fieldBytes[i] != container.bytes[container.offset + i])
        return false;
    }
    return true;
  }

  private OType readOType(final BytesContainer bytes) {
    return OType.values()[readByte(bytes)];
  }

  private void writeOType(BytesContainer bytes, int pos, OType type) {
    bytes.bytes[pos] = (byte) type.ordinal();
  }

  private Object readSingleValue(BytesContainer bytes, OType type, ODocument document) {
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
      value = readByte(bytes) == 1 ? true : false;
      break;
    case DATETIME:
      value = new Date(OVarIntSerializer.readAsLong(bytes));
      break;
    case DATE:
      long savedTime = OVarIntSerializer.readAsLong(bytes) * MILLISEC_PER_DAY;
      int offset = ODateHelper.getDatabaseTimeZone().getOffset(savedTime);
      value = new Date(savedTime - offset);
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
      value = readLinkCollection(bytes, new ORecordLazySet(document));
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
      bytes.skip(ODecimalSerializer.INSTANCE.getObjectSize(bytes.bytes, bytes.offset));
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
    int n = OVarIntSerializer.readAsInteger(bytes);
    byte[] newValue = new byte[n];
    System.arraycopy(bytes.bytes, bytes.offset, newValue, 0, newValue.length);
    bytes.skip(n);
    return newValue;
  }

  private Map<Object, OIdentifiable> readLinkMap(final BytesContainer bytes, final ODocument document) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    Map<Object, OIdentifiable> result = new ORecordLazyMap(document);
    while ((size--) > 0) {
      OType keyType = readOType(bytes);
      Object key = readSingleValue(bytes, keyType, document);
      OIdentifiable value = readOptimizedLink(bytes);
      result.put(key, value);
    }
    return result;
  }

  private Object readEmbeddedMap(final BytesContainer bytes, final ODocument document) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    final Map<Object, Object> result = new OTrackedMap<Object>(document);
    int last = 0;
    while ((size--) > 0) {
      OType keyType = readOType(bytes);
      Object key = readSingleValue(bytes, keyType, document);
      final int valuePos = readInteger(bytes);
      final OType type = readOType(bytes);
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
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      OIdentifiable identifiable = readLink(bytes);
      if (ORecordId.EMPTY_RECORD_ID.equals(identifiable))
        found.add(null);
      else
        found.add(identifiable);
    }
    return found;
  }

  private OIdentifiable readLink(final BytesContainer bytes) {
    return new ORecordId(readInteger(bytes), new OClusterPositionLong(readLong(bytes)));
  }

  private OIdentifiable readOptimizedLink(final BytesContainer bytes) {
    return new ORecordId(OVarIntSerializer.readAsInteger(bytes), new OClusterPositionLong(OVarIntSerializer.readAsLong(bytes)));
  }

  private Collection<?> readEmbeddedCollection(final BytesContainer bytes, final Collection<Object> found, final ODocument document) {
    final int items = OVarIntSerializer.readAsInteger(bytes);
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
      int offset = ODateHelper.getDatabaseTimeZone().getOffset(dateValue);
      pointer = OVarIntSerializer.write(bytes, (dateValue + offset) / MILLISEC_PER_DAY);
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
      if (entry.getValue() == null)
        writeOptimizedLink(bytes, ORecordId.EMPTY_RECORD_ID);
      else
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
        boolean empty = true;
        for (Object object : firstValue) {
          if (!(object instanceof OIdentifiable) && object != null)
            link = false;
          else if (object != null)
            empty = false;
        }
        if (empty)
          link = false;
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
    final int len = OVarIntSerializer.readAsInteger(bytes);
    final String res = new String(bytes.bytes, bytes.offset, len, utf8);
    bytes.skip(len);
    return res;
  }

  private int writeEmptyString(final BytesContainer bytes) {
    return OVarIntSerializer.write(bytes, 0);
  }

  public int readInteger(BytesContainer container) {
    final int value = OIntegerSerializer.INSTANCE.deserialize(container.bytes, container.offset);
    container.offset += OIntegerSerializer.INT_SIZE;
    return value;
  }

  public byte readByte(BytesContainer container) {
    return container.bytes[container.offset++];
  }

  public long readLong(BytesContainer container) {
    final long value = OLongSerializer.INSTANCE.deserialize(container.bytes, container.offset);
    container.offset += OLongSerializer.LONG_SIZE;
    return value;
  }

  public long readShort(BytesContainer container) {
    final short value = OShortSerializer.INSTANCE.deserialize(container.bytes, container.offset);
    container.offset += OShortSerializer.SHORT_SIZE;
    return value;
  }

  private int writeString(final BytesContainer bytes, final String toWrite) {
    final byte[] nameBytes = toWrite.getBytes(utf8);
    final int pointer = OVarIntSerializer.write(bytes, nameBytes.length);
    final int start = bytes.alloc(nameBytes.length);
    System.arraycopy(nameBytes, 0, bytes.bytes, start, nameBytes.length);
    return pointer;
  }

}
