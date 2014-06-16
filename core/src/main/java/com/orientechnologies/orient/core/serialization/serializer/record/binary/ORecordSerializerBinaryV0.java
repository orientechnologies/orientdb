package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class ORecordSerializerBinaryV0 implements ODocumentSerializer {

  private Charset utf8;

  public ORecordSerializerBinaryV0() {
    utf8 = Charset.forName("UTF-8");
  }

  @Override
  public void deserialize(ODocument document, BytesContainer bytes) {
    String field;
    while ((field = readString(bytes)) != null) {
      short valuePos = OShortSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
      bytes.read(2);
      OType type = readOType(bytes);
      short headerCursor = bytes.offset;
      bytes.offset = valuePos;
      Object value = readValue(bytes, type);
      bytes.offset = headerCursor;
      document.field(field, value, type);
    }
  }

  private OType readOType(BytesContainer bytes) {
    OType type = OType.values()[bytes.bytes[bytes.offset]];
    bytes.read(1);
    return type;
  }

  private void writeOType(BytesContainer bytes, short pos, OType type) {
    bytes.bytes[pos] = (byte) type.ordinal();
  }

  private Object readValue(BytesContainer bytes, OType type) {
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
    case EMBEDDED:
      value = new ODocument();
      deserialize((ODocument) value, bytes);
      break;
    case EMBEDDEDSET:
      value = readCollection(bytes, new HashSet<Object>());
      break;
    case EMBEDDEDLIST:
      value = readCollection(bytes, new ArrayList<Object>());
      break;
    case LINKSET:
      value = readLinkCollection(bytes, new HashSet<ORID>());
      break;
    case LINKLIST:
      value = readLinkCollection(bytes, new ArrayList<ORID>());
      break;
    case BINARY:
      Number n = OVarIntSerializer.read(bytes);
      byte[] newValue = new byte[n.intValue()];
      System.arraycopy(bytes.bytes, bytes.offset, newValue, 0, newValue.length);
      bytes.read(n.intValue());
      value = newValue;
      break;
    case LINK:
      value = readOptimizedLink(bytes);
      break;
    case DECIMAL:
      break;
    default:
      break;
    }
    return value;
  }

  private Collection<ORID> readLinkCollection(BytesContainer bytes, Collection<ORID> found) {
    int items = OVarIntSerializer.read(bytes).intValue();
    for (int i = 0; i < items; i++) {
      found.add(readLink(bytes));
    }
    return found;
  }

  private ORID readLink(BytesContainer bytes) {
    int cluster = OIntegerSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.read((short) OIntegerSerializer.INT_SIZE);
    long record = OLongSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.read((short) OLongSerializer.LONG_SIZE);
    return new ORecordId(cluster, new OClusterPositionLong(record));
  }

  private ORID readOptimizedLink(BytesContainer bytes) {
    int cluster = OVarIntSerializer.read(bytes).intValue();
    long record = OVarIntSerializer.read(bytes).longValue();
    return new ORecordId(cluster, new OClusterPositionLong(record));
  }

  private Collection<?> readCollection(BytesContainer bytes, Collection<Object> found) {
    int items = OVarIntSerializer.read(bytes).intValue();
    OType type = readOType(bytes);
    if (type == OType.ANY) {
      for (int i = 0; i < items; i++) {
        OType itemType = readOType(bytes);
        found.add(readValue(bytes, itemType));
      }
      return found;
    }
    // TODO: manage case where type is known
    return null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void serialize(ODocument document, BytesContainer bytes) {
    short[] pos = new short[document.fields()];
    int i = 0;
    Entry<String, ?> values[] = new Entry[document.fields()];
    for (Entry<String, Object> entry : document) {
      writeString(bytes, entry.getKey());
      pos[i] = bytes.alloc((short) 3);
      values[i] = entry;
      i++;
    }
    OVarIntSerializer.write(bytes, 0);

    for (i = 0; i < values.length; i++) {
      short pointer = 0;
      Object value = values[i].getValue();
      OType type = getFieldType(document, values[i].getKey(), value);
      // temporary skip serialization skip of unknown types
      if (type == null)
        continue;
      pointer = writeSingleValue(bytes, value, type);
      OShortSerializer.INSTANCE.serialize(pointer, bytes.bytes, pos[i]);
      writeOType(bytes, (short) (pos[i] + 2), type);
    }

  }

  @SuppressWarnings("unchecked")
  private short writeSingleValue(BytesContainer bytes, Object value, OType type) {
    short pointer = 0;
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
      pointer = bytes.alloc((short) OLongSerializer.LONG_SIZE);
      OLongSerializer.INSTANCE.serialize(dg, bytes.bytes, pointer);
      break;
    case FLOAT:
      int fg = Float.floatToIntBits((Float) value);
      pointer = bytes.alloc((short) OIntegerSerializer.INT_SIZE);
      OIntegerSerializer.INSTANCE.serialize(fg, bytes.bytes, pointer);
      break;
    case BYTE:
      pointer = bytes.alloc((short) 1);
      bytes.bytes[pointer] = (Byte) value;
      break;
    case BOOLEAN:
      pointer = bytes.alloc((short) 1);
      bytes.bytes[pointer] = ((Boolean) value) ? (byte) 1 : (byte) 0;
      break;
    case DATETIME:
      long time = ((Date) value).getTime();
      pointer = OVarIntSerializer.write(bytes, time);
      break;
    case DATE:
      break;
    case EMBEDDED:
      pointer = bytes.offset;
      serialize((ODocument) value, bytes);
      break;
    case EMBEDDEDSET:
    case EMBEDDEDLIST:
      pointer = writeCollection(bytes, (Collection<?>) value);
      break;
    case DECIMAL:
      break;
    case BINARY:
      byte[] valueBytes = (byte[]) (value);
      pointer = OVarIntSerializer.write(bytes, valueBytes.length);
      short start = bytes.alloc((short) valueBytes.length);
      System.arraycopy(valueBytes, 0, bytes.bytes, start, valueBytes.length);
      break;
    case LINKSET:
    case LINKLIST:
      Collection<ORID> ridCollection = (Collection<ORID>) value;
      pointer = writeLinkCollection(bytes, ridCollection);
      break;
    case LINK:
      pointer = writeOptimizedLink(bytes, (ORID) value);
      break;
    default:
      break;
    }
    return pointer;
  }

  public short writeOptimizedLink(BytesContainer bytes, ORID link) {
    short pos = OVarIntSerializer.write(bytes, link.getClusterId());
    OVarIntSerializer.write(bytes, link.getClusterPosition().longValue());
    return pos;
  }

  public short writeLink(BytesContainer bytes, ORID link) {
    short pos = bytes.alloc((short) OIntegerSerializer.INT_SIZE);
    OIntegerSerializer.INSTANCE.serialize(link.getClusterId(), bytes.bytes, pos);
    short posR = bytes.alloc((short) OLongSerializer.LONG_SIZE);
    OLongSerializer.INSTANCE.serialize(link.getClusterPosition().longValue(), bytes.bytes, posR);
    return pos;
  }

  private short writeLinkCollection(BytesContainer bytes, Collection<ORID> value) {
    short pos = OVarIntSerializer.write(bytes, value.size());
    for (ORID itemValue : value) {
      writeLink(bytes, itemValue);
    }
    return pos;
  }

  private short writeCollection(BytesContainer bytes, Collection<?> value) {
    short pos = OVarIntSerializer.write(bytes, value.size());
    // TODO manage embedded type from schema and autodeterminated.
    writeOType(bytes, bytes.alloc((short) 1), OType.ANY);
    for (Object itemValue : value) {
      // TODO:manage null entry;
      OType type = getTypeFromValue(itemValue);
      if (type != null) {
        writeOType(bytes, bytes.alloc((short) 1), type);
        writeSingleValue(bytes, itemValue, type);
      }
    }
    return pos;
  }

  private OType getFieldType(ODocument document, String key, Object fieldValue) {
    OType type = document.fieldType(key);
    if (type == null) {
      type = getTypeFromValue(fieldValue);
    }
    return type;
  }

  private OType getTypeFromValue(Object fieldValue) {
    OType type = null;
    if (fieldValue.getClass() == byte[].class)
      type = OType.BINARY;
    else if (fieldValue instanceof ORecord<?>) {
      if (fieldValue instanceof ODocument && ((ODocument) fieldValue).hasOwners()) {
        type = OType.EMBEDDED;
      } else
        type = OType.LINK;
    } else if (fieldValue instanceof ORID)
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
    else if (fieldValue instanceof OMultiCollectionIterator<?>)
      type = ((OMultiCollectionIterator<?>) fieldValue).isEmbedded() ? OType.EMBEDDEDLIST : OType.LINKLIST;
    else if (fieldValue.getClass().isArray()) {
      type = OType.EMBEDDEDLIST;
    } else if (fieldValue instanceof Collection<?>) {
      if (fieldValue instanceof Set<?>) {
        type = OType.EMBEDDEDSET;
      } else {
        type = OType.EMBEDDEDLIST;
      }
    }
    return type;
  }

  private String readString(BytesContainer bytes) {
    Number n = OVarIntSerializer.read(bytes);
    int len = n.intValue();
    if (len == 0)
      return null;
    String res = new String(bytes.bytes, bytes.offset, len, utf8);
    bytes.read(len);
    return res;
  }

  private short writeString(BytesContainer bytes, String toWrite) {
    byte[] nameBytes = toWrite.getBytes(utf8);
    short pointer = OVarIntSerializer.write(bytes, nameBytes.length);
    short start = bytes.alloc((short) nameBytes.length);
    System.arraycopy(nameBytes, 0, bytes.bytes, start, nameBytes.length);
    return pointer;
  }
}
