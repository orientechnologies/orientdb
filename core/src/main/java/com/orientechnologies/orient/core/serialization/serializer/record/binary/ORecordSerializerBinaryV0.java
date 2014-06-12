package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Date;
import java.util.Map.Entry;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class ORecordSerializerBinaryV0 implements ODocumentSerializer {

  @Override
  public void deserialize(ODocument document, BytesContainer bytes) {
    try {
      String field = readString(bytes);
      if (field != null) {
        OType type = OType.getById(bytes.bytes[bytes.offset]);
        short valuePos = (short) ((bytes.bytes[bytes.offset + 1] << 8) | (bytes.bytes[bytes.offset + 2] & 0xff));
        Object value = null;
        BytesContainer valueContainer = new BytesContainer(bytes.bytes, valuePos);
        switch (type) {
        case INTEGER:
        case LONG:
        case SHORT:
          value = OVarIntSerializer.read(valueContainer);
          break;
        case STRING:
          value = readString(valueContainer);
          break;
        case DOUBLE:
          long parsedd = OLongSerializer.INSTANCE.deserialize(bytes.bytes, valuePos);
          value = Double.longBitsToDouble(parsedd);
          break;
        case FLOAT:
          int parsedf = OIntegerSerializer.INSTANCE.deserialize(bytes.bytes, valuePos);
          value = Float.intBitsToFloat(parsedf);
          break;
        case BYTE:
          value = bytes.bytes[valuePos];
          break;
        case BOOLEAN:
          value = bytes.bytes[valuePos] == 1 ? true : false;
          break;
        case DECIMAL:
          break;
        default:
          break;
        }
        document.field(field, value, type);
      }
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  @Override
  public void serialize(ODocument document, BytesContainer bytes) {
    try {
      short[] pos = new short[document.fields()];
      int i = 0;
      for (Entry<String, Object> entry : document) {
        writeString(bytes, entry.getKey());
        pos[i++] = bytes.alloc((short) 3);
      }
      i = 0;
      for (Entry<String, Object> entry : document) {
        short pointer = 0;
        OType type = getFieldType(document, entry.getKey(), entry.getValue());
        switch (type) {
        case INTEGER:
        case LONG:
        case SHORT:
          pointer = OVarIntSerializer.write(bytes, ((Number) entry.getValue()).longValue());
          break;
        case STRING:
          pointer = writeString(bytes, (String) entry.getValue());
          break;
        case DOUBLE:
          long dg = Double.doubleToLongBits((Double) entry.getValue());
          pointer = bytes.alloc((short) OLongSerializer.LONG_SIZE);
          OLongSerializer.INSTANCE.serialize(dg, bytes.bytes, pointer);
          break;
        case FLOAT:
          int fg = Float.floatToIntBits((Float) entry.getValue());
          OIntegerSerializer.INSTANCE.serialize(fg, bytes.bytes, pointer);
          break;
        case BYTE:
          pointer = bytes.alloc((short) 1);
          bytes.bytes[pointer] = (Byte) entry.getValue();
          break;
        case BOOLEAN:
          pointer = bytes.alloc((short) 1);
          bytes.bytes[pointer] = ((Boolean) entry.getValue()) ? (byte) 1 : (byte) 0;
          break;
        case DECIMAL:
          break;
        default:
          break;
        }
        bytes.bytes[pos[i]] = (byte) ((pointer >>> 8) & 0xFF);
        bytes.bytes[pos[i] + 1] = (byte) ((pointer >>> 0) & 0xFF);
        bytes.bytes[pos[i] + 2] = (byte) type.ordinal();
      }
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private OType getFieldType(ODocument document, String key, Object fieldValue) {
    OType type = document.fieldType(key);
    if (type == null) {
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
      else if (fieldValue instanceof ORidBag)
        type = OType.LINKBAG;
      else if (fieldValue instanceof OMultiCollectionIterator<?>)
        type = ((OMultiCollectionIterator<?>) fieldValue).isEmbedded() ? OType.EMBEDDEDLIST : OType.LINKLIST;
      else if (fieldValue instanceof Collection<?> || fieldValue.getClass().isArray()) {

      }
    }
    return type;
  }

  private String readString(BytesContainer bytes) throws UnsupportedEncodingException {
    Number n = OVarIntSerializer.read(bytes);
    int len = n.intValue();
    if (len == 0)
      return null;
    String res = new String(bytes.bytes, bytes.offset, len, "UTF-8");
    bytes.read(len);
    return res;
  }

  private short writeString(BytesContainer bytes, String toWrite) throws UnsupportedEncodingException {
    short pointer = OVarIntSerializer.write(bytes, toWrite.length());
    byte[] nameBytes = toWrite.getBytes("UTF-8");
    short start = bytes.alloc((short) nameBytes.length);
    System.arraycopy(nameBytes, 0, bytes.bytes, start, nameBytes.length);
    return pointer;
  }
}
