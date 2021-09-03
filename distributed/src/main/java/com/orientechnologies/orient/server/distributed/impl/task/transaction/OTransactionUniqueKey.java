package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OTransactionUniqueKey implements Comparable<OTransactionUniqueKey> {
  private String index;
  private Object key;
  private int version;

  public OTransactionUniqueKey(String index, Object key, int version) {
    super();
    this.index = index;
    this.key = key;
    this.version = version;

    assert key instanceof Comparable || key == null;
  }

  public String getIndex() {
    return index;
  }

  public Object getKey() {
    return key;
  }

  public int getVersion() {
    return version;
  }

  public void write(ORecordSerializerNetworkV37 serializer, DataOutput out) throws IOException {
    out.writeUTF(getIndex());
    if (getKey() == null) {
      out.writeByte((byte) -1);
    } else if (getKey() instanceof OCompositeKey) {
      // Avoid the default serializer of OCompositeKey which converts the key to a document.
      out.writeByte((byte) -2);
      ((OCompositeKey) getKey()).toStream(serializer, out);
    } else {
      OType type = OType.getTypeByValue(getKey());
      byte[] bytes = serializer.serializeValue(getKey(), type);
      out.writeByte((byte) type.getId());
      out.writeInt(bytes.length);
      out.write(bytes);
    }
    out.writeInt(getVersion());
  }

  public static OTransactionUniqueKey read(DataInput in, ORecordSerializerNetworkV37 serializer)
      throws IOException {
    String index = in.readUTF();
    Object key;
    byte b = in.readByte();
    if (b == -1) {
      key = null;
    } else if (b == -2) {
      OCompositeKey compositeKey = new OCompositeKey();
      compositeKey.fromStream(serializer, in);
      key = compositeKey;
    } else {
      OType type = OType.getById(b);
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      key = serializer.deserializeValue(bytes, type);
    }
    int version = in.readInt();
    return new OTransactionUniqueKey(index, key, version);
  }

  @Override
  public int compareTo(OTransactionUniqueKey o) {
    int indexCompare = this.index.compareTo(o.index);
    if (indexCompare == 0) {
      if (this.key == null) {
        if (o.key == null) {
          return 0;
        } else {
          return -1;
        }
      } else if (o.key == null) {
        return 1;
      } else {
        return ((Comparable) this.key).compareTo(o.key);
      }
    }
    return indexCompare;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((index == null) ? 0 : index.hashCode());
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + version;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    OTransactionUniqueKey other = (OTransactionUniqueKey) obj;
    if (index == null) {
      if (other.index != null) return false;
    } else if (!index.equals(other.index)) return false;
    if (key == null) {
      if (other.key != null) return false;
    } else if (!key.equals(other.key)) return false;
    if (version != other.version) return false;
    return true;
  }
}
