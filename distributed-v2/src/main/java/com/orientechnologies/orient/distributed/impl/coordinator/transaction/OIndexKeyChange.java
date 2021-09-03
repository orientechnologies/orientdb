package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetwork;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OIndexKeyChange {

  // Key Type
  private static final int SIMPLE = 1;
  private static final int COMPOSITE = 2;

  private Object key;
  private List<OIndexKeyOperation> operations;

  public OIndexKeyChange(Object key, List<OIndexKeyOperation> operations) {
    this.key = key;
    this.operations = operations;
  }

  public OIndexKeyChange() {}

  public Object getKey() {
    return key;
  }

  public void serialize(DataOutput output) throws IOException {
    serializeKey(key, output);
    output.writeInt(operations.size());
    for (OIndexKeyOperation operation : operations) {
      operation.serialize(output);
    }
  }

  private void serializeKey(Object key, DataOutput output) throws IOException {
    if (key instanceof OCompositeKey) {
      output.writeBoolean(true);
      List<Object> keys = ((OCompositeKey) key).getKeys();
      output.writeInt(keys.size());
      for (Object o : keys) {
        serializeKey(o, output);
      }
    } else {
      output.writeBoolean(false);
      if (key == null) {
        output.writeBoolean(true);
      } else {
        output.writeBoolean(false);
        OType valType = OType.getTypeByValue(key);
        output.writeByte(valType.getId());
        byte[] bytes = ORecordSerializerNetwork.INSTANCE.serializeValue(key, valType);
        output.writeInt(bytes.length);
        output.write(bytes);
      }
    }
  }

  public void deserialize(DataInput input) throws IOException {
    key = deserializeKey(input);
    int operations = input.readInt();
    this.operations = new ArrayList<>(operations);
    while (operations-- > 0) {
      OIndexKeyOperation operation = new OIndexKeyOperation();
      operation.deserialize(input);
      this.operations.add(operation);
    }
  }

  private Object deserializeKey(DataInput input) throws IOException {
    boolean composite = input.readBoolean();
    if (composite) {
      int size = input.readInt();
      List<Object> keys = new ArrayList<>(size);
      while (size-- > 0) {
        keys.add(deserializeKey(input));
      }
      return new OCompositeKey(keys);
    } else {
      boolean isNull = input.readBoolean();
      if (isNull) return null;
      OType keyType = OType.getById(input.readByte());
      int keySize = input.readInt();
      byte[] bytes = new byte[keySize];
      input.readFully(bytes);
      return ORecordSerializerNetwork.INSTANCE.deserializeValue(bytes, keyType);
    }
  }

  public List<OIndexKeyOperation> getOperations() {
    return operations;
  }
}
