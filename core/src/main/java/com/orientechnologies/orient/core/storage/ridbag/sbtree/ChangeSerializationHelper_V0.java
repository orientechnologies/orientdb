package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tglman on 15/06/17.
 */
public class ChangeSerializationHelper_V0 extends ChangeSerializationHelper{    

  private Change deserializeChange(final byte[] stream, final int offset) {
    int value = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, offset + OByteSerializer.BYTE_SIZE);
    return createChangeInstance(OByteSerializer.INSTANCE.deserializeLiteral(stream, offset), value);
  }

  @Override
  public Map<OIdentifiable, Change> deserializeChanges(BytesContainer bytes) {
    byte[] stream = bytes.bytes;
    int offset = bytes.offset;
    
    final int count = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final HashMap<OIdentifiable, Change> res = new HashMap<>();
    for (int i = 0; i < count; i++) {
      ORecordId rid = OLinkSerializer.INSTANCE.deserialize(stream, offset);
      offset += OLinkSerializer.RID_SIZE;
      Change change = deserializeChange(stream, offset);
      offset += Change.SIZE;

      final OIdentifiable identifiable;
      if (rid.isTemporary() && rid.getRecord() != null)
        identifiable = rid.getRecord();
      else
        identifiable = rid;

      res.put(identifiable, change);
    }

    bytes.offset = offset;
    return res;
  }

  @Override
  public <K extends OIdentifiable> int serializeChanges(Map<K, Change> changes, OBinarySerializer<K> keySerializer, BytesContainer bytes) {
    int allocSize = OIntegerSerializer.INT_SIZE + getChangesSerializedSize(changes.size());
    bytes.offset = bytes.alloc(allocSize);
    
    byte[] stream = bytes.bytes;
    int offset = bytes.offset;
    
    OIntegerSerializer.INSTANCE.serializeLiteral(changes.size(), stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (Map.Entry<K, Change> entry : changes.entrySet()) {
      K key = entry.getKey();

      if (key.getIdentity().isTemporary())
        //noinspection unchecked
        key = key.getRecord();

      keySerializer.serialize(key, stream, offset);
      offset += keySerializer.getObjectSize(key);

      offset += entry.getValue().serialize(stream, offset);
    }
    
    bytes.offset = offset;
    return bytes.offset;
  }

  @Override
  protected int getChangesSerializedSize(int changesCount) {
    return changesCount * (OLinkSerializer.RID_SIZE + Change.SIZE);
  }
}
