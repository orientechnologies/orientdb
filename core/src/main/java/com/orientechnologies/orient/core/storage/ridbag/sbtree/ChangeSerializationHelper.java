package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tglman on 15/06/17.
 */
public class ChangeSerializationHelper {
  public static final ChangeSerializationHelper INSTANCE = new ChangeSerializationHelper();

  public static Change createChangeInstance(byte type, int value) {
    switch (type) {
    case AbsoluteChange.TYPE:
      return new AbsoluteChange(value);
    case DiffChange.TYPE:
      return new DiffChange(value);
    default:
      throw new IllegalArgumentException("Change type is incorrect");
    }
  }

  public Change deserializeChange(final byte[] stream, final int offset) {
    int value = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, offset + OByteSerializer.BYTE_SIZE);
    return createChangeInstance(OByteSerializer.INSTANCE.deserializeLiteral(stream, offset), value);
  }

  public Map<OIdentifiable, Change> deserializeChanges(BytesContainer container) {
    final int count = OVarIntSerializer.readAsInteger(container);
    int offset = container.offset;
    byte[] stream = container.bytes;
//    final int count = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, offset);
//    offset += OIntegerSerializer.INT_SIZE;

    final HashMap<OIdentifiable, Change> res = new HashMap<>();
    for (int i = 0; i < count; i++) {
//      ORecordId rid = OLinkSerializer.INSTANCE.deserialize(stream, offset);
//      offset += OLinkSerializer.RID_SIZE;
      short clusterId = OVarIntSerializer.readAsShort(container);
      long clusterPosition = OVarIntSerializer.readAsLong(container);
      ORecordId rid = new ORecordId(clusterId, clusterPosition);
      
      Change change = ChangeSerializationHelper.INSTANCE.deserializeChange(stream, offset);
      offset += Change.SIZE;

      final OIdentifiable identifiable;
      if (rid.isTemporary() && rid.getRecord() != null)
        identifiable = rid.getRecord();
      else
        identifiable = rid;

      res.put(identifiable, change);
    }

    return res;
  }

  public <K extends OIdentifiable> int serializeChanges(Map<K, Change> changes, OBinarySerializer<K> keySerializer, BytesContainer bytes) {    
    OVarIntSerializer.write(bytes, changes.size());

//    int size = getChangesSerializedSize(changes.size());
//    bytes.offset = bytes.alloc(size);

    for (Map.Entry<K, Change> entry : changes.entrySet()) {
      K key = entry.getKey();

      if (key.getIdentity().isTemporary())
        //noinspection unchecked
        key = key.getRecord();
            
      OIdentifiable id = (OIdentifiable)key;
      OVarIntSerializer.write(bytes, id.getIdentity().getClusterId());
      OVarIntSerializer.write(bytes, id.getIdentity().getClusterPosition());
//      }
//      else{
//        bytes.offset = bytes.alloc(OLinkSerializer.RID_SIZE);
//        keySerializer.serialize(key, bytes.bytes, bytes.offset);
//        bytes.offset += keySerializer.getObjectSize(key);
//      }

      bytes.offset = bytes.alloc(Change.SIZE);
      bytes.offset += entry.getValue().serialize(bytes.bytes, bytes.offset);
    }
    return bytes.offset;
  }

//  private int getChangesSerializedSize(int changesCount) {
//    int size = 0;
//    if (ODatabaseRecordThreadLocal.instance().get().getStorage() instanceof OStorageProxy
//        || ORecordSerializationContext.getContext() == null)      
//      size += changesCount * (OLinkSerializer.RID_SIZE + Change.SIZE);
//    //one int more for size
////    size += OIntegerSerializer.INT_SIZE;
//    return size;
//  }
}
