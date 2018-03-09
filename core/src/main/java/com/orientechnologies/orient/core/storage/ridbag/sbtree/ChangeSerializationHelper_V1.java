package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tglman on 15/06/17.
 */
public class ChangeSerializationHelper_V1 extends ChangeSerializationHelper{  

  private Change deserializeChange(final byte[] stream, final int offset) {
    int value = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, offset + OByteSerializer.BYTE_SIZE);
    return createChangeInstance(OByteSerializer.INSTANCE.deserializeLiteral(stream, offset), value);
  }

  @Override
  public Map<OIdentifiable, Change> deserializeChanges(BytesContainer container) {
    final int count = OVarIntSerializer.readAsInteger(container);    
    byte[] stream = container.bytes;

    final HashMap<OIdentifiable, Change> res = new HashMap<>();
    for (int i = 0; i < count; i++) {
      short clusterId = OVarIntSerializer.readAsShort(container);
      long clusterPosition = OVarIntSerializer.readAsLong(container);      
      
      ORecordId rid = new ORecordId(clusterId, clusterPosition);
      
      Change change = deserializeChange(stream, container.offset);
      container.offset += Change.SIZE;
           
      final OIdentifiable identifiable;
      if (rid.isTemporary() && rid.getRecord() != null)
        identifiable = rid.getRecord();
      else
        identifiable = rid;

      res.put(identifiable, change);
    }

    return res;
  }

  @Override
  public <K extends OIdentifiable> int serializeChanges(Map<K, Change> changes, OBinarySerializer<K> keySerializer, BytesContainer bytes) {    
    OVarIntSerializer.write(bytes, changes.size());

    //lets allocate these bytes for sure they will be needed in deserialization process
    int size = getChangesSerializedSize(changes.size());
    bytes.offset = bytes.alloc(size);

    for (Map.Entry<K, Change> entry : changes.entrySet()) {
      K key = entry.getKey();

      if (key.getIdentity().isTemporary())
        //noinspection unchecked
        key = key.getRecord();
            
      OIdentifiable id = (OIdentifiable)key;
      OVarIntSerializer.write(bytes, id.getIdentity().getClusterId());
      OVarIntSerializer.write(bytes, id.getIdentity().getClusterPosition());

      //check for additional space
      bytes.offset = bytes.alloc(Change.SIZE);
      bytes.offset += entry.getValue().serialize(bytes.bytes, bytes.offset);
    }
    return bytes.offset;
  }

  @Override
  protected int getChangesSerializedSize(int changesCount) {
    int size = 0;
    if (ODatabaseRecordThreadLocal.instance().get().getStorage() instanceof OStorageProxy
        || ORecordSerializationContext.getContext() == null)      
      size += changesCount * Change.SIZE;    
    return size;
  }
}
