package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;
import java.util.Map;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OCollectionNetworkSerializer;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag.Change;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OSBTGetRealBagSizeRequest<K, V> implements OBinaryRequest {

  private final OBonsaiCollectionPointer collectionPointer;
  private final Map<K, Change>           changes;
  private OBinarySerializer<K>           keySerializer;

  public OSBTGetRealBagSizeRequest(OBinarySerializer<K> keySerializer, OBonsaiCollectionPointer collectionPointer,
      Map<K, Change> changes) {
    this.collectionPointer = collectionPointer;
    this.changes = changes;
    this.keySerializer = keySerializer;
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(network, collectionPointer);
    final OSBTreeRidBag.ChangeSerializationHelper changeSerializer = OSBTreeRidBag.ChangeSerializationHelper.INSTANCE;
    final byte[] stream = new byte[OIntegerSerializer.INT_SIZE + changeSerializer.getChangesSerializedSize(changes.size())];
    changeSerializer.serializeChanges(changes, keySerializer, stream, 0);
    network.writeBytes(stream);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE;
  }
}