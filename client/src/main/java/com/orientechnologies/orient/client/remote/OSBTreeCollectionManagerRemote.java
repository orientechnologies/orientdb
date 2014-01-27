package com.orientechnologies.orient.client.remote;

import java.io.IOException;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManagerAbstract;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeCollectionManagerRemote extends OSBTreeCollectionManagerAbstract {

  private final OCollectionNetworkSerializer networkSerializer;

  public OSBTreeCollectionManagerRemote() {
    super();
    networkSerializer = new OCollectionNetworkSerializer();
  }

  public OSBTreeCollectionManagerRemote(OCollectionNetworkSerializer networkSerializer) {
    super();
    this.networkSerializer = networkSerializer;
  }

  @Override
  protected OSBTreeBonsaiRemote<OIdentifiable, Integer> createTree(int clusterId) {
    OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();

    try {
      OChannelBinaryAsynchClient client = storage.beginRequest(OChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI);
      client.writeInt(clusterId);
      storage.endRequest(client);

      storage.beginResponse(client);
      OBonsaiCollectionPointer pointer = networkSerializer.readCollectionPointer(client);
      storage.endResponse(client);

      return new OSBTreeBonsaiRemote<OIdentifiable, Integer>(pointer);
    } catch (IOException e) {
      throw new ODatabaseException("Can't create sb-tree bonsai.", e);
    }
  }

  @Override
  protected OSBTreeBonsai<OIdentifiable, Integer> loadTree(OBonsaiCollectionPointer collectionPointer) {
    return new OSBTreeBonsaiRemote<OIdentifiable, Integer>(collectionPointer);
  }
}
