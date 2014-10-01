package com.orientechnologies.orient.client.remote;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManagerAbstract;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeCollectionManagerRemote extends OSBTreeCollectionManagerAbstract {

  private final OCollectionNetworkSerializer             networkSerializer;
  private boolean                                        remoteCreationAllowed = false;

  private ThreadLocal<Map<UUID, WeakReference<ORidBag>>> pendingCollections    = new ThreadLocal<Map<UUID, WeakReference<ORidBag>>>() {
                                                                                 @Override
                                                                                 protected Map<UUID, WeakReference<ORidBag>> initialValue() {
                                                                                   return new HashMap<UUID, WeakReference<ORidBag>>();
                                                                                 }
                                                                               };

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
    if (remoteCreationAllowed) {
      OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();
      OChannelBinaryAsynchClient client = null;
      try {
        client = storage.beginRequest(OChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI);
        client.writeInt(clusterId);
        storage.endRequest(client);
        OBonsaiCollectionPointer pointer;
        try {
          storage.beginResponse(client);
          pointer = networkSerializer.readCollectionPointer(client);
        } finally {
          storage.endResponse(client);
        }

        OBinarySerializer<OIdentifiable> keySerializer = OLinkSerializer.INSTANCE;
        OBinarySerializer<Integer> valueSerializer = OIntegerSerializer.INSTANCE;

        return new OSBTreeBonsaiRemote<OIdentifiable, Integer>(pointer, keySerializer, valueSerializer);
      } catch (IOException e) {
        storage.getEngine().getConnectionManager().remove(client);
        throw new ODatabaseException("Can't create sb-tree bonsai.", e);
      } catch (RuntimeException e2) {
        storage.getEngine().getConnectionManager().release(client);
        throw e2;
      }
    } else {
      throw new UnsupportedOperationException("Creation of SB-Tree from remote storage is not allowed");
    }
  }

  @Override
  protected OSBTreeBonsai<OIdentifiable, Integer> loadTree(OBonsaiCollectionPointer collectionPointer) {
    OBinarySerializer<OIdentifiable> keySerializer = OLinkSerializer.INSTANCE;
    OBinarySerializer<Integer> valueSerializer = OIntegerSerializer.INSTANCE;

    return new OSBTreeBonsaiRemote<OIdentifiable, Integer>(collectionPointer, keySerializer, valueSerializer);
  }

  @Override
  public UUID listenForChanges(ORidBag collection) {
    UUID id = collection.getTemporaryId();
    if (id == null)
      id = UUID.randomUUID();

    pendingCollections.get().put(id, new WeakReference<ORidBag>(collection));

    return id;
  }

  @Override
  public void updateCollectionPointer(UUID uuid, OBonsaiCollectionPointer pointer) {
    final WeakReference<ORidBag> reference = pendingCollections.get().get(uuid);
    if (reference == null) {
      OLogManager.instance().warn(this, "Update of collection pointer is received but collection is not registered");
      return;
    }

    final ORidBag collection = reference.get();

    if (collection != null) {
      collection.notifySaved(pointer);
    }
  }

  @Override
  public void clearPendingCollections() {
    pendingCollections.get().clear();
  }

  @Override
  public Map<UUID, OBonsaiCollectionPointer> changedIds() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearChangedIds() {
    throw new UnsupportedOperationException();
  }
}
