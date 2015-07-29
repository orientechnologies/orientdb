/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.client.remote;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.Orient;
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
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OSBTreeCollectionManagerRemote extends OSBTreeCollectionManagerAbstract implements OOrientStartupListener,
    OOrientShutdownListener {

  private final OCollectionNetworkSerializer                      networkSerializer;
  private boolean                                                 remoteCreationAllowed = false;

  private volatile ThreadLocal<Map<UUID, WeakReference<ORidBag>>> pendingCollections    = new PendingCollectionsThreadLocal();

  public OSBTreeCollectionManagerRemote() {
    super();
    networkSerializer = new OCollectionNetworkSerializer();

    Orient.instance().registerWeakOrientStartupListener(this);
    Orient.instance().registerWeakOrientShutdownListener(this);
  }

  public OSBTreeCollectionManagerRemote(OCollectionNetworkSerializer networkSerializer) {
    super();
    this.networkSerializer = networkSerializer;

    Orient.instance().registerWeakOrientStartupListener(this);
    Orient.instance().registerWeakOrientShutdownListener(this);
  }

  @Override
  public void onShutdown() {
    pendingCollections = null;
  }

  @Override
  public void onStartup() {
    if (pendingCollections == null)
      pendingCollections = new PendingCollectionsThreadLocal();
  }

  @Override
  protected OSBTreeBonsaiRemote<OIdentifiable, Integer> createTree(int clusterId) {
    if (remoteCreationAllowed) {
      OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();
      OChannelBinaryAsynchClient client = null;
      while (true) {
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
        } catch (Exception e2) {
          storage.handleException(client, "Can't create sb-tree bonsai.", e2);
        }
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

  private static class PendingCollectionsThreadLocal extends ThreadLocal<Map<UUID, WeakReference<ORidBag>>> {
    @Override
    protected Map<UUID, WeakReference<ORidBag>> initialValue() {
      return new HashMap<UUID, WeakReference<ORidBag>>();
    }
  }
}
