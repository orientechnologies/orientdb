/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManagerAbstract;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OSBTreeCollectionManagerRemote extends OSBTreeCollectionManagerAbstract {

  private volatile ThreadLocal<Map<UUID, WeakReference<ORidBag>>> pendingCollections = new PendingCollectionsThreadLocal();

  public OSBTreeCollectionManagerRemote(OStorage storage) {
    super(storage);
  }

  @Override
  public void onShutdown() {
    pendingCollections = null;
    super.onShutdown();
  }

  @Override
  public void onStartup() {
    super.onStartup();
    if (pendingCollections == null)
      pendingCollections = new PendingCollectionsThreadLocal();
  }

  @Override
  protected OSBTreeBonsai<OIdentifiable, Integer> createEdgeTree(final int clusterId) {
    throw new UnsupportedOperationException("Creation of SB-Tree from remote storage is not allowed");
  }

  @Override
  protected OSBTreeBonsai<OIdentifiable, Integer> loadTree(OBonsaiCollectionPointer collectionPointer) {
    throw new UnsupportedOperationException();
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
