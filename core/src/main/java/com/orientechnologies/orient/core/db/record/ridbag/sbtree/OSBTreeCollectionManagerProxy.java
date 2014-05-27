/*
 * Copyright 2010-2013 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OProxedResource;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;

import java.util.Map;
import java.util.UUID;

public class OSBTreeCollectionManagerProxy extends OProxedResource<OSBTreeCollectionManager> implements OSBTreeCollectionManager {
  public OSBTreeCollectionManagerProxy(ODatabaseRecord database, OSBTreeCollectionManager delegate) {
    super(delegate, database);
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> createAndLoadTree(int clusterId) {

    return delegate.createAndLoadTree(clusterId);
  }

  @Override
  public OBonsaiCollectionPointer createSBTree(int clusterId, UUID ownerUUID) {

    return delegate.createSBTree(clusterId, ownerUUID);
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> loadSBTree(OBonsaiCollectionPointer collectionPointer) {

    return delegate.loadSBTree(collectionPointer);
  }

  @Override
  public void releaseSBTree(OBonsaiCollectionPointer collectionPointer) {

    delegate.releaseSBTree(collectionPointer);
  }

  @Override
  public void delete(OBonsaiCollectionPointer collectionPointer) {

    delegate.delete(collectionPointer);
  }

  @Override
  public UUID listenForChanges(ORidBag oIdentifiables) {
    if (delegate == null)
      return null;

    return delegate.listenForChanges(oIdentifiables);
  }

  @Override
  public void updateCollectionPointer(UUID uuid, OBonsaiCollectionPointer pointer) {

    delegate.updateCollectionPointer(uuid, pointer);
  }

  @Override
  public void clearPendingCollections() {

    delegate.clearPendingCollections();
  }

  @Override
  public Map<UUID, OBonsaiCollectionPointer> changedIds() {
    return delegate.changedIds();
  }

  @Override
  public void clearChangedIds() {

    delegate.clearChangedIds();
  }
}
