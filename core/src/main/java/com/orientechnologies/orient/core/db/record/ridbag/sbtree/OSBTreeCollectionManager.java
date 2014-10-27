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

package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import java.util.Map;
import java.util.UUID;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;

public interface OSBTreeCollectionManager {
  public OSBTreeBonsai<OIdentifiable, Integer> createAndLoadTree(int clusterId);

  OBonsaiCollectionPointer createSBTree(int clusterId, UUID ownerUUID);

  public OSBTreeBonsai<OIdentifiable, Integer> loadSBTree(OBonsaiCollectionPointer collectionPointer);

  public void releaseSBTree(OBonsaiCollectionPointer collectionPointer);

  public void delete(OBonsaiCollectionPointer collectionPointer);

  UUID listenForChanges(ORidBag collection);

  void updateCollectionPointer(UUID uuid, OBonsaiCollectionPointer pointer);

  void clearPendingCollections();

  Map<UUID, OBonsaiCollectionPointer> changedIds();

  void clearChangedIds();
}
