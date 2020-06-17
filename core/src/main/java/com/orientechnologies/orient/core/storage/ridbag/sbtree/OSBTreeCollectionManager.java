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

package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public interface OSBTreeCollectionManager {
  OSBTreeBonsai<OIdentifiable, Integer> createAndLoadTree(
      OAtomicOperation atomicOperation, int clusterId) throws IOException;

  OBonsaiCollectionPointer createSBTree(
      int clusterId, OAtomicOperation atomicOperation, UUID ownerUUID) throws IOException;

  OSBTreeBonsai<OIdentifiable, Integer> loadSBTree(OBonsaiCollectionPointer collectionPointer);

  void releaseSBTree(OBonsaiCollectionPointer collectionPointer);

  void delete(OBonsaiCollectionPointer collectionPointer);

  UUID listenForChanges(ORidBag collection);

  void updateCollectionPointer(UUID uuid, OBonsaiCollectionPointer pointer);

  void clearPendingCollections();

  Map<UUID, OBonsaiCollectionPointer> changedIds();

  void clearChangedIds();
}
