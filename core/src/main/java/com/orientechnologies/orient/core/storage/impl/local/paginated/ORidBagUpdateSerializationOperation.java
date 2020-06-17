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
package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 11/26/13
 */
public class ORidBagUpdateSerializationOperation implements ORecordSerializationOperation {
  private final NavigableMap<OIdentifiable, Change> changedValues;

  private final OBonsaiCollectionPointer collectionPointer;

  private final OSBTreeCollectionManager collectionManager;

  public ORidBagUpdateSerializationOperation(
      final NavigableMap<OIdentifiable, Change> changedValues,
      OBonsaiCollectionPointer collectionPointer) {
    this.changedValues = changedValues;
    this.collectionPointer = collectionPointer;

    collectionManager = ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager();
  }

  @Override
  public void execute(
      OAtomicOperation atomicOperation, OAbstractPaginatedStorage paginatedStorage) {
    if (changedValues.isEmpty()) {
      return;
    }

    OSBTreeBonsai<OIdentifiable, Integer> tree = loadTree();
    try {
      for (Map.Entry<OIdentifiable, Change> entry : changedValues.entrySet()) {
        Integer storedCounter = tree.get(entry.getKey());

        storedCounter = entry.getValue().applyTo(storedCounter);
        if (storedCounter <= 0) {
          tree.remove(atomicOperation, entry.getKey());
        } else {
          tree.put(atomicOperation, entry.getKey(), storedCounter);
        }
      }
    } catch (IOException e) {
      throw OException.wrapException(new ODatabaseException("Error during ridbag update"), e);
    } finally {
      releaseTree();
    }

    changedValues.clear();
  }

  private OSBTreeBonsai<OIdentifiable, Integer> loadTree() {
    return collectionManager.loadSBTree(collectionPointer);
  }

  private void releaseTree() {
    collectionManager.releaseSBTree(collectionPointer);
  }
}
