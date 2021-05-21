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
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeRidBag;
import java.io.IOException;

public class ORidBagDeleteSerializationOperation implements ORecordSerializationOperation {
  private final OBonsaiCollectionPointer collectionPointer;

  private final OSBTreeCollectionManager collectionManager;
  private final OSBTreeRidBag ridBag;
  private Runnable deleteTask;

  public ORidBagDeleteSerializationOperation(
      OBonsaiCollectionPointer collectionPointer, OSBTreeRidBag ridBag) {
    this.collectionPointer = collectionPointer;
    this.ridBag = ridBag;
    collectionManager = ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager();
  }

  @Override
  public void execute(
      final OAtomicOperation atomicOperation, final OAbstractPaginatedStorage paginatedStorage) {
    OSBTreeBonsai<OIdentifiable, Integer> treeBonsai = loadTree();
    try {
      treeBonsai.markToDelete(atomicOperation);
    } catch (IOException e) {
      throw OException.wrapException(new ODatabaseException("Error during ridbag deletion"), e);
    } finally {
      releaseTree();
    }

    paginatedStorage.tryToDeleteTreeRidBag(ridBag);
  }

  private OSBTreeBonsai<OIdentifiable, Integer> loadTree() {
    return collectionManager.loadSBTree(collectionPointer);
  }

  private void releaseTree() {
    collectionManager.releaseSBTree(collectionPointer);
  }
}
