package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;

public class ORidBagDeleteSerializationOperation implements ORecordSerializationOperation {
  private final OBonsaiCollectionPointer collectionPointer;

  private final OSBTreeCollectionManager collectionManager;
  private final OSBTreeRidBag            ridBag;

  public ORidBagDeleteSerializationOperation(OBonsaiCollectionPointer collectionPointer, OSBTreeRidBag ridBag) {
    this.collectionPointer = collectionPointer;
    this.ridBag = ridBag;
    collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();
  }

  @Override
  public void execute(OLocalPaginatedStorage paginatedStorage) {
    OSBTreeBonsai<OIdentifiable, Integer> treeBonsai = loadTree();
    try {
      treeBonsai.delete();
    } finally {
      releaseTree();
    }

    collectionManager.delete(collectionPointer);
    ridBag.confirmDelete();
  }

  private OSBTreeBonsai<OIdentifiable, Integer> loadTree() {
    return collectionManager.loadSBTree(collectionPointer);
  }

  private void releaseTree() {
    collectionManager.releaseSBTree(collectionPointer);
  }
}
