package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;

public class ORidBagDeleteSerializationOperation implements ORecordSerializationOperation {
  private final OBonsaiBucketPointer     rootPointer;

  private final OSBTreeCollectionManager collectionManager;

  public ORidBagDeleteSerializationOperation(OBonsaiBucketPointer rootPointer) {
    this.rootPointer = rootPointer;
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

    collectionManager.delete(rootPointer);
  }

  private OSBTreeBonsai<OIdentifiable, Integer> loadTree() {
    return collectionManager.loadSBTree(rootPointer);
  }

  private void releaseTree() {
    collectionManager.releaseSBTree(rootPointer);
  }
}
