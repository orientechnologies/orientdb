package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridset.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;

import java.util.NavigableSet;
import java.util.Set;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 11/26/13
 */
public class ORidSetUpdateSerializationOperation implements ORecordSerializationOperation {
  private final NavigableSet<OIdentifiable> addValues;
  private final Set<OIdentifiable>          removedValues;

  private final boolean                     clear;
  private final OBonsaiBucketPointer        rootPointer;

  private final OSBTreeCollectionManager    collectionManager;

  public ORidSetUpdateSerializationOperation(NavigableSet<OIdentifiable> addValues, Set<OIdentifiable> removedValues,
      boolean clear, OBonsaiBucketPointer rootPointer) {
    this.addValues = addValues;
    this.removedValues = removedValues;
    this.clear = clear;
    this.rootPointer = rootPointer;

    collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();
  }

  @Override
  public void execute(OLocalPaginatedStorage paginatedStorage) {
    OSBTreeBonsai<OIdentifiable, Boolean> tree = loadTree();
    try {
      if (clear)
        tree.clear();

      for (OIdentifiable identifiable : addValues)
        tree.put(identifiable, true);

      for (OIdentifiable identifiable : removedValues)
        tree.remove(identifiable);
    } finally {
      releaseTree();
    }
  }

  private OSBTreeBonsai<OIdentifiable, Boolean> loadTree() {
    return collectionManager.loadSBTree(rootPointer);
  }

  private void releaseTree() {
    collectionManager.releaseSBTree(rootPointer);
  }

}