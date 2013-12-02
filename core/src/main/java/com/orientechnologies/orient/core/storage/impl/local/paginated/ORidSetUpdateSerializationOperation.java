package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridset.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;

import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 11/26/13
 */
public class ORidSetUpdateSerializationOperation implements ORecordSerializationOperation {
  private final NavigableMap<OIdentifiable, OModifiableInteger> changedValues;

  private final OBonsaiBucketPointer                            rootPointer;

  private final OSBTreeCollectionManager                        collectionManager;

  public ORidSetUpdateSerializationOperation(final NavigableMap<OIdentifiable, OModifiableInteger> changedValues,
      OBonsaiBucketPointer rootPointer) {
    this.changedValues = changedValues;
    this.rootPointer = rootPointer;

    collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();
  }

  @Override
  public void execute(OLocalPaginatedStorage paginatedStorage) {
    OSBTreeBonsai<OIdentifiable, Integer> tree = loadTree();
    try {
      for (Map.Entry<OIdentifiable, OModifiableInteger> entry : changedValues.entrySet()) {
        Integer storedCounter = tree.get(entry.getKey());
        if (storedCounter == null)
          storedCounter = 0;

        storedCounter += entry.getValue().intValue();
        if (storedCounter <= 0)
          tree.remove(entry.getKey());
        else
          tree.put(entry.getKey(), storedCounter);

      }
    } finally {
      releaseTree();
    }

    changedValues.clear();
  }

  private OSBTreeBonsai<OIdentifiable, Integer> loadTree() {
    return collectionManager.loadSBTree(rootPointer);
  }

  private void releaseTree() {
    collectionManager.releaseSBTree(rootPointer);
  }

}