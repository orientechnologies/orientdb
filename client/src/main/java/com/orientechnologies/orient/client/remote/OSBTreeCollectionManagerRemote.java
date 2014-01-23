package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManagerAbstract;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeCollectionManagerRemote extends OSBTreeCollectionManagerAbstract {

  public OSBTreeCollectionManagerRemote() {
    super();
  }

  @Override
  protected OSBTreeBonsaiRemote<OIdentifiable, Integer> createTree(int clusterId) {
    // TODO create tree on other side
    OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();

    // storage.beginRequest()

    return new OSBTreeBonsaiRemote<OIdentifiable, Integer>(null);
  }

  @Override
  protected OSBTreeBonsai<OIdentifiable, Integer> loadTree(OBonsaiCollectionPointer collectionPointer) {
    // TODO load remote tree
    OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();

    return new OSBTreeBonsaiRemote<OIdentifiable, Integer>(collectionPointer);
  }
}
