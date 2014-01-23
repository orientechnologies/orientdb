package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsaiRemote;

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
    return new OSBTreeBonsaiRemote<OIdentifiable, Integer>();
  }

  @Override
  protected OSBTreeBonsai<OIdentifiable, Integer> loadTree(OBonsaiCollectionPointer collectionPointer) {
    // TODO load remote tree
    return new OSBTreeBonsaiRemote<OIdentifiable, Integer>();
  }
}
