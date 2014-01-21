package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsaiRemote;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeCollectionManagerRemote extends OSBTreeCollectionManagerAbstract {

  public OSBTreeCollectionManagerRemote() {
    super();
  }

  @Override
  protected OSBTreeBonsaiRemote<OIdentifiable, Integer> createTree(String extension) {
    return new OSBTreeBonsaiRemote<OIdentifiable, Integer>();
  }
}
