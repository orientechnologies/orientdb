package com.orientechnologies.orient.core.storage.index.sbtreebonsai.local;

import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

public abstract class OSBTreeBonsaiAbstract<K, V> extends ODurableComponent implements OSBTreeBonsaiLocal<K, V> {
  public OSBTreeBonsaiAbstract(String name, String dataFileExtension, OAbstractPaginatedStorage storage) {
    super(storage, name, dataFileExtension, name + dataFileExtension);
  }
}
