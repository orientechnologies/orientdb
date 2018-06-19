package com.orientechnologies.orient.core.storage.index.hashindex.local;

import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

public abstract class OLocalHashTableAbstract<K, V> extends ODurableComponent implements OHashTable<K, V> {
  public OLocalHashTableAbstract(OAbstractPaginatedStorage storage, String name, String extension, String lockName) {
    super(storage, name, extension, lockName);
  }
}
