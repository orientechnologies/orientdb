package com.orientechnologies.orient.core.storage.index.nkbtree;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

public class NormalizedKeyBTreeValue<K> extends ODurableComponent implements NormalizedKeyBTree<K> {
  private final String nullFileExtension;

  public NormalizedKeyBTreeValue(
      final String name,
      final String dataFileExtension,
      final String nullFileExtension,
      final OAbstractPaginatedStorage storage) {
    super(storage, name, dataFileExtension, name + dataFileExtension);
    acquireExclusiveLock();
    try {
      this.nullFileExtension = nullFileExtension;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public byte[] get(final OCompositeKey key) {
    return new byte[0];
  }

  @Override
  public void put(final OCompositeKey key, final byte[] value) {}
}
