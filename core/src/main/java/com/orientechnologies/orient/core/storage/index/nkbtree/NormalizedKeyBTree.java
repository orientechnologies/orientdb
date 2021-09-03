package com.orientechnologies.orient.core.storage.index.nkbtree;

import com.orientechnologies.orient.core.index.OCompositeKey;

public interface NormalizedKeyBTree<K> {
  byte[] get(final OCompositeKey key);

  void put(final OCompositeKey key, final byte[] value);
}
