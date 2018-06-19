package com.orientechnologies.orient.core.storage.index.hashindex.local;

import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;

public interface OLocalHashTable<K, V> extends OHashTable<K, V> {
  int HASH_CODE_SIZE = 64;

  void deleteRollback(final OAtomicOperation atomicOperation);

  void putRollback(final byte[] rawKey, final byte[] rawValue, final OAtomicOperation atomicOperation);

  void removeRollback(final byte[] rawKey, final OAtomicOperation atomicOperation);
}
