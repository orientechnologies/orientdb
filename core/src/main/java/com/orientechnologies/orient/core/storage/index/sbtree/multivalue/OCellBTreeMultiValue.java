package com.orientechnologies.orient.core.storage.index.sbtree.multivalue;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface OCellBTreeMultiValue<K> {
  void create(OAtomicOperation atomicOperation, OBinarySerializer<K> keySerializer, OType[] keyTypes, int keySize, OEncryption encryption) throws IOException;

  List<ORID> get(K key);

  void put(OAtomicOperation atomicOperation, K key, ORID value) throws IOException;

  void close();

  void clear(OAtomicOperation atomicOperation) throws IOException;

  void delete(OAtomicOperation atomicOperation) throws IOException;

  void deleteWithoutLoad(OAtomicOperation atomicOperation) throws IOException;

  void load(String name, int keySize, OType[] keyTypes, OBinarySerializer<K> keySerializer, OEncryption encryption);

  long size();

  boolean remove(OAtomicOperation atomicOperation, K key) throws IOException;

  boolean remove(OAtomicOperation atomicOperation, K key, ORID value) throws IOException;

  OCellBTreeCursor<K, ORID> iterateEntriesMinor(K key, boolean inclusive, boolean ascSortOrder);

  OCellBTreeCursor<K, ORID> iterateEntriesMajor(K key, boolean inclusive, boolean ascSortOrder);

  K firstKey();

  K lastKey();

  OCellBTreeKeyCursor<K> keyCursor();

  OCellBTreeCursor<K, ORID> iterateEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive,
      boolean ascSortOrder);

  void acquireAtomicExclusiveLock();

  public interface OCellBTreeCursor<K2, V> {
    Map.Entry<K2, V> next(int prefetchSize);
  }

  public interface OCellBTreeKeyCursor<K2> {
    K2 next(int prefetchSize);
  }
}
