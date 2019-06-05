package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;

import java.io.IOException;
import java.util.Map;

public interface OSBTree<K, V> {
  void create(OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer, OType[] keyTypes, int keySize,
      boolean nullPointerSupport, OEncryption encryption) throws IOException;

  boolean isNullPointerSupport();

  V get(K key);

  void put(K key, V value) throws IOException;

  boolean validatedPut(K key, V value, OBaseIndexEngine.Validator<K, V> validator) throws IOException;

  boolean update(K key, OIndexKeyUpdater<V> updater, OBaseIndexEngine.Validator<K, V> validator)
      throws IOException;

  void close(boolean flush);

  void close();

  void clear() throws IOException;

  void delete() throws IOException;

  void deleteWithoutLoad() throws IOException;

  void load(String name, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer, OType[] keyTypes, int keySize,
      boolean nullPointerSupport, OEncryption encryption);

  long size();

  V remove(K key) throws IOException;

  OSBTreeCursor<K, V> iterateEntriesMinor(K key, boolean inclusive, boolean ascSortOrder);

  OSBTreeCursor<K, V> iterateEntriesMajor(K key, boolean inclusive, boolean ascSortOrder);

  K firstKey();

  K lastKey();

  OSBTreeKeyCursor<K> keyCursor();

  OSBTreeCursor<K, V> iterateEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, boolean ascSortOrder);

  void flush();

  void acquireAtomicExclusiveLock();

  interface OSBTreeCursor<K2, V2> {
    Map.Entry<K2, V2> next(int prefetchSize);
  }

  interface OSBTreeKeyCursor<K2> {
    K2 next(int prefetchSize);
  }
}
