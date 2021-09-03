package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.IOException;
import java.util.stream.Stream;

public interface OSBTree<K, V> {
  void create(
      OAtomicOperation atomicOperation,
      OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer,
      OType[] keyTypes,
      int keySize,
      boolean nullPointerSupport,
      OEncryption encryption)
      throws IOException;

  boolean isNullPointerSupport();

  V get(K key);

  void put(OAtomicOperation atomicOperation, K key, V value) throws IOException;

  boolean validatedPut(
      OAtomicOperation atomicOperation, K key, V value, OBaseIndexEngine.Validator<K, V> validator)
      throws IOException;

  boolean update(
      OAtomicOperation atomicOperation,
      K key,
      OIndexKeyUpdater<V> updater,
      OBaseIndexEngine.Validator<K, V> validator)
      throws IOException;

  void close(boolean flush);

  void close();

  void delete(OAtomicOperation atomicOperation) throws IOException;

  void load(
      String name,
      OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer,
      OType[] keyTypes,
      int keySize,
      boolean nullPointerSupport,
      OEncryption encryption);

  long size();

  V remove(OAtomicOperation atomicOperation, K key) throws IOException;

  Stream<ORawPair<K, V>> iterateEntriesMinor(K key, boolean inclusive, boolean ascSortOrder);

  Stream<ORawPair<K, V>> iterateEntriesMajor(K key, boolean inclusive, boolean ascSortOrder);

  K firstKey();

  K lastKey();

  Stream<K> keyStream();

  Stream<ORawPair<K, V>> iterateEntriesBetween(
      K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, boolean ascSortOrder);

  void flush();

  void acquireAtomicExclusiveLock();
}
