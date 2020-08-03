package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.IOException;
import java.util.stream.Stream;

public interface OCellBTreeSingleValue<K> {
  void create(
      OAtomicOperation atomicOperation,
      OBinarySerializer<K> keySerializer,
      OType[] keyTypes,
      int keySize,
      OEncryption encryption)
      throws IOException;

  ORID get(K key);

  void put(OAtomicOperation atomicOperation, K key, ORID value) throws IOException;

  boolean validatedPut(
      OAtomicOperation atomicOperation,
      K key,
      ORID value,
      OBaseIndexEngine.Validator<K, ORID> validator)
      throws IOException;

  void close();

  void delete(OAtomicOperation atomicOperation) throws IOException;

  void load(
      String name,
      int keySize,
      OType[] keyTypes,
      OBinarySerializer<K> keySerializer,
      OEncryption encryption);

  long size();

  ORID remove(OAtomicOperation atomicOperation, K key) throws IOException;

  Stream<ORawPair<K, ORID>> iterateEntriesMinor(K key, boolean inclusive, boolean ascSortOrder);

  Stream<ORawPair<K, ORID>> iterateEntriesMajor(K key, boolean inclusive, boolean ascSortOrder);

  K firstKey();

  K lastKey();

  Stream<K> keyStream();

  Stream<ORawPair<K, ORID>> allEntries();

  Stream<ORawPair<K, ORID>> iterateEntriesBetween(
      K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, boolean ascSortOrder);

  void acquireAtomicExclusiveLock();
}
