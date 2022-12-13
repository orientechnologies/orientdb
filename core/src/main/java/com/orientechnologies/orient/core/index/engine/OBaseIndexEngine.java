package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

public interface OBaseIndexEngine {
  int getId();

  void init(
      String indexName,
      String indexType,
      OIndexDefinition indexDefinition,
      boolean isAutomatic,
      ODocument metadata);

  void flush();

  void create(
      OAtomicOperation atomicOperation,
      OBinarySerializer valueSerializer,
      boolean isAutomatic,
      OType[] keyTypes,
      boolean nullPointerSupport,
      OBinarySerializer keySerializer,
      int keySize,
      Map<String, String> engineProperties,
      OEncryption encryption)
      throws IOException;

  void delete(OAtomicOperation atomicOperation) throws IOException;

  void clear(OAtomicOperation atomicOperation) throws IOException;

  void close();

  Stream<ORawPair<Object, ORID>> iterateEntriesBetween(
      Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      ValuesTransformer transformer);

  Stream<ORawPair<Object, ORID>> iterateEntriesMajor(
      Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer);

  Stream<ORawPair<Object, ORID>> iterateEntriesMinor(
      final Object toKey,
      final boolean isInclusive,
      boolean ascSortOrder,
      ValuesTransformer transformer);

  Stream<ORawPair<Object, ORID>> stream(ValuesTransformer valuesTransformer);

  Stream<ORawPair<Object, ORID>> descStream(ValuesTransformer valuesTransformer);

  Stream<Object> keyStream();

  long size(ValuesTransformer transformer);

  boolean hasRangeQuerySupport();

  int getEngineAPIVersion();

  String getName();

  /**
   * Acquires exclusive lock in the active atomic operation running on the current thread for this
   * index engine.
   *
   * <p>
   *
   * <p>If this index engine supports a more narrow locking, for example key-based sharding, it may
   * use the provided {@code key} to infer a more narrow lock scope, but that is not a requirement.
   *
   * @param key the index key to lock.
   * @return {@code true} if this index was locked entirely, {@code false} if this index locking is
   *     sensitive to the provided {@code key} and only some subset of this index was locked.
   */
  boolean acquireAtomicExclusiveLock(Object key);

  String getIndexNameByKey(Object key);

  void updateUniqueIndexVersion(Object key);

  int getUniqueIndexVersion(Object key);

  default boolean hasRidBagTreesSupport() {
    return false;
  }

  interface ValuesTransformer {
    Collection<ORID> transformFromValue(Object value);
  }

  /**
   * Put operation validator.
   *
   * @param <K> the key type.
   * @param <V> the value type.
   */
  interface Validator<K, V> {

    /**
     * Indicates that a put request should be silently ignored by the store.
     *
     * @see #validate(Object, Object, Object)
     */
    Object IGNORE = new Object();

    /**
     * Validates the put operation for the given key, the old value and the new value. May throw an
     * exception to abort the current put operation with an error.
     *
     * @param key the put operation key.
     * @param oldValue the old value or {@code null} if no value is currently stored.
     * @param newValue the new value passed to validatedPut(Object, OIdentifiable, Validator).
     * @return the new value to store, may differ from the passed one, or the special {@link
     *     #IGNORE} value to silently ignore the put operation request being processed.
     */
    Object validate(K key, V oldValue, V newValue);
  }
}
