package com.orientechnologies.orient.core.storage.index.learned;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.index.engine.OIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

public class MyIndexEngine implements OIndexEngine {
  @Override
  public Object get(Object key) {
    return null;
  }

  @Override
  public void put(OAtomicOperation atomicOperation, Object key, Object value) throws IOException {}

  @Override
  public void update(OAtomicOperation atomicOperation, Object key, OIndexKeyUpdater<Object> updater)
      throws IOException {}

  @Override
  public boolean remove(OAtomicOperation atomicOperation, Object key) throws IOException {
    return false;
  }

  @Override
  public boolean validatedPut(
      OAtomicOperation atomicOperation, Object key, ORID value, Validator<Object, ORID> validator)
      throws IOException {
    return false;
  }

  @Override
  public void load(
      String indexName,
      OBinarySerializer valueSerializer,
      boolean isAutomatic,
      OBinarySerializer keySerializer,
      OType[] keyTypes,
      boolean nullPointerSupport,
      int keySize,
      Map<String, String> engineProperties,
      OEncryption encryption) {}

  @Override
  public int getId() {
    return 0;
  }

  @Override
  public void init(
      String indexName,
      String indexType,
      OIndexDefinition indexDefinition,
      boolean isAutomatic,
      ODocument metadata) {}

  @Override
  public void flush() {}

  @Override
  public void create(
      OAtomicOperation atomicOperation,
      OBinarySerializer valueSerializer,
      boolean isAutomatic,
      OType[] keyTypes,
      boolean nullPointerSupport,
      OBinarySerializer keySerializer,
      int keySize,
      Map<String, String> engineProperties,
      OEncryption encryption)
      throws IOException {}

  @Override
  public void delete(OAtomicOperation atomicOperation) throws IOException {}

  @Override
  public void clear(OAtomicOperation atomicOperation) throws IOException {}

  @Override
  public void close() {}

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesBetween(
      Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      ValuesTransformer transformer) {
    return null;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMajor(
      Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return null;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMinor(
      Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return null;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> stream(ValuesTransformer valuesTransformer) {
    return null;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> descStream(ValuesTransformer valuesTransformer) {
    return null;
  }

  @Override
  public Stream<Object> keyStream() {
    return null;
  }

  @Override
  public long size(ValuesTransformer transformer) {
    return 0;
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    return false;
  }

  @Override
  public String getIndexNameByKey(Object key) {
    return null;
  }

  @Override
  public void updateUniqueIndexVersion(Object key) {}

  @Override
  public int getUniqueIndexVersion(Object key) {
    return 0;
  }
}
