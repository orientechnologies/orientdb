package com.orientechnologies.orient.core.index.engine.v1;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.engine.OSingleValueIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.OCellBTreeSingleValue;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1.CellBTreeSingleValueV1;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueV3;

import java.io.IOException;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class OCellBTreeSingleValueIndexEngine implements OSingleValueIndexEngine, OCellBTreeIndexEngine {
  private static final String DATA_FILE_EXTENSION        = ".cbt";
  private static final String NULL_BUCKET_FILE_EXTENSION = ".nbt";

  private final OCellBTreeSingleValue<Object> sbTree;
  private final String                        name;
  private final int                           id;

  public OCellBTreeSingleValueIndexEngine(int id, String name, OAbstractPaginatedStorage storage, int version) {
    this.name = name;
    this.id = id;

    if (version < 3) {
      this.sbTree = new CellBTreeSingleValueV1<>(name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
    } else if (version == 3 || version == 4) {
      this.sbTree = new CellBTreeSingleValueV3<>(name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
    } else {
      throw new IllegalStateException("Invalid tree version " + version);
    }
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public void init(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata) {
  }

  @Override
  public void flush() {
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void create(OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes, boolean nullPointerSupport,
      OBinarySerializer keySerializer, int keySize, Map<String, String> engineProperties, OEncryption encryption) {
    try {
      //noinspection unchecked
      sbTree.create(keySerializer, keyTypes, keySize, encryption);
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error of creation of index " + name), e);
    }
  }

  @Override
  public void delete() {
    try {
      doClearTree();

      sbTree.delete();
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during deletion of index " + name), e);
    }
  }

  private void doClearTree() throws IOException {
    try (Stream<Object> stream = sbTree.keyStream()) {
      stream.forEach((key) -> {
        try {
          sbTree.remove(key);
        } catch (IOException e) {
          throw OException.wrapException(new OIndexException("Can not clear index"), e);
        }
      });
    }

    sbTree.remove(null);
  }

  @Override
  public void load(String indexName, final int keySize, final OType[] keyTypes, final OBinarySerializer keySerializer,
      final OEncryption encryption) {
    //noinspection unchecked
    sbTree.load(indexName, keySize, keyTypes, keySerializer, encryption);
  }

  @Override
  public boolean remove(Object key) {
    try {
      return sbTree.remove(key) != null;
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during removal of key " + key + " from index " + name), e);
    }
  }

  @Override
  public void clear() {
    try {
      doClearTree();
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during clear of index " + name), e);
    }
  }

  @Override
  public void close() {
    sbTree.close();
  }

  @Override
  public Stream<ORID> get(Object key) {
    return Stream.of(sbTree.get(key));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> stream(ValuesTransformer valuesTransformer) {
    final Object firstKey = sbTree.firstKey();
    if (firstKey == null) {
      return emptyStream();
    }

    return sbTree.iterateEntriesMajor(firstKey, true, true);
  }

  private static Stream<ORawPair<Object, ORID>> emptyStream() {
    return StreamSupport.stream(Spliterators.emptySpliterator(), false);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> descStream(ValuesTransformer valuesTransformer) {
    final Object lastKey = sbTree.lastKey();
    if (lastKey == null) {
      return emptyStream();
    }

    return sbTree.iterateEntriesMinor(lastKey, true, false);
  }

  @Override
  public Stream<Object> keyStream() {
    return sbTree.keyStream();
  }

  @Override
  public void put(Object key, ORID value) {
    try {
      sbTree.put(key, value);
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during insertion of key " + key + " into index " + name), e);
    }
  }

  @Override
  public boolean validatedPut(Object key, ORID value, Validator<Object, ORID> validator) {
    try {
      return sbTree.validatedPut(key, value, validator);
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during insertion of key " + key + " into index " + name), e);
    }
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo,
      boolean toInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return sbTree.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer transformer) {
    return sbTree.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer transformer) {
    return sbTree.iterateEntriesMinor(toKey, isInclusive, ascSortOrder);
  }

  @Override
  public long size(final ValuesTransformer transformer) {
    return sbTree.size();
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return true;
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    sbTree.acquireAtomicExclusiveLock();
    return true;
  }

  @Override
  public String getIndexNameByKey(Object key) {
    return name;
  }
}
