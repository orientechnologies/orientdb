package com.orientechnologies.orient.core.index.engine.v1;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.IndexCursor;
import com.orientechnologies.orient.core.index.IndexKeySpliterator;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.engine.OSingleValueIndexEngine;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.OCellBTreeSingleValue;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1.CellBTreeSingleValueV1;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueV3;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;
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
    } else if (version == 3) {
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
    final Spliterator<Object> spliterator = sbTree.keySpliterator();
    StreamSupport.stream(spliterator, false).forEach((key) -> {
      try {
        sbTree.remove(key);
      } catch (IOException e) {
        throw OException.wrapException(new OIndexException("Can not clear index"), e);
      }
    });

    sbTree.remove(null);
  }

  @Override
  public void load(String indexName, final int keySize, final OType[] keyTypes, final OBinarySerializer keySerializer,
      final OEncryption encryption) {
    //noinspection unchecked
    sbTree.load(indexName, keySize, keyTypes, keySerializer, encryption);
  }

  @Override
  public boolean contains(Object key) {
    return sbTree.get(key) != null;
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
  public ORID get(Object key) {
    return sbTree.get(key);
  }

  @Override
  public IndexCursor cursor(ValuesTransformer valuesTransformer) {
    final Object firstKey = sbTree.firstKey();
    if (firstKey == null) {
      return new NullCursor();
    }

    return new TreeIndexCursor(sbTree.iterateEntriesMajor(firstKey, true, true), valuesTransformer);
  }

  @Override
  public IndexCursor descCursor(ValuesTransformer valuesTransformer) {
    final Object lastKey = sbTree.lastKey();
    if (lastKey == null) {
      return new NullCursor();
    }

    return new TreeIndexCursor(sbTree.iterateEntriesMinor(lastKey, true, false), valuesTransformer);
  }

  @Override
  public IndexKeySpliterator keyCursor() {
    return new IndexKeySpliterator() {
      private final Spliterator<Object> treeKeySpliterator = sbTree.keySpliterator();

      @Override
      public boolean tryAdvance(Consumer<? super Object> action) {
        return treeKeySpliterator.tryAdvance(action);
      }

      @Override
      public Spliterator<Object> trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return treeKeySpliterator.estimateSize();
      }

      @Override
      public int characteristics() {
        return NONNULL | ORDERED;
      }
    };
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
  public Object getFirstKey() {
    return sbTree.firstKey();
  }

  @Override
  public Object getLastKey() {
    return sbTree.lastKey();
  }

  @Override
  public IndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer) {
    return new TreeIndexCursor(sbTree.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder),
        transformer);
  }

  @Override
  public IndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return new TreeIndexCursor(sbTree.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder), transformer);
  }

  @Override
  public IndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return new TreeIndexCursor(sbTree.iterateEntriesMinor(toKey, isInclusive, ascSortOrder), transformer);
  }

  @Override
  public long size(final ValuesTransformer transformer) {
    if (transformer != null) {
      throw new IllegalStateException("Transformer is not allowed in index which does not support multiple values");
    }

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

  private static final class TreeIndexCursor implements IndexCursor {
    private final Spliterator<ORawPair<Object, ORID>> treeSpliterator;
    private final ValuesTransformer                   valuesTransformer;

    private Iterator<ORID> currentIterator = OEmptyIterator.IDENTIFIABLE_INSTANCE;
    private Object         currentKey      = null;

    private TreeIndexCursor(Spliterator<ORawPair<Object, ORID>> treeSpliterator, ValuesTransformer valuesTransformer) {
      this.treeSpliterator = treeSpliterator;
      this.valuesTransformer = valuesTransformer;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
      if (valuesTransformer == null) {
        return treeSpliterator.tryAdvance(action);
      }

      if (currentIterator == null) {
        return false;
      }

      if (currentIterator.hasNext()) {
        action.accept(new ORawPair<>(currentKey, currentIterator.next()));
        return true;
      }

      while (true) {
        @SuppressWarnings("ObjectAllocationInLoop")
        final Object[] next = new Object[1];

        @SuppressWarnings("ObjectAllocationInLoop")
        final boolean result = treeSpliterator.tryAdvance((pair) -> {
          currentKey = pair.first;
          next[0] = pair.second;
        });

        if (!result) {
          return false;
        }

        currentIterator = valuesTransformer.transformFromValue(next[0]).iterator();
        if (currentIterator.hasNext()) {
          action.accept(new ORawPair<>(currentKey, currentIterator.next()));
          return true;
        }
      }
    }

    @Override
    public Spliterator<ORawPair<Object, ORID>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
      return NONNULL | ORDERED;
    }
  }

  private static class NullCursor implements IndexCursor {
    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
      return false;
    }

    @Override
    public Spliterator<ORawPair<Object, ORID>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return 0;
    }

    @Override
    public int characteristics() {
      return 0;
    }
  }
}
