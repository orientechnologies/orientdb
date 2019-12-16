package com.orientechnologies.orient.core.index.engine.v1;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.index.engine.OMultiValueIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.OCellBTreeMultiValue;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v1.OCellBTreeMultiValueV1;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class OCellBTreeMultiValueIndexEngine implements OMultiValueIndexEngine, OCellBTreeIndexEngine {
  public static final  String DATA_FILE_EXTENSION        = ".cbt";
  private static final String NULL_BUCKET_FILE_EXTENSION = ".nbt";
  public static final  String M_CONTAINER_EXTENSION      = ".mbt";

  private final OCellBTreeMultiValue<Object> sbTree;
  private final String                       name;

  public OCellBTreeMultiValueIndexEngine(String name, OAbstractPaginatedStorage storage, final int version) {
    this.name = name;
    if (version == 1) {
      this.sbTree = new OCellBTreeMultiValueV1<>(name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
    } else if (version == 2) {
      this.sbTree = new CellBTreeMultiValueV2<>(name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, M_CONTAINER_EXTENSION,
          storage);
    } else {
      throw new IllegalArgumentException("Invalid version number " + version);
    }

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
  public void create(OAtomicOperation atomicOperation, OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes,
      boolean nullPointerSupport, OBinarySerializer keySerializer, int keySize, Set<String> clustersToIndex, Map<String, String> engineProperties,
      ODocument metadata, OEncryption encryption) {
    try {
      //noinspection unchecked
      sbTree.create(atomicOperation, keySerializer, keyTypes, keySize, encryption);
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during creation of index " + name), e);
    }
  }

  @Override
  public void delete(OAtomicOperation atomicOperation) {
    try {
      sbTree.delete(atomicOperation);
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during deletion of index " + name), e);
    }
  }

  @Override
  public void deleteWithoutLoad(OAtomicOperation atomicOperation, String indexName) {
    try {
      sbTree.deleteWithoutLoad(atomicOperation);
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during deletion of index " + name), e);
    }
  }

  @Override
  public void load(final String name, final int keySize, final OType[] keyTypes, final OBinarySerializer keySerializer,
      final OEncryption encryption) {
    //noinspection unchecked
    sbTree.load(name, keySize, keyTypes, keySerializer, encryption);
  }

  @Override
  public boolean contains(Object key) {
    return !sbTree.get(key).isEmpty();
  }

  @Override
  public boolean remove(OAtomicOperation atomicOperation, Object key) {
    try {
      return sbTree.remove(atomicOperation, key);
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during removal of key " + key + " from index " + name), e);
    }
  }

  @Override
  public boolean remove(OAtomicOperation atomicOperation, Object key, ORID value) {
    try {
      return sbTree.remove(atomicOperation, key, value);
    } catch (IOException e) {
      throw OException.wrapException(
          new OIndexException("Error during removal of entry with key " + key + "and RID " + value + " from index " + name), e);
    }
  }

  @Override
  public void clear(OAtomicOperation atomicOperation) {
    try {
      sbTree.clear(atomicOperation);
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during clearing of index " + name), e);
    }
  }

  @Override
  public void close() {
    sbTree.close();
  }

  @Override
  public List<ORID> get(Object key) {
    return sbTree.get(key);
  }

  @Override
  public OIndexCursor cursor(ValuesTransformer valuesTransformer) {
    final Object firstKey = sbTree.firstKey();
    if (firstKey == null) {
      return new NullCursor();
    }

    return new OCellBTreeIndexCursor(sbTree.iterateEntriesMajor(firstKey, true, true));
  }

  @Override
  public OIndexCursor descCursor(ValuesTransformer valuesTransformer) {
    final Object lastKey = sbTree.lastKey();
    if (lastKey == null) {
      return new NullCursor();
    }

    return new OCellBTreeIndexCursor(sbTree.iterateEntriesMinor(lastKey, true, false));
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    return new OIndexKeyCursor() {
      private final OCellBTreeMultiValue.OCellBTreeKeyCursor<Object> sbTreeKeyCursor = sbTree.keyCursor();

      @Override
      public Object next(int prefetchSize) {
        return sbTreeKeyCursor.next(prefetchSize);
      }
    };
  }

  @Override
  public void put(OAtomicOperation atomicOperation, Object key, ORID value) {
    try {
      sbTree.put(atomicOperation, key, value);
    } catch (IOException e) {
      throw OException
          .wrapException(new OIndexException("Error during insertion of key " + key + " and RID " + value + " to index " + name),
              e);
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
  public OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer) {
    return new OCellBTreeIndexCursor(sbTree.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder));
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer transformer) {
    return new OCellBTreeIndexCursor(sbTree.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder));
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return new OCellBTreeIndexCursor(sbTree.iterateEntriesMinor(toKey, isInclusive, ascSortOrder));
  }

  @Override
  public long size(final ValuesTransformer transformer) {
    if (transformer == null) {
      final Object firstKey = sbTree.firstKey();
      final Object lastKey = sbTree.lastKey();

      int counter = 0;

      if (!sbTree.get(null).isEmpty()) {
        counter++;
      }

      if (firstKey != null && lastKey != null) {
        final OCellBTreeMultiValue.OCellBTreeCursor<Object, ORID> cursor = sbTree
            .iterateEntriesBetween(firstKey, true, lastKey, true, true);

        Object prevKey = new Object();
        while (true) {
          final Map.Entry<Object, ORID> entry = cursor.next(-1);
          if (entry == null) {
            break;
          }

          if (!prevKey.equals(entry.getKey())) {
            counter++;
          }

          prevKey = entry.getKey();
        }
      }

      return counter;
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

  private static final class OCellBTreeIndexCursor extends OIndexAbstractCursor {
    private final OCellBTreeMultiValue.OCellBTreeCursor<Object, ORID> treeCursor;

    private OCellBTreeIndexCursor(OCellBTreeMultiValue.OCellBTreeCursor<Object, ORID> treeCursor) {
      this.treeCursor = treeCursor;
    }

    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {
      final Object entry = treeCursor.next(getPrefetchSize());
      //noinspection unchecked
      return (Map.Entry<Object, OIdentifiable>) entry;
    }
  }

  private static class NullCursor extends OIndexAbstractCursor {
    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {
      return null;
    }
  }
}
