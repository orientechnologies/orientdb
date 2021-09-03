package com.orientechnologies.orient.core.index.engine.v1;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.engine.OMultiValueIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OCompactedLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.CompositeKeySerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.OCellBTreeMultiValue;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.OCellBTreeSingleValue;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueV3;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class OCellBTreeMultiValueIndexEngine
    implements OMultiValueIndexEngine, OCellBTreeIndexEngine {

  public static final String DATA_FILE_EXTENSION = ".cbt";
  private static final String NULL_BUCKET_FILE_EXTENSION = ".nbt";
  public static final String M_CONTAINER_EXTENSION = ".mbt";

  private final OCellBTreeMultiValue<Object> mvTree;

  private final OCellBTreeSingleValue<OCompositeKey> svTree;
  private final OCellBTreeSingleValue<OIdentifiable> nullTree;

  private final String name;
  private final int id;
  private final String nullTreeName;

  public OCellBTreeMultiValueIndexEngine(
      int id, String name, OAbstractPaginatedStorage storage, final int version) {
    this.id = id;
    this.name = name;
    nullTreeName = name + "$null";

    if (version == 1) {
      throw new IllegalArgumentException("Unsupported version of index : " + version);
    } else if (version == 2) {
      this.mvTree =
          new CellBTreeMultiValueV2<>(
              name,
              DATA_FILE_EXTENSION,
              NULL_BUCKET_FILE_EXTENSION,
              M_CONTAINER_EXTENSION,
              storage);
      this.svTree = null;
      this.nullTree = null;
    } else if (version == 3) {
      throw new IllegalArgumentException("Unsupported version of index : " + version);
    } else if (version == 4) {
      mvTree = null;
      svTree =
          new CellBTreeSingleValueV3<>(
              name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
      nullTree =
          new CellBTreeSingleValueV3<>(
              nullTreeName, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
    } else {
      throw new IllegalStateException("Invalid tree version " + version);
    }
  }

  @Override
  public int getId() {
    return id;
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
  public String getName() {
    return name;
  }

  @Override
  public void create(
      OAtomicOperation atomicOperation,
      @SuppressWarnings("rawtypes") OBinarySerializer valueSerializer,
      boolean isAutomatic,
      OType[] keyTypes,
      boolean nullPointerSupport,
      @SuppressWarnings("rawtypes") OBinarySerializer keySerializer,
      int keySize,
      Map<String, String> engineProperties,
      OEncryption encryption) {
    try {
      if (mvTree != null) {
        //noinspection unchecked
        mvTree.create(keySerializer, keyTypes, keySize, encryption, atomicOperation);
      } else {
        final OType[] sbTypes = calculateTypes(keyTypes);
        assert svTree != null;
        assert nullTree != null;

        svTree.create(
            atomicOperation, new CompositeKeySerializer(), sbTypes, keySize + 1, encryption);
        nullTree.create(
            atomicOperation, OCompactedLinkSerializer.INSTANCE, new OType[] {OType.LINK}, 1, null);
      }
    } catch (IOException e) {
      throw OException.wrapException(
          new OIndexException("Error during creation of index " + name), e);
    }
  }

  @Override
  public void delete(OAtomicOperation atomicOperation) {
    try {
      if (mvTree != null) {
        doClearMVTree(atomicOperation);
        mvTree.delete(atomicOperation);
      } else {
        assert svTree != null;
        assert nullTree != null;

        doClearSVTree(atomicOperation);
        svTree.delete(atomicOperation);
        nullTree.delete(atomicOperation);
      }
    } catch (IOException e) {
      throw OException.wrapException(
          new OIndexException("Error during deletion of index " + name), e);
    }
  }

  private void doClearMVTree(final OAtomicOperation atomicOperation) {
    assert mvTree != null;

    final Object firstKey = mvTree.firstKey();
    final Object lastKey = mvTree.lastKey();

    try (Stream<ORawPair<Object, ORID>> stream =
        mvTree.iterateEntriesBetween(firstKey, true, lastKey, true, true)) {
      stream.forEach(
          (pair) -> {
            try {
              mvTree.remove(atomicOperation, pair.first, pair.second);
            } catch (IOException e) {
              throw OException.wrapException(
                  new OIndexException("Error during cleaning of index " + name), e);
            }
          });
    }

    try (final Stream<ORID> rids = mvTree.get(null)) {
      rids.forEach(
          (rid) -> {
            try {
              mvTree.remove(atomicOperation, null, rid);
            } catch (final IOException e) {
              throw OException.wrapException(
                  new OStorageException("Error during cleaning of index " + name), e);
            }
          });
    }
  }

  private void doClearSVTree(final OAtomicOperation atomicOperation) {
    assert svTree != null;
    assert nullTree != null;

    {
      final OCompositeKey firstKey = svTree.firstKey();
      final OCompositeKey lastKey = svTree.lastKey();

      try (Stream<ORawPair<OCompositeKey, ORID>> stream =
          svTree.iterateEntriesBetween(firstKey, true, lastKey, true, true)) {
        stream.forEach(
            (pair) -> {
              try {
                svTree.remove(atomicOperation, pair.first);
              } catch (IOException e) {
                throw OException.wrapException(
                    new OIndexException("Error during index cleaning"), e);
              }
            });
      }
    }

    {
      final OIdentifiable firstKey = nullTree.firstKey();
      final OIdentifiable lastKey = nullTree.lastKey();

      if (firstKey != null && lastKey != null) {
        try (Stream<ORawPair<OIdentifiable, ORID>> stream =
            nullTree.iterateEntriesBetween(firstKey, true, lastKey, true, true)) {
          stream.forEach(
              (pair) -> {
                try {
                  nullTree.remove(atomicOperation, pair.first);
                } catch (IOException e) {
                  throw OException.wrapException(
                      new OIndexException("Error during index cleaning"), e);
                }
              });
        }
      }
    }
  }

  @Override
  public void load(
      final String name,
      final int keySize,
      final OType[] keyTypes,
      @SuppressWarnings("rawtypes") final OBinarySerializer keySerializer,
      final OEncryption encryption) {
    if (mvTree != null) {
      //noinspection unchecked
      mvTree.load(name, keySize, keyTypes, keySerializer, encryption);
    } else {
      assert svTree != null;
      assert nullTree != null;

      final OType[] sbTypes = calculateTypes(keyTypes);

      svTree.load(name, keySize + 1, sbTypes, new CompositeKeySerializer(), null);
      nullTree.load(
          nullTreeName, 1, new OType[] {OType.LINK}, OCompactedLinkSerializer.INSTANCE, null);
    }
  }

  @Override
  public boolean remove(final OAtomicOperation atomicOperation, Object key, ORID value) {
    try {
      if (mvTree != null) {
        return mvTree.remove(atomicOperation, key, value);
      } else {
        if (key != null) {
          assert svTree != null;

          final OCompositeKey compositeKey = createCompositeKey(key, value);

          final boolean[] removed = new boolean[1];
          try (Stream<ORawPair<OCompositeKey, ORID>> stream =
              svTree.iterateEntriesBetween(compositeKey, true, compositeKey, true, true)) {
            stream.forEach(
                (pair) -> {
                  try {
                    final boolean result = svTree.remove(atomicOperation, pair.first) != null;
                    removed[0] = result || removed[0];
                  } catch (final IOException e) {
                    throw OException.wrapException(
                        new OIndexException(
                            "Error during remove of entry (" + key + ", " + value + ")"),
                        e);
                  }
                });
          }

          return removed[0];
        } else {
          assert nullTree != null;
          return nullTree.remove(atomicOperation, value) != null;
        }
      }
    } catch (IOException e) {
      throw OException.wrapException(
          new OIndexException(
              "Error during removal of entry with key "
                  + key
                  + "and RID "
                  + value
                  + " from index "
                  + name),
          e);
    }
  }

  @Override
  public void clear(OAtomicOperation atomicOperation) {
    if (mvTree != null) {
      doClearMVTree(atomicOperation);
    } else {
      doClearSVTree(atomicOperation);
    }
  }

  @Override
  public void close() {
    if (mvTree != null) {
      mvTree.close();
    } else {
      assert svTree != null;
      assert nullTree != null;

      svTree.close();
      nullTree.close();
    }
  }

  @Override
  public Stream<ORID> get(Object key) {
    if (mvTree != null) {
      return mvTree.get(key);
    } else if (key != null) {
      assert svTree != null;

      final OCompositeKey firstKey = convertToCompositeKey(key);
      final OCompositeKey lastKey = convertToCompositeKey(key);

      //noinspection resource
      return svTree
          .iterateEntriesBetween(firstKey, true, lastKey, true, true)
          .map((pair) -> pair.second);
    } else {
      assert nullTree != null;

      //noinspection resource
      return nullTree
          .iterateEntriesBetween(
              new ORecordId(0, 0), true, new ORecordId(Short.MAX_VALUE, Long.MAX_VALUE), true, true)
          .map((pair) -> pair.second);
    }
  }

  @Override
  public Stream<ORawPair<Object, ORID>> stream(ValuesTransformer valuesTransformer) {
    if (mvTree != null) {
      final Object firstKey = mvTree.firstKey();
      if (firstKey == null) {
        return emptyStream();
      }

      return mvTree.iterateEntriesMajor(firstKey, true, true);
    } else {
      assert svTree != null;

      final OCompositeKey firstKey = svTree.firstKey();
      if (firstKey == null) {
        return emptyStream();
      }

      return mapSVStream(svTree.iterateEntriesMajor(firstKey, true, true));
    }
  }

  private static Stream<ORawPair<Object, ORID>> mapSVStream(
      Stream<ORawPair<OCompositeKey, ORID>> stream) {
    return stream.map((entry) -> new ORawPair<>(extractKey(entry.first), entry.second));
  }

  private static Stream<ORawPair<Object, ORID>> emptyStream() {
    return StreamSupport.stream(Spliterators.emptySpliterator(), false);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> descStream(ValuesTransformer valuesTransformer) {
    if (mvTree != null) {
      final Object lastKey = mvTree.lastKey();
      if (lastKey == null) {
        return emptyStream();
      }
      return mvTree.iterateEntriesMinor(lastKey, true, false);
    } else {
      assert svTree != null;

      final OCompositeKey lastKey = svTree.lastKey();
      if (lastKey == null) {
        return emptyStream();
      }
      return mapSVStream(svTree.iterateEntriesMinor(lastKey, true, false));
    }
  }

  @Override
  public Stream<Object> keyStream() {
    if (mvTree != null) {
      return mvTree.keyStream();
    }

    assert svTree != null;
    //noinspection resource
    return svTree.keyStream().map(OCellBTreeMultiValueIndexEngine::extractKey);
  }

  @Override
  public void put(OAtomicOperation atomicOperation, Object key, ORID value) {
    if (mvTree != null) {
      try {
        mvTree.put(atomicOperation, key, value);
      } catch (IOException e) {
        throw OException.wrapException(
            new OIndexException(
                "Error during insertion of key " + key + " and RID " + value + " to index " + name),
            e);
      }
    } else if (key != null) {
      assert svTree != null;
      try {
        svTree.put(atomicOperation, createCompositeKey(key, value), value);
      } catch (IOException e) {
        throw OException.wrapException(
            new OIndexException(
                "Error during insertion of key " + key + " and RID " + value + " to index " + name),
            e);
      }
    } else {
      assert nullTree != null;
      try {
        nullTree.put(atomicOperation, value, value);
      } catch (IOException e) {
        throw OException.wrapException(
            new OIndexException(
                "Error during insertion of null key and RID " + value + " to index " + name),
            e);
      }
    }
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesBetween(
      Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      ValuesTransformer transformer) {
    if (mvTree != null) {
      return mvTree.iterateEntriesBetween(
          rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder);
    }
    assert svTree != null;

    // "from", "to" are null, then scan whole tree as for infinite range
    if (rangeFrom == null && rangeTo == null) {
      return mapSVStream(svTree.allEntries());
    }

    // "from" could be null, then "to" is not (minor)
    final OCompositeKey toKey = convertToCompositeKey(rangeTo);
    if (rangeFrom == null) {
      return mapSVStream(svTree.iterateEntriesMinor(toKey, toInclusive, ascSortOrder));
    }
    final OCompositeKey fromKey = convertToCompositeKey(rangeFrom);
    // "to" could be null, then "from" is not (major)
    if (rangeTo == null) {
      return mapSVStream(svTree.iterateEntriesMajor(fromKey, fromInclusive, ascSortOrder));
    }
    return mapSVStream(
        svTree.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascSortOrder));
  }

  private static OCompositeKey convertToCompositeKey(Object rangeFrom) {
    OCompositeKey firstKey;
    if (rangeFrom instanceof OCompositeKey) {
      firstKey = (OCompositeKey) rangeFrom;
    } else {
      firstKey = new OCompositeKey(rangeFrom);
    }
    return firstKey;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMajor(
      Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    if (mvTree != null) {
      return mvTree.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder);
    }
    assert svTree != null;

    final OCompositeKey firstKey = convertToCompositeKey(fromKey);
    return mapSVStream(svTree.iterateEntriesMajor(firstKey, isInclusive, ascSortOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMinor(
      Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    if (mvTree != null) {
      return mvTree.iterateEntriesMinor(toKey, isInclusive, ascSortOrder);
    }
    assert svTree != null;

    final OCompositeKey lastKey = convertToCompositeKey(toKey);
    return mapSVStream(svTree.iterateEntriesMinor(lastKey, isInclusive, ascSortOrder));
  }

  @Override
  public long size(final ValuesTransformer transformer) {
    if (mvTree != null) {
      return mvTreeSize(transformer);
    }

    assert svTree != null;
    assert nullTree != null;

    return svTreeEntries();
  }

  private long mvTreeSize(final ValuesTransformer transformer) {
    assert mvTree != null;

    // calculate amount of keys
    if (transformer == null) {
      final Object firstKey = mvTree.firstKey();
      final Object lastKey = mvTree.lastKey();

      int counter = 0;

      try (Stream<ORID> oridStream = mvTree.get(null)) {
        if (oridStream.iterator().hasNext()) {
          counter++;
        }
      }

      if (firstKey != null && lastKey != null) {
        final Object[] prevKey = new Object[] {new Object()};
        try (final Stream<ORawPair<Object, ORID>> stream =
            mvTree.iterateEntriesBetween(firstKey, true, lastKey, true, true)) {
          counter +=
              stream
                  .filter(
                      (pair) -> {
                        final boolean result = !prevKey[0].equals(pair.first);
                        prevKey[0] = pair.first;
                        return result;
                      })
                  .count();
        }
      }
      return counter;
    }
    // calculate amount of entries
    return mvTree.size();
  }

  private long svTreeEntries() {
    assert svTree != null;
    assert nullTree != null;
    return svTree.size() + nullTree.size();
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return true;
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    if (mvTree != null) {
      mvTree.acquireAtomicExclusiveLock();
    } else {
      assert svTree != null;
      assert nullTree != null;

      svTree.acquireAtomicExclusiveLock();
      nullTree.acquireAtomicExclusiveLock();
    }

    return true;
  }

  @Override
  public String getIndexNameByKey(Object key) {
    return name;
  }

  @Override
  public void updateUniqueIndexVersion(final Object key) {
    // not implemented
  }

  @Override
  public int getUniqueIndexVersion(final Object key) {
    return 0; // not implemented
  }

  private static OType[] calculateTypes(final OType[] keyTypes) {
    final OType[] sbTypes;
    if (keyTypes != null) {
      sbTypes = new OType[keyTypes.length + 1];
      System.arraycopy(keyTypes, 0, sbTypes, 0, keyTypes.length);
      sbTypes[sbTypes.length - 1] = OType.LINK;
    } else {
      throw new OIndexException("Types of fields should be provided upon of creation of index");
    }
    return sbTypes;
  }

  private static OCompositeKey createCompositeKey(final Object key, final ORID value) {
    final OCompositeKey compositeKey = new OCompositeKey(key);
    compositeKey.addKey(value);
    return compositeKey;
  }

  private static Object extractKey(final OCompositeKey compositeKey) {
    if (compositeKey == null) {
      return null;
    }
    final List<Object> keys = compositeKey.getKeys();

    final Object key;
    if (keys.size() == 2) {
      key = keys.get(0);
    } else {
      key = new OCompositeKey(keys.subList(0, keys.size() - 1));
    }
    return key;
  }
}
