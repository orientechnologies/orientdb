package com.orientechnologies.orient.core.index.engine.v1;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OUTF8Serializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.index.engine.OMultiValueIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OCompactedLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.StatefulCompositeKeySerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.OCellBTreeMultiValue;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.OCellBTreeSingleValue;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueV3;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class OCellBTreeMultiValueIndexEngine implements OMultiValueIndexEngine, OCellBTreeIndexEngine {

  public static final  String DATA_FILE_EXTENSION        = ".cbt";
  private static final String NULL_BUCKET_FILE_EXTENSION = ".nbt";
  public static final  String M_CONTAINER_EXTENSION      = ".mbt";

  private final OCellBTreeMultiValue<Object> mvTree;

  private final OCellBTreeSingleValue<OCompositeKey> svTree;
  private final OCellBTreeSingleValue<OIdentifiable> nullTree;

  private final String name;
  private final int    id;
  private final String nullTreeName;

  public OCellBTreeMultiValueIndexEngine(int id, String name, OAbstractPaginatedStorage storage, final int version) {
    this.id = id;
    this.name = name;
    nullTreeName = name + "$null";

    if (version == 1) {
      throw new IllegalArgumentException("Unsupported version of index : " + version);
    } else if (version == 2) {
      this.mvTree = new CellBTreeMultiValueV2<>(name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, M_CONTAINER_EXTENSION,
          storage);
      this.svTree = null;
      this.nullTree = null;
    } else if (version == 3) {
      throw new IllegalArgumentException("Unsupported version of index : " + version);
    } else if (version == 4) {
      mvTree = null;
      svTree = new CellBTreeSingleValueV3<>(name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
      nullTree = new CellBTreeSingleValueV3<>(nullTreeName, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
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
      if (mvTree != null) {
        //noinspection unchecked
        mvTree.create(keySerializer, keyTypes, keySize, encryption);
      } else {
        final OType[] sbTypes = calculateTypes(keyTypes);
        assert svTree != null;
        assert nullTree != null;

        svTree.create(defineKeySerializer(), sbTypes, keySize + 1, encryption);
        nullTree.create(OCompactedLinkSerializer.INSTANCE, new OType[] { OType.LINK }, 1, null);
      }
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during creation of index " + name), e);
    }
  }

  @Override
  public void delete() {
    try {
      if (mvTree != null) {
        doClearMBTree();
        mvTree.delete();
      } else {
        assert svTree != null;
        assert nullTree != null;

        doClearSVTree();
        svTree.delete();
        nullTree.delete();
      }
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during deletion of index " + name), e);
    }
  }

  private void doClearMBTree() throws IOException {
    assert mvTree != null;

    final Object firstKey = mvTree.firstKey();
    final Object lastKey = mvTree.lastKey();

    final Spliterator<ORawPair<Object, ORID>> spliterator = mvTree.iterateEntriesBetween(firstKey, true, lastKey, true, true);

    StreamSupport.stream(spliterator, false).forEach((pair) -> {
      try {
        mvTree.remove(pair.first, pair.second);
      } catch (IOException e) {
        throw OException.wrapException(new OIndexException("Error during index cleaning"), e);
      }
    });

    final List<ORID> rids = mvTree.get(null);
    for (final ORID rid : rids) {
      mvTree.remove(null, rid);
    }
  }

  private void doClearSVTree() {
    assert svTree != null;
    assert nullTree != null;

    {
      final OCompositeKey firstKey = svTree.firstKey();
      final OCompositeKey lastKey = svTree.lastKey();

      final Spliterator<ORawPair<OCompositeKey, ORID>> spliterator = svTree
          .iterateEntriesBetween(firstKey, true, lastKey, true, true);

      StreamSupport.stream(spliterator, false).forEach((pair) -> {
        try {
          svTree.remove(pair.first);
        } catch (IOException e) {
          throw OException.wrapException(new OIndexException("Error during index cleaning"), e);
        }
      });
    }

    {
      final OIdentifiable firstKey = nullTree.firstKey();
      final OIdentifiable lastKey = nullTree.lastKey();

      if (firstKey != null && lastKey != null) {
        final Spliterator<ORawPair<OIdentifiable, ORID>> spliterator = nullTree
            .iterateEntriesBetween(firstKey, true, lastKey, true, true);

        StreamSupport.stream(spliterator, false).forEach((pair) -> {
          try {
            nullTree.remove(pair.first);
          } catch (IOException e) {
            throw OException.wrapException(new OIndexException("Error during index cleaning"), e);
          }
        });
      }
    }
  }

  @Override
  public void load(final String name, final int keySize, final OType[] keyTypes, final OBinarySerializer keySerializer,
      final OEncryption encryption) {
    if (mvTree != null) {
      //noinspection unchecked
      mvTree.load(name, keySize, keyTypes, keySerializer, encryption);
    } else {
      assert svTree != null;
      assert nullTree != null;

      final OType[] sbTypes = calculateTypes(keyTypes);

      svTree.load(name, keySize + 1, sbTypes, defineKeySerializer(), null);
      nullTree.load(nullTreeName, 1, new OType[] { OType.LINK }, OCompactedLinkSerializer.INSTANCE, null);
    }
  }

  private static StatefulCompositeKeySerializer defineKeySerializer() {
    final StatefulCompositeKeySerializer serializer = new StatefulCompositeKeySerializer();
    serializer.defineSerializer(OType.LINK, OCompactedLinkSerializer.INSTANCE);
    serializer.defineSerializer(OType.STRING, OUTF8Serializer.INSTANCE);

    return serializer;
  }

  @Override
  public boolean contains(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object key, ORID value) {
    try {
      if (mvTree != null) {
        return mvTree.remove(key, value);
      } else {
        if (key != null) {
          assert svTree != null;

          final OCompositeKey compositeKey = createCompositeKey(key, value);
          final Spliterator<ORawPair<OCompositeKey, ORID>> spliterator = svTree
              .iterateEntriesBetween(compositeKey, true, compositeKey, true, true);

          final boolean[] removed = new boolean[1];
          StreamSupport.stream(spliterator, false).forEach((pair) -> {
            try {
              final boolean result = svTree.remove(pair.first) != null;
              removed[0] = result || removed[0];
            } catch (final IOException e) {
              throw OException.wrapException(new OIndexException("Error during remove of entry (" + key + ", " + value + ")"), e);
            }
          });

          return removed[0];
        } else {
          assert nullTree != null;
          return nullTree.remove(value) != null;
        }
      }
    } catch (IOException e) {
      throw OException.wrapException(
          new OIndexException("Error during removal of entry with key " + key + "and RID " + value + " from index " + name), e);
    }
  }

  @Override
  public void clear() {
    try {
      if (mvTree != null) {
        doClearMBTree();
      } else {
        doClearSVTree();
      }
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during clearing of index " + name), e);
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
  public List<ORID> get(Object key) {
    if (mvTree != null) {
      return mvTree.get(key);
    } else if (key != null) {
      assert svTree != null;

      final OCompositeKey firstKey = convertToCompositeKey(key);
      final OCompositeKey lastKey = convertToCompositeKey(key);

      final Spliterator<ORawPair<OCompositeKey, ORID>> spliterator = svTree
          .iterateEntriesBetween(firstKey, true, lastKey, true, true);
      return StreamSupport.stream(spliterator, false).map((pair) -> pair.second).collect(Collectors.toList());
    } else {
      assert nullTree != null;
      final Spliterator<ORawPair<OIdentifiable, ORID>> cursor = nullTree
          .iterateEntriesBetween(new ORecordId(0, 0), true, new ORecordId(Short.MAX_VALUE, Long.MAX_VALUE), true, true);

      return StreamSupport.stream(cursor, false).map((pair) -> pair.second).collect(Collectors.toList());
    }
  }

  @Override
  public IndexCursor cursor(ValuesTransformer valuesTransformer) {
    if (mvTree != null) {
      final Object firstKey = mvTree.firstKey();
      if (firstKey == null) {
        return new NullCursor();
      }

      return new MVTreeIndexCursor(mvTree.iterateEntriesMajor(firstKey, true, true));
    } else {
      assert svTree != null;

      final OCompositeKey firstKey = svTree.firstKey();
      if (firstKey == null) {
        return new NullCursor();
      }

      return new SVTreeIndexCursor(svTree.iterateEntriesMajor(firstKey, true, true));
    }
  }

  @Override
  public IndexCursor descCursor(ValuesTransformer valuesTransformer) {
    if (mvTree != null) {
      final Object lastKey = mvTree.lastKey();
      if (lastKey == null) {
        return new NullCursor();
      }

      return new MVTreeIndexCursor(mvTree.iterateEntriesMinor(lastKey, true, false));
    } else {
      assert svTree != null;

      final OCompositeKey lastKey = svTree.lastKey();
      if (lastKey == null) {
        return new NullCursor();
      }

      return new SVTreeIndexCursor(svTree.iterateEntriesMinor(lastKey, true, false));
    }
  }

  @Override
  public IndexKeySpliterator keyCursor() {
    if (mvTree != null) {
      return new IndexKeySpliterator() {
        private final Spliterator<Object> keySpliterator = mvTree.keySpliterator();

        @Override
        public boolean tryAdvance(Consumer<? super Object> action) {
          return keySpliterator.tryAdvance(action);
        }

        @Override
        public Spliterator<Object> trySplit() {
          return null;
        }

        @Override
        public long estimateSize() {
          return keySpliterator.estimateSize();
        }

        @Override
        public int characteristics() {
          return NONNULL | ORDERED;
        }
      };
    }

    assert svTree != null;
    return new IndexKeySpliterator() {
      private final Spliterator<OCompositeKey> keySpliterator = svTree.keySpliterator();

      @Override
      public boolean tryAdvance(Consumer<? super Object> action) {
        return keySpliterator.tryAdvance(OCellBTreeMultiValueIndexEngine::extractKey);
      }

      @Override
      public Spliterator<Object> trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return keySpliterator.estimateSize();
      }

      @Override
      public int characteristics() {
        return NONNULL | ORDERED;
      }
    };
  }

  @Override
  public void put(Object key, ORID value) {
    if (mvTree != null) {
      try {
        mvTree.put(key, value);
      } catch (IOException e) {
        throw OException
            .wrapException(new OIndexException("Error during insertion of key " + key + " and RID " + value + " to index " + name),
                e);
      }
    } else if (key != null) {
      assert svTree != null;
      try {
        svTree.put(createCompositeKey(key, value), value);
      } catch (IOException e) {
        throw OException
            .wrapException(new OIndexException("Error during insertion of key " + key + " and RID " + value + " to index " + name),
                e);
      }
    } else {
      assert nullTree != null;
      try {
        nullTree.put(value, value);
      } catch (IOException e) {
        throw OException
            .wrapException(new OIndexException("Error during insertion of null key and RID " + value + " to index " + name), e);
      }
    }
  }

  @Override
  public Object getFirstKey() {
    if (mvTree != null) {
      return mvTree.firstKey();
    } else {
      assert svTree != null;
      return extractKey(svTree.firstKey());
    }
  }

  @Override
  public Object getLastKey() {
    if (mvTree != null) {
      return mvTree.lastKey();
    } else {
      assert svTree != null;
      return extractKey(svTree.lastKey());
    }
  }

  @Override
  public IndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer) {
    if (mvTree != null) {
      return new MVTreeIndexCursor(mvTree.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder));
    }

    assert svTree != null;
    final OCompositeKey firstKey = convertToCompositeKey(rangeFrom);
    final OCompositeKey lastKey = convertToCompositeKey(rangeTo);

    return new SVTreeIndexCursor(svTree.iterateEntriesBetween(firstKey, fromInclusive, lastKey, toInclusive, ascSortOrder));
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
  public IndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    if (mvTree != null) {
      return new MVTreeIndexCursor(mvTree.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder));
    }
    assert svTree != null;

    final OCompositeKey firstKey = convertToCompositeKey(fromKey);
    return new SVTreeIndexCursor(svTree.iterateEntriesMajor(firstKey, isInclusive, ascSortOrder));
  }

  @Override
  public IndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    if (mvTree != null) {
      return new MVTreeIndexCursor(mvTree.iterateEntriesMinor(toKey, isInclusive, ascSortOrder));
    }

    assert svTree != null;

    final OCompositeKey lastKey = convertToCompositeKey(toKey);
    return new SVTreeIndexCursor(svTree.iterateEntriesMinor(lastKey, isInclusive, ascSortOrder));
  }

  @Override
  public long size(final ValuesTransformer transformer) {
    if (mvTree != null) {
      return mvTreeSize(transformer);
    }

    assert svTree != null;
    assert nullTree != null;

    if (transformer == null) {
      return svTreeKeys();
    }

    return svTreeEntries();
  }

  private long mvTreeSize(ValuesTransformer transformer) {
    assert mvTree != null;

    //calculate amount of keys
    if (transformer == null) {
      final Object firstKey = mvTree.firstKey();
      final Object lastKey = mvTree.lastKey();

      int counter = 0;

      if (!mvTree.get(null).isEmpty()) {
        counter++;
      }

      if (firstKey != null && lastKey != null) {
        final Spliterator<ORawPair<Object, ORID>> spliterator = mvTree.iterateEntriesBetween(firstKey, true, lastKey, true, true);

        final Object[] prevKey = new Object[] { new Object() };
        counter += StreamSupport.stream(spliterator, false).filter((pair) -> {
          final boolean result = !prevKey[0].equals(pair.first);
          prevKey[0] = pair.first;
          return result;
        }).count();
      }

      return counter;
    }

    //calculate amount of entries
    return mvTree.size();
  }

  private long svTreeEntries() {
    assert svTree != null;
    assert nullTree != null;

    return svTree.size() + nullTree.size();
  }

  private long svTreeKeys() {
    assert svTree != null;
    assert nullTree != null;

    int count = 0;
    if (svTree.size() > 0) {
      count++;
    }

    final OCompositeKey firstKey = svTree.firstKey();
    final OCompositeKey lastKey = svTree.lastKey();

    final Spliterator<ORawPair<OCompositeKey, ORID>> spliterator = svTree
        .iterateEntriesBetween(firstKey, true, lastKey, true, true);

    final Object[] prevKey = new Object[] { new Object() };
    count += StreamSupport.stream(spliterator, false).filter((pair) -> {
      final Object key = extractKey(pair.first);
      final boolean result = !prevKey[0].equals(key);
      prevKey[0] = key;
      return result;
    }).count();

    return count;
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return true;
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    if (mvTree != null) {
      mvTree.acquireAtomicExclusiveLock();
    }

    assert svTree != null;
    assert nullTree != null;

    svTree.acquireAtomicExclusiveLock();
    nullTree.acquireAtomicExclusiveLock();

    return true;
  }

  @Override
  public String getIndexNameByKey(Object key) {
    return name;
  }

  private static final class MVTreeIndexCursor implements IndexCursor {
    private final Spliterator<ORawPair<Object, ORID>> treeSpliterator;

    private MVTreeIndexCursor(Spliterator<ORawPair<Object, ORID>> treeSpliterator) {
      this.treeSpliterator = treeSpliterator;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
      return treeSpliterator.tryAdvance(action);
    }

    @Override
    public Spliterator<ORawPair<Object, ORID>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return treeSpliterator.estimateSize();
    }

    @Override
    public int characteristics() {
      return NONNULL | ORDERED;
    }
  }

  private static OType[] calculateTypes(OType[] keyTypes) {
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

  private static OCompositeKey createCompositeKey(Object key, ORID value) {
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

  private static final class SVTreeIndexCursor implements IndexCursor {
    private final Spliterator<ORawPair<OCompositeKey, ORID>> treeSpliterator;

    private SVTreeIndexCursor(Spliterator<ORawPair<OCompositeKey, ORID>> treeSpliterator) {
      this.treeSpliterator = treeSpliterator;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
      return treeSpliterator.tryAdvance((pair) -> action.accept(new ORawPair<>(extractKey(pair.first), pair.second)));
    }

    @Override
    public Spliterator<ORawPair<Object, ORID>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return treeSpliterator.estimateSize();
    }

    @Override
    public int characteristics() {
      return NONNULL | ORDERED;
    }
  }

  private static final class NullCursor implements IndexCursor {
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
