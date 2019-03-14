package com.orientechnologies.orient.core.index.engine.v1;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexAbstractCursor;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.index.engine.OMultiValueIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTree;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.OCellBTreeMultiValue;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v1.OCellBTreeMultiValueV1;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.OCellBTreeMultiValueV2;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.OCellBTee;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1.OCellBTreeSingleValueV1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class OCellBTreeMultiValueIndexEngine implements OMultiValueIndexEngine, OCellBTreeIndexEngine {
  public static final  String DATA_FILE_EXTENSION        = ".cbt";
  private static final String NULL_BUCKET_FILE_EXTENSION = ".nbt";
  public static final  String M_CONTAINER_EXTENSION      = ".mbt";

  private final OCellBTreeMultiValue<Object> multiValueTree;

  private final OCellBTreeSingleValueV1<OCompositeKey> singleValueTree;
  private final OSBTree<OIdentifiable, Boolean>        nullValueTree;

  private final String name;

  public OCellBTreeMultiValueIndexEngine(String name, OAbstractPaginatedStorage storage, final int version) {
    this.name = name;

    if (version == 1) {
      this.multiValueTree = new OCellBTreeMultiValueV1<>(name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);

      this.singleValueTree = null;
      this.nullValueTree = null;
    } else if (version == 2) {
      this.multiValueTree = new OCellBTreeMultiValueV2<>(name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION,
          M_CONTAINER_EXTENSION, storage);
      this.singleValueTree = null;
      this.nullValueTree = null;
    } else if (version == 3) {
      this.multiValueTree = null;
      this.singleValueTree = new OCellBTreeSingleValueV1<>(name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
      this.nullValueTree = new OSBTree<>(name + "$n", DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
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
  public void create(OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes, boolean nullPointerSupport,
      OBinarySerializer keySerializer, int keySize, Set<String> clustersToIndex, Map<String, String> engineProperties,
      ODocument metadata, OEncryption encryption) {
    try {
      if (multiValueTree != null) {
        //noinspection unchecked
        multiValueTree.create(keySerializer, keyTypes, keySize, encryption);
      } else {
        final OType[] types = createMEnhancedKeyTypes(keyTypes, keySize);

        assert singleValueTree != null;
        assert nullValueTree != null;
        singleValueTree.create(OCompositeKeySerializer.INSTANCE, types, keySize + 1, encryption);
        nullValueTree.create(OLinkSerializer.INSTANCE, OBooleanSerializer.INSTANCE, null, 1, false, null);
      }
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during creation of index " + name), e);
    }
  }

  private static OType[] createMEnhancedKeyTypes(final OType[] keyTypes, final int keySize) {
    final OType[] types = new OType[keySize + 1];
    System.arraycopy(keyTypes, 0, types, 0, keyTypes.length);
    types[types.length - 1] = OType.LINK;
    return types;
  }

  @Override
  public void delete() {
    try {
      if (multiValueTree != null) {
        multiValueTree.delete();
      }

      if (singleValueTree != null) {
        singleValueTree.delete();
      }

      if (nullValueTree != null) {
        nullValueTree.delete();
      }
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during deletion of index " + name), e);
    }
  }

  @Override
  public void deleteWithoutLoad(String indexName) {
    try {
      if (multiValueTree != null) {
        multiValueTree.deleteWithoutLoad();
      }

      if (singleValueTree != null) {
        singleValueTree.deleteWithoutLoad();
      }

      if (nullValueTree != null) {
        nullValueTree.delete();
      }
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during deletion of index " + name), e);
    }
  }

  @Override
  public void load(final String name, final int keySize, final OType[] keyTypes, final OBinarySerializer keySerializer,
      final OEncryption encryption) {
    if (multiValueTree != null) {
      //noinspection unchecked
      multiValueTree.load(name, keySize, keyTypes, keySerializer, encryption);
    } else {
      assert singleValueTree != null;
      assert nullValueTree != null;

      final OType[] types = createMEnhancedKeyTypes(keyTypes, keySize);
      singleValueTree.load(name, keySize + 1, types, OCompositeKeySerializer.INSTANCE, encryption);
      nullValueTree.load(name + "$n", OLinkSerializer.INSTANCE, OBooleanSerializer.INSTANCE, null, 1, false, null);
    }
  }

  @Override
  public boolean contains(Object key) {
    if (multiValueTree != null) {
      return !multiValueTree.get(key).isEmpty();
    }

    assert singleValueTree != null;

    if (key != null) {
      final OCellBTee.OCellBTreeCursor<OCompositeKey, ORID> cursor = singleValueTree
          .iterateEntriesMajor(new OCompositeKey(key), false, true);

      return cursor.next(1) != null;
    } else {
      assert nullValueTree != null;
      return nullValueTree.size() == 0;
    }
  }

  @Override
  public boolean remove(Object key) {
    try {
      if (multiValueTree != null) {
        return multiValueTree.remove(key);
      }

      if (key != null) {
        assert singleValueTree != null;

        final OCellBTee.OCellBTreeCursor<OCompositeKey, ORID> cursor = singleValueTree
            .iterateEntriesMajor(new OCompositeKey(key), false, true);

        final List<OCompositeKey> keysToDelete = new ArrayList<>(16);
        Map.Entry<OCompositeKey, ORID> entry = cursor.next(-1);

        while (entry != null) {
          keysToDelete.add(entry.getKey());
          entry = cursor.next(-1);
        }

        for (OCompositeKey keyToDelete : keysToDelete) {
          singleValueTree.remove(keyToDelete);
        }

        return !keysToDelete.isEmpty();
      } else {
        assert nullValueTree != null;
        final OSBTree.OSBTreeKeyCursor<OIdentifiable> keyCursor = nullValueTree.keyCursor();
        OIdentifiable keyToDelete = keyCursor.next(-1);

        boolean removed = false;
        while (keyToDelete != null) {
          removed = true;
          nullValueTree.remove(keyToDelete);
          keyToDelete = keyCursor.next(-1);
        }

        return removed;
      }
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during removal of key " + key + " from index " + name), e);
    }
  }

  @Override
  public boolean remove(Object key, ORID value) {
    try {
      if (multiValueTree != null) {
        return multiValueTree.remove(key, value);
      }

      if (key != null) {
        assert singleValueTree != null;
        return singleValueTree.remove(new OCompositeKey(key, value)) != null;
      } else {
        assert nullValueTree != null;
        return nullValueTree.remove(value) != null;
      }
    } catch (IOException e) {
      throw OException.wrapException(
          new OIndexException("Error during removal of entry with key " + key + "and RID " + value + " from index " + name), e);
    }
  }

  @Override
  public void clear() {
    try {
      if (multiValueTree != null) {
        final OCellBTreeMultiValue.OCellBTreeKeyCursor<Object> cursor = multiValueTree.keyCursor();
        Object key = cursor.next(-1);
        while (key != null) {
          multiValueTree.remove(key);
          key = cursor.next(-1);
        }
      }

      if (singleValueTree != null) {
        final OCellBTee.OCellBTreeKeyCursor<OCompositeKey> cursor = singleValueTree.keyCursor();
        OCompositeKey key = cursor.next(-1);
        while (key != null) {
          singleValueTree.remove(key);
          key = cursor.next(-1);
        }
      }

      if (nullValueTree != null) {
        final OSBTree.OSBTreeKeyCursor<OIdentifiable> cursor = nullValueTree.keyCursor();
        OIdentifiable key = cursor.next(-1);
        while (key != null) {
          nullValueTree.remove(key);
          key = cursor.next(-1);
        }
      }
    } catch (IOException e) {
      throw OException.wrapException(new OIndexException("Error during clearing of index " + name), e);
    }
  }

  @Override
  public void close() {
    if (multiValueTree != null) {
      multiValueTree.close();
    }

    if (singleValueTree != null) {
      singleValueTree.close();
    }

    if (nullValueTree != null) {
      nullValueTree.close();
    }
  }

  @Override
  public List<ORID> get(Object key) {
    if (multiValueTree != null) {
      return multiValueTree.get(key);
    }

    if (key != null) {
      final OCompositeKey compositeKey = new OCompositeKey(key);

      assert singleValueTree != null;
      final OCellBTee.OCellBTreeCursor<OCompositeKey, ORID> cursor = singleValueTree
          .iterateEntriesBetween(compositeKey, true, compositeKey, true, true);

      final List<ORID> result = new ArrayList<>();

      Map.Entry<OCompositeKey, ORID> entry = cursor.next(-1);
      while (entry != null) {
        result.add(entry.getValue());
        entry = cursor.next(-1);
      }

      return result;
    }

    assert nullValueTree != null;

    final List<ORID> result = new ArrayList<>();
    final OSBTree.OSBTreeKeyCursor<OIdentifiable> cursor = nullValueTree.keyCursor();
    OIdentifiable item = cursor.next(-1);
    while (item != null) {
      result.add(item.getIdentity());
      item = cursor.next(-1);
    }

    return result;
  }

  @Override
  public OIndexCursor cursor(ValuesTransformer valuesTransformer) {
    if (multiValueTree != null) {
      final Object firstKey = multiValueTree.firstKey();
      if (firstKey == null) {
        return new NullCursor();
      }

      return new OCellBTreeIndexCursor(multiValueTree.iterateEntriesMajor(firstKey, true, true));
    }

    assert singleValueTree != null;

    final Object firstKey = singleValueTree.firstKey();
    if (firstKey == null) {
      return new NullCursor();
    }

    return new OMCellBTreeIndexCursor(singleValueTree.iterateEntriesMajor(new OCompositeKey(firstKey), true, true));
  }

  @Override
  public OIndexCursor descCursor(ValuesTransformer valuesTransformer) {
    if (multiValueTree != null) {
      final Object lastKey = multiValueTree.lastKey();
      if (lastKey == null) {
        return new NullCursor();
      }

      return new OCellBTreeIndexCursor(multiValueTree.iterateEntriesMinor(lastKey, true, false));
    }

    assert singleValueTree != null;

    final Object lastKey = singleValueTree.lastKey();
    if (lastKey == null) {
      return new NullCursor();
    }

    return new OMCellBTreeIndexCursor(singleValueTree.iterateEntriesMinor(new OCompositeKey(lastKey), true, false));
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    if (multiValueTree != null) {
      return new OIndexKeyCursor() {
        private final OCellBTreeMultiValue.OCellBTreeKeyCursor<Object> sbTreeKeyCursor = multiValueTree.keyCursor();

        @Override
        public Object next(int prefetchSize) {
          return sbTreeKeyCursor.next(prefetchSize);
        }
      };
    }

    assert singleValueTree != null;

    return new OIndexKeyCursor() {
      private final OCellBTee.OCellBTreeKeyCursor<OCompositeKey> keyCursor = singleValueTree.keyCursor();

      @Override
      public Object next(final int prefetchSize) {
        return convertMKey(keyCursor.next(prefetchSize));
      }
    };
  }

  @Override
  public void put(Object key, ORID value) {
    try {
      if (multiValueTree != null) {
        multiValueTree.put(key, value);
      } else {
        if (key != null) {
          assert singleValueTree != null;
          singleValueTree.put(new OCompositeKey(key, value), value);
        } else {
          assert nullValueTree != null;
          nullValueTree.put(value, true);
        }
      }
    } catch (IOException e) {
      throw OException
          .wrapException(new OIndexException("Error during insertion of key " + key + " and RID " + value + " to index " + name),
              e);
    }
  }

  @Override
  public Object getFirstKey() {
    if (multiValueTree != null) {
      return multiValueTree.firstKey();
    }

    assert singleValueTree != null;
    return convertMKey(singleValueTree.firstKey());
  }

  @Override
  public Object getLastKey() {
    if (multiValueTree != null) {
      return multiValueTree.lastKey();
    }

    assert singleValueTree != null;
    return convertMKey(singleValueTree.lastKey());
  }

  private static Object convertMKey(OCompositeKey key) {
    if (key == null) {
      return null;
    }

    final List<Object> keys = key.getKeys();
    assert keys.size() > 1;

    if (keys.size() == 2) {
      return keys.get(0);
    }

    return new OCompositeKey(keys.subList(0, keys.size() - 1));
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer) {
    if (multiValueTree != null) {
      return new OCellBTreeIndexCursor(
          multiValueTree.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder));
    }

    assert singleValueTree != null;

    return new OMCellBTreeIndexCursor(singleValueTree
        .iterateEntriesBetween(new OCompositeKey(rangeFrom), fromInclusive, new OCompositeKey(rangeTo), toInclusive, ascSortOrder));
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer transformer) {
    if (multiValueTree != null) {
      return new OCellBTreeIndexCursor(multiValueTree.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder));
    }

    assert singleValueTree != null;
    return new OMCellBTreeIndexCursor(singleValueTree.iterateEntriesMajor(new OCompositeKey(fromKey), isInclusive, ascSortOrder));
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    if (multiValueTree != null) {
      return new OCellBTreeIndexCursor(multiValueTree.iterateEntriesMinor(toKey, isInclusive, ascSortOrder));
    }

    assert singleValueTree != null;
    return new OMCellBTreeIndexCursor(singleValueTree.iterateEntriesMinor(new OCompositeKey(toKey), isInclusive, ascSortOrder));
  }

  @Override
  public long size(final ValuesTransformer transformer) {
    if (multiValueTree != null) {
      if (transformer == null) {
        final Object firstKey = multiValueTree.firstKey();
        final Object lastKey = multiValueTree.lastKey();

        int counter = 0;

        if (!multiValueTree.get(null).isEmpty()) {
          counter++;
        }

        if (firstKey != null && lastKey != null) {
          final OCellBTreeMultiValue.OCellBTreeCursor<Object, ORID> cursor = multiValueTree
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

      return multiValueTree.size();
    }

    assert singleValueTree != null;
    assert nullValueTree != null;

    if (transformer == null) {
      long counter = 0;
      if (nullValueTree.size() > 0) {
        counter++;
      }

      Object lastKey = null;

      final OCellBTee.OCellBTreeKeyCursor<OCompositeKey> cursor = singleValueTree.keyCursor();
      OCompositeKey key = cursor.next(-1);
      while (key != null) {

        final Object mKey = convertMKey(key);
        if (lastKey == null || !lastKey.equals(mKey)) {
          counter++;
        }

        lastKey = mKey;
        key = cursor.next(-1);
      }

      return counter;
    }

    long counter = nullValueTree.size();
    counter += singleValueTree.size();

    return counter;
  }

  @Override
  public long approximateSize() {
    if (singleValueTree != null) {
      assert nullValueTree != null;

      long counter = nullValueTree.size();
      counter += singleValueTree.size();

      return counter;
    }

    assert multiValueTree != null;
    return multiValueTree.size();
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return true;
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    if (multiValueTree != null) {
      multiValueTree.acquireAtomicExclusiveLock();
    } else {
      assert singleValueTree != null;
      singleValueTree.acquireAtomicExclusiveLock();
    }

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

  private static final class OMCellBTreeIndexCursor extends OIndexAbstractCursor {
    private final OCellBTee.OCellBTreeCursor<OCompositeKey, ORID> treeCursor;

    private OMCellBTreeIndexCursor(OCellBTee.OCellBTreeCursor<OCompositeKey, ORID> treeCursor) {
      this.treeCursor = treeCursor;
    }

    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {
      final Map.Entry<OCompositeKey, ORID> entry = treeCursor.next(getPrefetchSize());
      if (entry == null) {
        return null;
      }

      return new Map.Entry<Object, OIdentifiable>() {
        @Override
        public Object getKey() {
          return convertMKey(entry.getKey());
        }

        @Override
        public OIdentifiable getValue() {
          return entry.getValue();
        }

        @Override
        public OIdentifiable setValue(final OIdentifiable value) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
