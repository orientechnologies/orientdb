package com.orientechnologies.orient.core.storage.index.hashindex.local.v2;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.exception.NotEmptyComponentCanNotBeRemovedException;
import com.orientechnologies.orient.core.exception.OLocalHashTableV2Exception;
import com.orientechnologies.orient.core.exception.OTooBigIndexKeyException;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.index.engine.OIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashFunction;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashTable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Implementation of hash index which is based on <a
 * href="http://en.wikipedia.org/wiki/Extendible_hashing">extendible hashing algorithm</a>. The
 * directory for extindible hashing is implemented in {@link HashTableDirectory} class. Directory is
 * not implemented according to classic algorithm because of its big memory consumption in case of
 * non-uniform data distribution instead it is implemented according too "Multilevel Extendible
 * Hashing Sven Helmer, Thomas Neumann, Guido Moerkotte April 17, 2002". Which has much less memory
 * consumption in case of nonuniform data distribution. Index itself uses so called "multilevel
 * schema" when first level contains 256 buckets, when bucket is split it is put at the end of other
 * file which represents second level. So if data which are put has distribution close to uniform
 * (this index was designed to be use as rid index for DHT storage) buckets split will be preformed
 * in append only manner to speed up index write speed. So hash index bucket itself has following
 * structure:
 *
 * <ol>
 *   <li>Bucket depth - 1 byte.
 *   <li>Bucket's size - amount of entities (key, value) in one bucket, 4 bytes
 *   <li>Page indexes of parents of this bucket, page indexes of buckets split of which created
 *       current bucket - 64*8 bytes.
 *   <li>Offsets of entities stored in this bucket relatively to it's beginning. It is array of int
 *       values of undefined size.
 *   <li>Entities itself
 * </ol>
 *
 * So if 1-st and 2-nd fields are clear. We should discuss the last ones. Entities in bucket are
 * sorted by key's hash code so each entity has following storage format in bucket: key's hash code
 * (8 bytes), key, value. Because entities are stored in sorted order it means that every time when
 * we insert new entity old ones should be moved. There are 2 reasons why it is bad:
 *
 * <ol>
 *   <li>It will generate write ahead log of enormous size.
 *   <li>The more amount of memory is affected in operation the less speed we will have. In worst
 *       case 60 kb of memory should be moved.
 * </ol>
 *
 * To avoid disadvantages listed above entries ara appended to the end of bucket, but their offsets
 * are stored at the beginning of bucket. Offsets are stored in sorted order (ordered by hash code
 * of entity's key) so we need to move only small amount of memory to store entities in sorted
 * order. About indexes of parents of current bucket. When item is removed from bucket we check
 * space which is needed to store all entities of this bucket, it's buddy bucket (bucket which was
 * also created from parent bucket during split) and if space of single bucket is enough to save all
 * entities from both buckets we remove these buckets and put all content in parent bucket. That is
 * why we need indexes of parents of current bucket. Also hash index has special file of one page
 * long which contains information about state of each level of buckets in index. This information
 * is stored as array index of which equals to file level. All array item has following structure:
 *
 * <ol>
 *   <li>Is level removed (in case all buckets are empty or level was not created yet) - 1 byte
 *   <li>File's level id - 8 bytes
 *   <li>Amount of buckets in given level - 8 bytes.
 *   <li>Index of page of first removed bucket (is not split but removed) - 8 bytes
 * </ol>
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 12.03.13
 */
public class LocalHashTableV2<K, V> extends ODurableComponent implements OHashTable<K, V> {
  private static final int MAX_KEY_SIZE =
      OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.getValueAsInteger();

  private static final long HASH_CODE_MIN_VALUE = 0;
  private static final long HASH_CODE_MAX_VALUE = 0xFFFFFFFFFFFFFFFFL;
  private static final int OFFSET_MASK = 0xFF;

  private final String metadataConfigurationFileExtension;
  private final String treeStateFileExtension;

  private static final int HASH_CODE_SIZE = 64;
  private static final int MAX_LEVEL_DEPTH = 8;
  static final int MAX_LEVEL_SIZE = 1 << MAX_LEVEL_DEPTH;

  private static final int LEVEL_MASK = Integer.MAX_VALUE >>> (31 - MAX_LEVEL_DEPTH);

  private OHashFunction<K> keyHashFunction;

  private OBinarySerializer<K> keySerializer;
  private OBinarySerializer<V> valueSerializer;
  private OType[] keyTypes;

  private OHashTable.KeyHashCodeComparator<K> comparator;

  private boolean nullKeyIsSupported;
  private long nullBucketFileId = -1;
  private final String nullBucketFileExtension;

  private long fileStateId;
  private long fileId;

  private long hashStateEntryIndex;

  private HashTableDirectory directory;

  private OEncryption encryption;

  public LocalHashTableV2(
      final String name,
      final String metadataConfigurationFileExtension,
      final String treeStateFileExtension,
      final String bucketFileExtension,
      final String nullBucketFileExtension,
      final OAbstractPaginatedStorage abstractPaginatedStorage) {
    super(abstractPaginatedStorage, name, bucketFileExtension, name + bucketFileExtension);

    this.metadataConfigurationFileExtension = metadataConfigurationFileExtension;
    this.treeStateFileExtension = treeStateFileExtension;
    this.nullBucketFileExtension = nullBucketFileExtension;
  }

  @Override
  public void create(
      final OAtomicOperation atomicOperation,
      final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer,
      final OType[] keyTypes,
      final OEncryption encryption,
      final OHashFunction<K> keyHashFunction,
      final boolean nullKeyIsSupported)
      throws IOException {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            this.keyHashFunction = keyHashFunction;
            this.comparator = new OHashTable.KeyHashCodeComparator<>(this.keyHashFunction);
            this.encryption = encryption;

            if (keyTypes != null) {
              this.keyTypes = Arrays.copyOf(keyTypes, keyTypes.length);
            } else {
              this.keyTypes = null;
            }

            this.nullKeyIsSupported = nullKeyIsSupported;

            this.directory =
                new HashTableDirectory(treeStateFileExtension, getName(), getFullName(), storage);

            fileStateId = addFile(atomicOperation, getName() + metadataConfigurationFileExtension);

            directory.create(atomicOperation);

            try (final OCacheEntry hashStateEntry = addPage(atomicOperation, fileStateId)) {
              @SuppressWarnings("unused")
              final HashIndexMetadataPageV2 page = new HashIndexMetadataPageV2(hashStateEntry);
              page.init();

              hashStateEntryIndex = hashStateEntry.getPageIndex();
            }

            final String fileName = getFullName();
            fileId = addFile(atomicOperation, fileName);

            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;

            initHashTreeState(atomicOperation);

            if (nullKeyIsSupported) {
              nullBucketFileId = addFile(atomicOperation, getName() + nullBucketFileExtension);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public V get(K key) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        checkNullSupport(key);
        if (key == null) {
          if (getFilledUpTo(atomicOperation, nullBucketFileId) == 0) {
            return null;
          }

          V result;

          try (final OCacheEntry cacheEntry =
              loadPageForRead(atomicOperation, nullBucketFileId, 0)) {
            final HashIndexNullBucketV2<V> nullBucket = new HashIndexNullBucketV2<>(cacheEntry);
            result = nullBucket.getValue(valueSerializer);
          }

          return result;
        } else {
          key = keySerializer.preprocess(key, (Object[]) keyTypes);

          final long hashCode = keyHashFunction.hashCode(key);

          final OHashTable.BucketPath bucketPath = getBucket(hashCode, atomicOperation);
          final long bucketPointer =
              directory.getNodePointer(
                  bucketPath.nodeIndex,
                  bucketPath.itemIndex + bucketPath.hashMapOffset,
                  atomicOperation);

          if (bucketPointer == 0) {
            return null;
          }

          final long pageIndex = getPageIndex(bucketPointer);

          try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
            final HashIndexBucketV2<K, V> bucket = new HashIndexBucketV2<>(cacheEntry);

            final Entry<K, V> entry =
                bucket.find(key, hashCode, encryption, keySerializer, valueSerializer);
            if (entry == null) {
              return null;
            }

            return entry.value;
          }
        }

      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new OIndexException("Exception during index value retrieval"), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public boolean isNullKeyIsSupported() {
    acquireSharedLock();
    try {
      return nullKeyIsSupported;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void put(final OAtomicOperation atomicOperation, final K key, final V value) {
    put(atomicOperation, key, value, null);
  }

  @Override
  public boolean validatedPut(
      final OAtomicOperation atomicOperation,
      final K key,
      final V value,
      final OBaseIndexEngine.Validator<K, V> validator) {
    return put(atomicOperation, key, value, validator);
  }

  @Override
  public V remove(final OAtomicOperation atomicOperation, final K k) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            K key = k;
            checkNullSupport(key);

            int sizeDiff = 0;
            if (key != null) {
              key = keySerializer.preprocess(key, (Object[]) keyTypes);

              final long hashCode = keyHashFunction.hashCode(key);

              final OHashTable.BucketPath nodePath = getBucket(hashCode, atomicOperation);
              final long bucketPointer =
                  directory.getNodePointer(
                      nodePath.nodeIndex,
                      nodePath.itemIndex + nodePath.hashMapOffset,
                      atomicOperation);

              final long pageIndex = getPageIndex(bucketPointer);
              final V removed;
              final boolean found;

              try (final OCacheEntry cacheEntry =
                  loadPageForWrite(atomicOperation, fileId, pageIndex, true)) {
                final HashIndexBucketV2<K, V> bucket = new HashIndexBucketV2<>(cacheEntry);
                final int positionIndex = bucket.getIndex(hashCode, key, encryption, keySerializer);
                found = positionIndex >= 0;

                if (found) {
                  final byte[] rawKey = serializeKey(key);
                  final byte[] rawValue =
                      bucket.getRawValue(positionIndex, rawKey.length, valueSerializer);
                  removed = valueSerializer.deserializeNativeObject(rawValue, 0);
                  bucket.deleteEntry(positionIndex, hashCode, rawKey, rawValue);
                  sizeDiff--;
                } else {
                  removed = null;
                }
              }

              if (found) {
                if (nodePath.parent != null) {
                  final int hashMapSize = 1 << nodePath.nodeLocalDepth;

                  final boolean allMapsContainSameBucket =
                      checkAllMapsContainSameBucket(
                          directory.getNode(nodePath.nodeIndex, atomicOperation), hashMapSize);
                  if (allMapsContainSameBucket) {
                    mergeNodeToParent(nodePath, atomicOperation);
                  }
                }

                changeSize(sizeDiff, atomicOperation);
              }

              return removed;
            } else {
              if (getFilledUpTo(atomicOperation, nullBucketFileId) == 0) {
                return null;
              }

              V removed;

              OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, nullBucketFileId, 0, true);
              if (cacheEntry == null) {
                cacheEntry = addPage(atomicOperation, nullBucketFileId);
              }

              try {
                final HashIndexNullBucketV2<V> nullBucket = new HashIndexNullBucketV2<>(cacheEntry);

                final byte[] rawRemoved = nullBucket.getRawValue(valueSerializer);
                if (rawRemoved != null) {
                  removed = valueSerializer.deserializeNativeObject(rawRemoved, 0);
                  nullBucket.removeValue(rawRemoved);
                  sizeDiff--;
                } else {
                  removed = null;
                }
              } finally {
                cacheEntry.close();
              }

              changeSize(sizeDiff, atomicOperation);

              return removed;
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  private byte[] serializeKey(K key) {
    if (key == null) {
      return null;
    }

    final byte[] serializedKey = keySerializer.serializeNativeAsWhole(key, (Object[]) keyTypes);
    final byte[] rawKey;
    if (encryption == null) {
      rawKey = serializedKey;
    } else {
      final byte[] encryptedKey = encryption.encrypt(serializedKey);
      rawKey = new byte[encryptedKey.length + OIntegerSerializer.INT_SIZE];
      OIntegerSerializer.INSTANCE.serializeNative(encryptedKey.length, rawKey, 0);
      System.arraycopy(encryptedKey, 0, rawKey, OIntegerSerializer.INT_SIZE, encryptedKey.length);
    }
    return rawKey;
  }

  private void changeSize(final int sizeDiff, final OAtomicOperation atomicOperation)
      throws IOException {
    if (sizeDiff != 0) {
      try (final OCacheEntry hashStateEntry =
          loadPageForWrite(atomicOperation, fileStateId, hashStateEntryIndex, true)) {
        final HashIndexMetadataPageV2 page = new HashIndexMetadataPageV2(hashStateEntry);

        page.setRecordsCount(page.getRecordsCount() + sizeDiff);
      }
    }
  }

  @Override
  public Entry<K, V>[] higherEntries(final K key) {
    return higherEntries(key, -1);
  }

  @Override
  public Entry<K, V>[] higherEntries(K key, final int limit) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        final long hashCode = keyHashFunction.hashCode(key);
        OHashTable.BucketPath bucketPath = getBucket(hashCode, atomicOperation);
        final long bucketPointer =
            directory.getNodePointer(
                bucketPath.nodeIndex,
                bucketPath.itemIndex + bucketPath.hashMapOffset,
                atomicOperation);

        long pageIndex = getPageIndex(bucketPointer);

        OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex);
        try {
          HashIndexBucketV2<K, V> bucket = new HashIndexBucketV2<>(cacheEntry);

          while (bucket.size() == 0
              || comparator.compare(
                      bucket.getKey(bucket.size() - 1, encryption, keySerializer), key)
                  <= 0) {
            bucketPath = nextBucketToFind(bucketPath, bucket.getDepth(), atomicOperation);
            if (bucketPath == null) {
              //noinspection unchecked
              return OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
            }

            cacheEntry.close();

            final long nextPointer =
                directory.getNodePointer(
                    bucketPath.nodeIndex,
                    bucketPath.itemIndex + bucketPath.hashMapOffset,
                    atomicOperation);

            pageIndex = getPageIndex(nextPointer);

            cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex);
            bucket = new HashIndexBucketV2<>(cacheEntry);
          }

          final int index = bucket.getIndex(hashCode, key, encryption, keySerializer);
          final int startIndex;
          if (index >= 0) {
            startIndex = index + 1;
          } else {
            startIndex = -index - 1;
          }

          final int endIndex;
          if (limit <= 0) {
            endIndex = bucket.size();
          } else {
            endIndex = Math.min(bucket.size(), startIndex + limit);
          }

          return convertBucketToEntries(bucket, startIndex, endIndex);
        } finally {
          cacheEntry.close();
        }

      } finally {
        releaseSharedLock();
      }
    } catch (final IOException ioe) {
      throw OException.wrapException(
          new OLocalHashTableV2Exception("Exception during data retrieval", this), ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public void load(
      final String name,
      final OType[] keyTypes,
      final boolean nullKeyIsSupported,
      final OEncryption encryption,
      final OHashFunction<K> keyHashFunction,
      final OBinarySerializer<K> keySerializer,
      final OBinarySerializer<V> valueSerializer) {
    acquireExclusiveLock();
    try {
      this.keyHashFunction = keyHashFunction;
      this.comparator = new OHashTable.KeyHashCodeComparator<>(this.keyHashFunction);
      if (keyTypes != null) {
        this.keyTypes = Arrays.copyOf(keyTypes, keyTypes.length);
      } else {
        this.keyTypes = null;
      }

      this.encryption = encryption;
      this.nullKeyIsSupported = nullKeyIsSupported;

      final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

      fileStateId = openFile(atomicOperation, name + metadataConfigurationFileExtension);

      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;

      directory = new HashTableDirectory(treeStateFileExtension, name, getFullName(), storage);
      directory.open(atomicOperation);

      try (final OCacheEntry hashStateEntry = loadPageForRead(atomicOperation, fileStateId, 0)) {
        hashStateEntryIndex = hashStateEntry.getPageIndex();
      }

      if (nullKeyIsSupported) {
        nullBucketFileId = openFile(atomicOperation, name + nullBucketFileExtension);
      }

      fileId = openFile(atomicOperation, getFullName());
    } catch (final IOException e) {
      throw OException.wrapException(
          new OLocalHashTableV2Exception("Exception during hash table loading", this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private Entry<K, V>[] convertBucketToEntries(
      final HashIndexBucketV2<K, V> bucket, final int startIndex, final int endIndex) {
    @SuppressWarnings("unchecked")
    final Entry<K, V>[] entries = new Entry[endIndex - startIndex];
    final Iterator<Entry<K, V>> iterator =
        bucket.iterator(startIndex, keySerializer, valueSerializer, encryption);

    for (int i = 0, k = startIndex; k < endIndex; i++, k++) {
      entries[i] = iterator.next();
    }

    return entries;
  }

  private OHashTable.BucketPath nextBucketToFind(
      final OHashTable.BucketPath bucketPath,
      final int bucketDepth,
      final OAtomicOperation atomicOperation)
      throws IOException {
    int offset = bucketPath.nodeGlobalDepth - bucketDepth;

    OHashTable.BucketPath currentNode = bucketPath;
    int nodeLocalDepth = directory.getNodeLocalDepth(bucketPath.nodeIndex, atomicOperation);

    assert directory.getNodeLocalDepth(bucketPath.nodeIndex, atomicOperation)
        == bucketPath.nodeLocalDepth;

    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentNode = bucketPath.parent;
        nodeLocalDepth = currentNode.nodeLocalDepth;
        assert directory.getNodeLocalDepth(currentNode.nodeIndex, atomicOperation)
            == currentNode.nodeLocalDepth;
      }
    }

    final int diff = bucketDepth - (currentNode.nodeGlobalDepth - nodeLocalDepth);
    final int interval = (1 << (nodeLocalDepth - diff));
    final int firstStartIndex =
        currentNode.itemIndex & ((LEVEL_MASK << (nodeLocalDepth - diff)) & LEVEL_MASK);

    final OHashTable.BucketPath bucketPathToFind;
    final int globalIndex = firstStartIndex + interval + currentNode.hashMapOffset;
    if (globalIndex >= MAX_LEVEL_SIZE) {
      bucketPathToFind = nextLevelUp(currentNode, atomicOperation);
    } else {
      final int hashMapSize = 1 << currentNode.nodeLocalDepth;
      final int hashMapOffset = globalIndex / hashMapSize * hashMapSize;

      final int startIndex = globalIndex - hashMapOffset;

      bucketPathToFind =
          new OHashTable.BucketPath(
              currentNode.parent,
              hashMapOffset,
              startIndex,
              currentNode.nodeIndex,
              currentNode.nodeLocalDepth,
              currentNode.nodeGlobalDepth);
    }

    return nextNonEmptyNode(bucketPathToFind, atomicOperation);
  }

  private OHashTable.BucketPath nextNonEmptyNode(
      OHashTable.BucketPath bucketPath, final OAtomicOperation atomicOperation) throws IOException {
    nextBucketLoop:
    while (bucketPath != null) {
      final long[] node = directory.getNode(bucketPath.nodeIndex, atomicOperation);
      final int startIndex = bucketPath.itemIndex + bucketPath.hashMapOffset;
      @SuppressWarnings("UnnecessaryLocalVariable")
      final int endIndex = MAX_LEVEL_SIZE;

      for (int i = startIndex; i < endIndex; i++) {
        final long position = node[i];

        if (position > 0) {
          final int hashMapSize = 1 << bucketPath.nodeLocalDepth;
          final int hashMapOffset = (i / hashMapSize) * hashMapSize;
          final int itemIndex = i - hashMapOffset;

          return new OHashTable.BucketPath(
              bucketPath.parent,
              hashMapOffset,
              itemIndex,
              bucketPath.nodeIndex,
              bucketPath.nodeLocalDepth,
              bucketPath.nodeGlobalDepth);
        }

        if (position < 0) {
          final int childNodeIndex = (int) ((position & Long.MAX_VALUE) >> 8);
          final int childItemOffset = (int) position & 0xFF;

          final OHashTable.BucketPath parent =
              new OHashTable.BucketPath(
                  bucketPath.parent,
                  0,
                  i,
                  bucketPath.nodeIndex,
                  bucketPath.nodeLocalDepth,
                  bucketPath.nodeGlobalDepth);

          final int childLocalDepth = directory.getNodeLocalDepth(childNodeIndex, atomicOperation);
          bucketPath =
              new OHashTable.BucketPath(
                  parent,
                  childItemOffset,
                  0,
                  childNodeIndex,
                  childLocalDepth,
                  bucketPath.nodeGlobalDepth + childLocalDepth);

          continue nextBucketLoop;
        }
      }

      bucketPath = nextLevelUp(bucketPath, atomicOperation);
    }

    return null;
  }

  private OHashTable.BucketPath nextLevelUp(
      final OHashTable.BucketPath bucketPath, final OAtomicOperation atomicOperation)
      throws IOException {
    if (bucketPath.parent == null) {
      return null;
    }

    final int nodeLocalDepth = bucketPath.nodeLocalDepth;

    assert directory.getNodeLocalDepth(bucketPath.nodeIndex, atomicOperation)
        == bucketPath.nodeLocalDepth;

    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - nodeLocalDepth);

    final OHashTable.BucketPath parent = bucketPath.parent;

    if (parent.itemIndex < MAX_LEVEL_SIZE / 2) {
      final int nextParentIndex = (parent.itemIndex / pointersSize + 1) * pointersSize;
      return new OHashTable.BucketPath(
          parent.parent,
          0,
          nextParentIndex,
          parent.nodeIndex,
          parent.nodeLocalDepth,
          parent.nodeGlobalDepth);
    }

    final int nextParentIndex =
        ((parent.itemIndex - MAX_LEVEL_SIZE / 2) / pointersSize + 1) * pointersSize
            + MAX_LEVEL_SIZE / 2;
    if (nextParentIndex < MAX_LEVEL_SIZE) {
      return new BucketPath(
          parent.parent,
          0,
          nextParentIndex,
          parent.nodeIndex,
          parent.nodeLocalDepth,
          parent.nodeGlobalDepth);
    }

    return nextLevelUp(
        new OHashTable.BucketPath(
            parent.parent,
            0,
            MAX_LEVEL_SIZE - 1,
            parent.nodeIndex,
            parent.nodeLocalDepth,
            parent.nodeGlobalDepth),
        atomicOperation);
  }

  @Override
  public Entry<K, V>[] ceilingEntries(K key) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        final long hashCode = keyHashFunction.hashCode(key);
        OHashTable.BucketPath bucketPath = getBucket(hashCode, atomicOperation);

        final long bucketPointer =
            directory.getNodePointer(
                bucketPath.nodeIndex,
                bucketPath.itemIndex + bucketPath.hashMapOffset,
                atomicOperation);

        long pageIndex = getPageIndex(bucketPointer);

        OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex);
        try {
          HashIndexBucketV2<K, V> bucket = new HashIndexBucketV2<>(cacheEntry);
          while (bucket.size() == 0) {
            bucketPath = nextBucketToFind(bucketPath, bucket.getDepth(), atomicOperation);
            if (bucketPath == null) {
              //noinspection unchecked
              return OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
            }

            cacheEntry.close();
            final long nextPointer =
                directory.getNodePointer(
                    bucketPath.nodeIndex,
                    bucketPath.itemIndex + bucketPath.hashMapOffset,
                    atomicOperation);

            pageIndex = getPageIndex(nextPointer);

            cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex);
            bucket = new HashIndexBucketV2<>(cacheEntry);
          }

          final int index = bucket.getIndex(hashCode, key, encryption, keySerializer);
          final int startIndex;
          if (index >= 0) {
            startIndex = index;
          } else {
            startIndex = -index - 1;
          }

          final int endIndex = bucket.size();
          return convertBucketToEntries(bucket, startIndex, endIndex);
        } finally {
          cacheEntry.close();
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException ioe) {
      throw OException.wrapException(
          new OLocalHashTableV2Exception("Error during data retrieval", this), ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public Entry<K, V> firstEntry() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        OHashTable.BucketPath bucketPath = getBucket(HASH_CODE_MIN_VALUE, atomicOperation);
        final long bucketPointer =
            directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex, atomicOperation);

        long pageIndex = getPageIndex(bucketPointer);

        OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex);
        try {
          HashIndexBucketV2<K, V> bucket = new HashIndexBucketV2<>(cacheEntry);

          while (bucket.size() == 0) {
            bucketPath = nextBucketToFind(bucketPath, bucket.getDepth(), atomicOperation);
            if (bucketPath == null) {
              return null;
            }

            cacheEntry.close();
            final long nextPointer =
                directory.getNodePointer(
                    bucketPath.nodeIndex,
                    bucketPath.itemIndex + bucketPath.hashMapOffset,
                    atomicOperation);

            pageIndex = getPageIndex(nextPointer);

            cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex);
            bucket = new HashIndexBucketV2<>(cacheEntry);
          }

          return bucket.getEntry(0, encryption, keySerializer, valueSerializer);
        } finally {
          cacheEntry.close();
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException ioe) {
      throw OException.wrapException(
          new OLocalHashTableV2Exception("Exception during data read", this), ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public Entry<K, V> lastEntry() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        OHashTable.BucketPath bucketPath = getBucket(HASH_CODE_MAX_VALUE, atomicOperation);
        final long bucketPointer =
            directory.getNodePointer(
                bucketPath.nodeIndex,
                bucketPath.itemIndex + bucketPath.hashMapOffset,
                atomicOperation);

        long pageIndex = getPageIndex(bucketPointer);

        OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex);
        try {
          HashIndexBucketV2<K, V> bucket = new HashIndexBucketV2<>(cacheEntry);

          while (bucket.size() == 0) {
            final OHashTable.BucketPath prevBucketPath =
                prevBucketToFind(bucketPath, bucket.getDepth(), atomicOperation);
            if (prevBucketPath == null) {
              return null;
            }

            cacheEntry.close();

            final long prevPointer =
                directory.getNodePointer(
                    prevBucketPath.nodeIndex,
                    prevBucketPath.itemIndex + prevBucketPath.hashMapOffset,
                    atomicOperation);

            pageIndex = getPageIndex(prevPointer);

            cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex);

            bucket = new HashIndexBucketV2<>(cacheEntry);

            bucketPath = prevBucketPath;
          }

          return bucket.getEntry(bucket.size() - 1, encryption, keySerializer, valueSerializer);
        } finally {
          cacheEntry.close();
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException ioe) {
      throw OException.wrapException(
          new OLocalHashTableV2Exception("Exception during data read", this), ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public Entry<K, V>[] lowerEntries(K key) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        final long hashCode = keyHashFunction.hashCode(key);
        OHashTable.BucketPath bucketPath = getBucket(hashCode, atomicOperation);

        final long bucketPointer =
            directory.getNodePointer(
                bucketPath.nodeIndex,
                bucketPath.itemIndex + bucketPath.hashMapOffset,
                atomicOperation);

        long pageIndex = getPageIndex(bucketPointer);

        OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex);
        try {
          HashIndexBucketV2<K, V> bucket = new HashIndexBucketV2<>(cacheEntry);
          while (bucket.size() == 0
              || comparator.compare(bucket.getKey(0, encryption, keySerializer), key) >= 0) {
            final OHashTable.BucketPath prevBucketPath =
                prevBucketToFind(bucketPath, bucket.getDepth(), atomicOperation);
            if (prevBucketPath == null) {
              //noinspection unchecked
              return OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
            }

            cacheEntry.close();

            final long prevPointer =
                directory.getNodePointer(
                    prevBucketPath.nodeIndex,
                    prevBucketPath.itemIndex + prevBucketPath.hashMapOffset,
                    atomicOperation);

            pageIndex = getPageIndex(prevPointer);

            cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex);
            bucket = new HashIndexBucketV2<>(cacheEntry);

            bucketPath = prevBucketPath;
          }

          final int startIndex = 0;
          final int index = bucket.getIndex(hashCode, key, encryption, keySerializer);

          final int endIndex;
          if (index >= 0) {
            endIndex = index;
          } else {
            endIndex = -index - 1;
          }

          return convertBucketToEntries(bucket, startIndex, endIndex);
        } finally {
          cacheEntry.close();
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException ioe) {
      throw OException.wrapException(
          new OLocalHashTableV2Exception("Exception during data read", this), ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public Entry<K, V>[] floorEntries(K key) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        final long hashCode = keyHashFunction.hashCode(key);
        OHashTable.BucketPath bucketPath = getBucket(hashCode, atomicOperation);

        final long bucketPointer =
            directory.getNodePointer(
                bucketPath.nodeIndex,
                bucketPath.itemIndex + bucketPath.hashMapOffset,
                atomicOperation);

        long pageIndex = getPageIndex(bucketPointer);

        OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex);
        try {
          HashIndexBucketV2<K, V> bucket = new HashIndexBucketV2<>(cacheEntry);
          while (bucket.size() == 0) {
            final OHashTable.BucketPath prevBucketPath =
                prevBucketToFind(bucketPath, bucket.getDepth(), atomicOperation);
            if (prevBucketPath == null) {
              //noinspection unchecked
              return OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
            }

            cacheEntry.close();

            final long prevPointer =
                directory.getNodePointer(
                    prevBucketPath.nodeIndex,
                    prevBucketPath.itemIndex + prevBucketPath.hashMapOffset,
                    atomicOperation);

            pageIndex = getPageIndex(prevPointer);

            cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex);

            bucket = new HashIndexBucketV2<>(cacheEntry);

            bucketPath = prevBucketPath;
          }

          final int startIndex = 0;
          final int index = bucket.getIndex(hashCode, key, encryption, keySerializer);

          final int endIndex;
          if (index >= 0) {
            endIndex = index + 1;
          } else {
            endIndex = -index - 1;
          }

          return convertBucketToEntries(bucket, startIndex, endIndex);
        } finally {
          cacheEntry.close();
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException ioe) {
      throw OException.wrapException(
          new OLocalHashTableV2Exception("Exception during data read", this), ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  private OHashTable.BucketPath prevBucketToFind(
      final OHashTable.BucketPath bucketPath,
      final int bucketDepth,
      final OAtomicOperation atomicOperation)
      throws IOException {
    int offset = bucketPath.nodeGlobalDepth - bucketDepth;

    OHashTable.BucketPath currentBucket = bucketPath;
    int nodeLocalDepth = bucketPath.nodeLocalDepth;
    while (offset > 0) {
      offset -= nodeLocalDepth;

      //noinspection IfStatementMissingBreakInLoop
      if (offset > 0) {
        currentBucket = bucketPath.parent;
        nodeLocalDepth = currentBucket.nodeLocalDepth;
      }
    }

    final int diff = bucketDepth - (currentBucket.nodeGlobalDepth - nodeLocalDepth);
    final int firstStartIndex =
        currentBucket.itemIndex & ((LEVEL_MASK << (nodeLocalDepth - diff)) & LEVEL_MASK);
    final int globalIndex = firstStartIndex + currentBucket.hashMapOffset - 1;

    final OHashTable.BucketPath bucketPathToFind;
    if (globalIndex < 0) {
      bucketPathToFind = prevLevelUp(bucketPath);
    } else {
      final int hashMapSize = 1 << currentBucket.nodeLocalDepth;
      final int hashMapOffset = globalIndex / hashMapSize * hashMapSize;

      final int startIndex = globalIndex - hashMapOffset;

      bucketPathToFind =
          new OHashTable.BucketPath(
              currentBucket.parent,
              hashMapOffset,
              startIndex,
              currentBucket.nodeIndex,
              currentBucket.nodeLocalDepth,
              currentBucket.nodeGlobalDepth);
    }

    return prevNonEmptyNode(bucketPathToFind, atomicOperation);
  }

  private OHashTable.BucketPath prevNonEmptyNode(
      OHashTable.BucketPath nodePath, final OAtomicOperation atomicOperation) throws IOException {
    prevBucketLoop:
    while (nodePath != null) {
      final long[] node = directory.getNode(nodePath.nodeIndex, atomicOperation);
      final int startIndex = 0;
      final int endIndex = nodePath.itemIndex + nodePath.hashMapOffset;

      for (int i = endIndex; i >= startIndex; i--) {
        final long position = node[i];
        if (position > 0) {
          final int hashMapSize = 1 << nodePath.nodeLocalDepth;
          final int hashMapOffset = (i / hashMapSize) * hashMapSize;
          final int itemIndex = i - hashMapOffset;

          return new OHashTable.BucketPath(
              nodePath.parent,
              hashMapOffset,
              itemIndex,
              nodePath.nodeIndex,
              nodePath.nodeLocalDepth,
              nodePath.nodeGlobalDepth);
        }

        if (position < 0) {
          final int childNodeIndex = (int) ((position & Long.MAX_VALUE) >> 8);
          final int childItemOffset = (int) position & 0xFF;
          final int nodeLocalDepth = directory.getNodeLocalDepth(childNodeIndex, atomicOperation);
          final int endChildIndex = (1 << nodeLocalDepth) - 1;

          final OHashTable.BucketPath parent =
              new OHashTable.BucketPath(
                  nodePath.parent,
                  0,
                  i,
                  nodePath.nodeIndex,
                  nodePath.nodeLocalDepth,
                  nodePath.nodeGlobalDepth);
          nodePath =
              new OHashTable.BucketPath(
                  parent,
                  childItemOffset,
                  endChildIndex,
                  childNodeIndex,
                  nodeLocalDepth,
                  parent.nodeGlobalDepth + nodeLocalDepth);
          continue prevBucketLoop;
        }
      }

      nodePath = prevLevelUp(nodePath);
    }

    return null;
  }

  private static OHashTable.BucketPath prevLevelUp(final OHashTable.BucketPath bucketPath) {
    if (bucketPath.parent == null) {
      return null;
    }

    final int nodeLocalDepth = bucketPath.nodeLocalDepth;
    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - nodeLocalDepth);

    final OHashTable.BucketPath parent = bucketPath.parent;

    if (parent.itemIndex > MAX_LEVEL_SIZE / 2) {
      final int prevParentIndex =
          ((parent.itemIndex - MAX_LEVEL_SIZE / 2) / pointersSize) * pointersSize
              + MAX_LEVEL_SIZE / 2
              - 1;
      return new OHashTable.BucketPath(
          parent.parent,
          0,
          prevParentIndex,
          parent.nodeIndex,
          parent.nodeLocalDepth,
          parent.nodeGlobalDepth);
    }

    final int prevParentIndex = (parent.itemIndex / pointersSize) * pointersSize - 1;
    if (prevParentIndex >= 0) {
      return new BucketPath(
          parent.parent,
          0,
          prevParentIndex,
          parent.nodeIndex,
          parent.nodeLocalDepth,
          parent.nodeGlobalDepth);
    }

    return prevLevelUp(
        new OHashTable.BucketPath(
            parent.parent, 0, 0, parent.nodeIndex, parent.nodeLocalDepth, -1));
  }

  @Override
  public long size() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        try (final OCacheEntry hashStateEntry =
            loadPageForRead(atomicOperation, fileStateId, hashStateEntryIndex)) {
          final HashIndexMetadataPageV2 metadataPage = new HashIndexMetadataPageV2(hashStateEntry);
          return metadataPage.getRecordsCount();
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new OLocalHashTableV2Exception("Error during index size request", this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public void close() {
    acquireExclusiveLock();
    try {
      flush();

      directory.close();
      readCache.closeFile(fileStateId, true, writeCache);
      readCache.closeFile(fileId, true, writeCache);
    } catch (final IOException e) {
      throw OException.wrapException(
          new OLocalHashTableV2Exception("Error during hash table close", this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void delete(OAtomicOperation atomicOperation) throws IOException {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final long size = size();
            if (size > 0) {
              throw new NotEmptyComponentCanNotBeRemovedException(
                  getName()
                      + " : Not empty index can not be deleted. Index has "
                      + size
                      + " records");
            }

            directory.delete(atomicOperation);
            deleteFile(atomicOperation, fileStateId);
            deleteFile(atomicOperation, fileId);

            if (nullKeyIsSupported) {
              deleteFile(atomicOperation, nullBucketFileId);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  private void mergeNodeToParent(
      final OHashTable.BucketPath nodePath, final OAtomicOperation atomicOperation)
      throws IOException {
    final int startIndex = findParentNodeStartIndex(nodePath);
    final int localNodeDepth = nodePath.nodeLocalDepth;
    final int hashMapSize = 1 << localNodeDepth;

    final int parentIndex = nodePath.parent.nodeIndex;
    for (int i = 0, k = startIndex; i < MAX_LEVEL_SIZE; i += hashMapSize, k++) {
      directory.setNodePointer(
          parentIndex,
          k,
          directory.getNodePointer(nodePath.nodeIndex, i, atomicOperation),
          atomicOperation);
    }

    directory.deleteNode(nodePath.nodeIndex, atomicOperation);

    if (nodePath.parent.itemIndex < MAX_LEVEL_SIZE / 2) {
      final int maxChildDepth = directory.getMaxLeftChildDepth(parentIndex, atomicOperation);
      if (maxChildDepth == localNodeDepth) {
        directory.setMaxLeftChildDepth(
            parentIndex,
            (byte) getMaxLevelDepth(parentIndex, 0, MAX_LEVEL_SIZE / 2, atomicOperation),
            atomicOperation);
      }
    } else {
      final int maxChildDepth = directory.getMaxRightChildDepth(parentIndex, atomicOperation);
      if (maxChildDepth == localNodeDepth) {
        directory.setMaxRightChildDepth(
            parentIndex,
            (byte)
                getMaxLevelDepth(parentIndex, MAX_LEVEL_SIZE / 2, MAX_LEVEL_SIZE, atomicOperation),
            atomicOperation);
      }
    }
  }

  public void flush() {
    acquireExclusiveLock();
    try {
      writeCache.flush(fileStateId);
      writeCache.flush(fileId);

      directory.flush();

      if (nullKeyIsSupported) {
        writeCache.flush(nullBucketFileId);
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void acquireAtomicExclusiveLock() {
    atomicOperationsManager.acquireExclusiveLockTillOperationComplete(this);
  }

  private boolean put(
      final OAtomicOperation atomicOperation,
      K k,
      final V value,
      final OBaseIndexEngine.Validator<K, V> validator) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          K key = k;
          acquireExclusiveLock();
          try {
            checkNullSupport(key);

            if (key != null) {
              final int keySize = keySerializer.getObjectSize(key, (Object[]) keyTypes);
              if (keySize > MAX_KEY_SIZE) {
                throw new OTooBigIndexKeyException(
                    "Key size is more than allowed, operation was canceled. Current key size "
                        + keySize
                        + ", allowed  "
                        + MAX_KEY_SIZE,
                    getName());
              }
            }

            key = keySerializer.preprocess(key, (Object[]) keyTypes);
            return doPut(
                key,
                serializeKey(key),
                value,
                valueSerializer.serializeNativeAsWhole(value),
                validator,
                atomicOperation);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @SuppressWarnings("unchecked")
  private boolean doPut(
      final K key,
      final byte[] rawKey,
      V value,
      byte[] rawValue,
      final OIndexEngine.Validator<K, V> validator,
      final OAtomicOperation atomicOperation)
      throws IOException {
    int sizeDiff = 0;

    if (key == null) {
      final boolean isNew;
      final OCacheEntry cacheEntry;
      if (getFilledUpTo(atomicOperation, nullBucketFileId) == 0) {
        cacheEntry = addPage(atomicOperation, nullBucketFileId);
        isNew = true;
      } else {
        cacheEntry = loadPageForWrite(atomicOperation, nullBucketFileId, 0, true);
        isNew = false;
      }

      final V oldValue;
      try {
        final HashIndexNullBucketV2<V> nullBucket = new HashIndexNullBucketV2<>(cacheEntry);
        if (isNew) {
          nullBucket.init();
        }

        final byte[] oldRawValue = nullBucket.getRawValue(valueSerializer);
        oldValue =
            Optional.ofNullable(oldRawValue)
                .map(bytes -> valueSerializer.deserializeNativeObject(bytes, 0))
                .orElse(null);

        if (validator != null) {
          final Object result = validator.validate(null, oldValue, value);
          if (result == OBaseIndexEngine.Validator.IGNORE) {
            return false;
          }

          if (value != result) {
            value = (V) result;
            rawValue = valueSerializer.serializeNativeAsWhole(value);
          }
        }

        if (oldValue != null) {
          sizeDiff--;
        }

        nullBucket.setValue(rawValue, oldRawValue);
        sizeDiff++;
      } finally {
        cacheEntry.close();
      }

      changeSize(sizeDiff, atomicOperation);

    } else {
      final long hashCode = keyHashFunction.hashCode(key);

      final OHashTable.BucketPath bucketPath = getBucket(hashCode, atomicOperation);
      final long bucketPointer =
          directory.getNodePointer(
              bucketPath.nodeIndex,
              bucketPath.itemIndex + bucketPath.hashMapOffset,
              atomicOperation);
      if (bucketPointer == 0) {
        throw new IllegalStateException(
            "In this version of hash table buckets are added through split only.");
      }

      final long pageIndex = getPageIndex(bucketPointer);

      final OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true);
      try {
        final HashIndexBucketV2<K, V> bucket = new HashIndexBucketV2<>(cacheEntry);
        final int index = bucket.getIndex(hashCode, key, encryption, keySerializer);

        final byte[] oldRawValue =
            index > -1 ? bucket.getRawValue(index, rawKey.length, valueSerializer) : null;
        final V oldValue =
            Optional.ofNullable(oldRawValue)
                .map(bytes -> valueSerializer.deserializeNativeObject(bytes, 0))
                .orElse(null);

        if (validator != null) {
          final Object result = validator.validate(key, oldValue, value);
          if (result == OBaseIndexEngine.Validator.IGNORE) {
            return false;
          }

          if (result != value) {
            value = (V) result;
            rawValue = valueSerializer.serializeNativeAsWhole(value);
          }
        }

        if (index > -1) {
          assert oldRawValue != null;
          final int updateResult = bucket.updateEntry(index, rawValue, oldRawValue, rawKey.length);
          if (updateResult == 0) {
            // we already keep entry with given key-value.
            return true;
          }

          if (updateResult == 1) {
            changeSize(sizeDiff, atomicOperation);

            return true;
          }

          assert updateResult == -1;

          bucket.deleteEntry(index, hashCode, rawKey, oldRawValue);
          sizeDiff--;
        }

        if (bucket.addEntry(index < 0 ? -index - 1 : index, hashCode, rawKey, rawValue)) {
          sizeDiff++;

          changeSize(sizeDiff, atomicOperation);
          return true;
        }

        final OHashTable.BucketSplitResult splitResult =
            splitBucket(bucket, pageIndex, atomicOperation);

        final long updatedBucketPointer = splitResult.updatedBucketPointer;
        final long newBucketPointer = splitResult.newBucketPointer;
        final int bucketDepth = splitResult.newDepth;

        if (bucketDepth <= bucketPath.nodeGlobalDepth) {
          updateNodeAfterBucketSplit(
              bucketPath, bucketDepth, newBucketPointer, updatedBucketPointer, atomicOperation);
        } else {
          if (bucketPath.nodeLocalDepth < MAX_LEVEL_DEPTH) {
            final OHashTable.NodeSplitResult nodeSplitResult =
                splitNode(bucketPath, atomicOperation);

            assert !(nodeSplitResult.allLeftHashMapsEqual && nodeSplitResult.allRightHashMapsEqual);

            final long[] newNode = nodeSplitResult.newNode;

            final int nodeLocalDepth = bucketPath.nodeLocalDepth + 1;
            final int hashMapSize = 1 << nodeLocalDepth;

            assert nodeSplitResult.allRightHashMapsEqual
                == checkAllMapsContainSameBucket(newNode, hashMapSize);

            int newNodeIndex = -1;
            if (!nodeSplitResult.allRightHashMapsEqual
                || bucketPath.itemIndex >= MAX_LEVEL_SIZE / 2) {
              newNodeIndex =
                  directory.addNewNode(
                      (byte) 0, (byte) 0, (byte) nodeLocalDepth, newNode, atomicOperation);
            }

            final int updatedItemIndex = bucketPath.itemIndex << 1;
            final int updatedOffset = bucketPath.hashMapOffset << 1;
            final int updatedGlobalDepth = bucketPath.nodeGlobalDepth + 1;

            boolean allLeftHashMapsEqual = nodeSplitResult.allLeftHashMapsEqual;
            boolean allRightHashMapsEqual = nodeSplitResult.allRightHashMapsEqual;

            if (updatedOffset < MAX_LEVEL_SIZE) {
              allLeftHashMapsEqual = false;
              final OHashTable.BucketPath updatedBucketPath =
                  new OHashTable.BucketPath(
                      bucketPath.parent,
                      updatedOffset,
                      updatedItemIndex,
                      bucketPath.nodeIndex,
                      nodeLocalDepth,
                      updatedGlobalDepth);
              updateNodeAfterBucketSplit(
                  updatedBucketPath,
                  bucketDepth,
                  newBucketPointer,
                  updatedBucketPointer,
                  atomicOperation);
            } else {
              allRightHashMapsEqual = false;
              final OHashTable.BucketPath newBucketPath =
                  new OHashTable.BucketPath(
                      bucketPath.parent,
                      updatedOffset - MAX_LEVEL_SIZE,
                      updatedItemIndex,
                      newNodeIndex,
                      nodeLocalDepth,
                      updatedGlobalDepth);
              updateNodeAfterBucketSplit(
                  newBucketPath,
                  bucketDepth,
                  newBucketPointer,
                  updatedBucketPointer,
                  atomicOperation);
            }

            updateNodesAfterSplit(
                bucketPath,
                bucketPath.nodeIndex,
                newNode,
                nodeLocalDepth,
                hashMapSize,
                allLeftHashMapsEqual,
                allRightHashMapsEqual,
                newNodeIndex,
                atomicOperation);

            if (allLeftHashMapsEqual) {
              directory.deleteNode(bucketPath.nodeIndex, atomicOperation);
            }
          } else {
            addNewLevelNode(
                bucketPath,
                bucketPath.nodeIndex,
                newBucketPointer,
                updatedBucketPointer,
                atomicOperation);
          }
        }
      } finally {
        cacheEntry.close();
      }

      changeSize(sizeDiff, atomicOperation);
      doPut(key, rawKey, value, rawValue, null /* already validated */, atomicOperation);
    }
    return true;
  }

  private void checkNullSupport(final K key) {
    if (key == null && !nullKeyIsSupported) {
      throw new OLocalHashTableV2Exception("Null keys are not supported.", this);
    }
  }

  private void updateNodesAfterSplit(
      final OHashTable.BucketPath bucketPath,
      final int nodeIndex,
      final long[] newNode,
      final int nodeLocalDepth,
      final int hashMapSize,
      final boolean allLeftHashMapEquals,
      final boolean allRightHashMapsEquals,
      final int newNodeIndex,
      final OAtomicOperation atomicOperation)
      throws IOException {

    final int startIndex = findParentNodeStartIndex(bucketPath);

    final int parentNodeIndex = bucketPath.parent.nodeIndex;
    assert assertParentNodeStartIndex(
        bucketPath, directory.getNode(parentNodeIndex, atomicOperation), startIndex);

    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - nodeLocalDepth);
    if (allLeftHashMapEquals) {
      for (int i = 0; i < pointersSize; i++) {
        final long position = directory.getNodePointer(nodeIndex, i * hashMapSize, atomicOperation);
        directory.setNodePointer(parentNodeIndex, startIndex + i, position, atomicOperation);
      }
    } else {
      for (int i = 0; i < pointersSize; i++) {
        directory.setNodePointer(
            parentNodeIndex,
            startIndex + i,
            (bucketPath.nodeIndex << 8) | (i * hashMapSize) | Long.MIN_VALUE,
            atomicOperation);
      }
    }

    if (allRightHashMapsEquals) {
      for (int i = 0; i < pointersSize; i++) {
        final long position = newNode[i * hashMapSize];
        directory.setNodePointer(
            parentNodeIndex, startIndex + pointersSize + i, position, atomicOperation);
      }
    } else {
      for (int i = 0; i < pointersSize; i++) {
        directory.setNodePointer(
            parentNodeIndex,
            startIndex + pointersSize + i,
            (newNodeIndex << 8) | (i * hashMapSize) | Long.MIN_VALUE,
            atomicOperation);
      }
    }

    updateMaxChildDepth(bucketPath.parent, bucketPath.nodeLocalDepth + 1, atomicOperation);
  }

  private void updateMaxChildDepth(
      final OHashTable.BucketPath parentPath,
      final int childDepth,
      final OAtomicOperation atomicOperation)
      throws IOException {
    if (parentPath == null) {
      return;
    }

    if (parentPath.itemIndex < MAX_LEVEL_SIZE / 2) {
      final int maxChildDepth =
          directory.getMaxLeftChildDepth(parentPath.nodeIndex, atomicOperation);
      if (childDepth > maxChildDepth) {
        directory.setMaxLeftChildDepth(parentPath.nodeIndex, (byte) childDepth, atomicOperation);
      }
    } else {
      final int maxChildDepth =
          directory.getMaxRightChildDepth(parentPath.nodeIndex, atomicOperation);
      if (childDepth > maxChildDepth) {
        directory.setMaxRightChildDepth(parentPath.nodeIndex, (byte) childDepth, atomicOperation);
      }
    }
  }

  private static boolean assertParentNodeStartIndex(
      final OHashTable.BucketPath bucketPath, final long[] parentNode, final int calculatedIndex) {
    int startIndex = -1;
    for (int i = 0; i < parentNode.length; i++) {
      if (parentNode[i] < 0 && (parentNode[i] & Long.MAX_VALUE) >>> 8 == bucketPath.nodeIndex) {
        startIndex = i;
        break;
      }
    }

    return startIndex == calculatedIndex;
  }

  private static int findParentNodeStartIndex(final OHashTable.BucketPath bucketPath) {
    final OHashTable.BucketPath parentBucketPath = bucketPath.parent;
    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - bucketPath.nodeLocalDepth);

    if (parentBucketPath.itemIndex < MAX_LEVEL_SIZE / 2) {
      return (parentBucketPath.itemIndex / pointersSize) * pointersSize;
    }

    return ((parentBucketPath.itemIndex - MAX_LEVEL_SIZE / 2) / pointersSize) * pointersSize
        + MAX_LEVEL_SIZE / 2;
  }

  private void addNewLevelNode(
      final OHashTable.BucketPath bucketPath,
      final int nodeIndex,
      final long newBucketPointer,
      final long updatedBucketPointer,
      final OAtomicOperation atomicOperation)
      throws IOException {
    final int newNodeDepth;
    final int newNodeStartIndex;
    final int mapInterval;

    if (bucketPath.itemIndex < MAX_LEVEL_SIZE / 2) {
      final int maxDepth = directory.getMaxLeftChildDepth(bucketPath.nodeIndex, atomicOperation);

      assert getMaxLevelDepth(bucketPath.nodeIndex, 0, MAX_LEVEL_SIZE / 2, atomicOperation)
          == maxDepth;

      if (maxDepth > 0) {
        newNodeDepth = maxDepth;
      } else {
        newNodeDepth = 1;
      }

      mapInterval = 1 << (MAX_LEVEL_DEPTH - newNodeDepth);
      newNodeStartIndex = (bucketPath.itemIndex / mapInterval) * mapInterval;
    } else {
      final int maxDepth = directory.getMaxRightChildDepth(bucketPath.nodeIndex, atomicOperation);
      assert getMaxLevelDepth(
              bucketPath.nodeIndex, MAX_LEVEL_SIZE / 2, MAX_LEVEL_SIZE, atomicOperation)
          == maxDepth;
      if (maxDepth > 0) {
        newNodeDepth = maxDepth;
      } else {
        newNodeDepth = 1;
      }

      mapInterval = 1 << (MAX_LEVEL_DEPTH - newNodeDepth);
      newNodeStartIndex =
          ((bucketPath.itemIndex - MAX_LEVEL_SIZE / 2) / mapInterval) * mapInterval
              + MAX_LEVEL_SIZE / 2;
    }

    final int newNodeIndex =
        directory.addNewNode(
            (byte) 0, (byte) 0, (byte) newNodeDepth, new long[MAX_LEVEL_SIZE], atomicOperation);

    final int mapSize = 1 << newNodeDepth;
    for (int i = 0; i < mapInterval; i++) {
      final int nodeOffset = i + newNodeStartIndex;
      final long bucketPointer = directory.getNodePointer(nodeIndex, nodeOffset, atomicOperation);

      if (nodeOffset != bucketPath.itemIndex) {
        for (int n = i << newNodeDepth; n < (i + 1) << newNodeDepth; n++) {
          directory.setNodePointer(newNodeIndex, n, bucketPointer, atomicOperation);
        }
      } else {
        for (int n = i << newNodeDepth; n < (2 * i + 1) << (newNodeDepth - 1); n++) {
          directory.setNodePointer(newNodeIndex, n, updatedBucketPointer, atomicOperation);
        }

        for (int n = (2 * i + 1) << (newNodeDepth - 1); n < (i + 1) << newNodeDepth; n++) {
          directory.setNodePointer(newNodeIndex, n, newBucketPointer, atomicOperation);
        }
      }

      directory.setNodePointer(
          nodeIndex,
          nodeOffset,
          (newNodeIndex << 8) | (i * mapSize) | Long.MIN_VALUE,
          atomicOperation);
    }

    updateMaxChildDepth(bucketPath, newNodeDepth, atomicOperation);
  }

  private int getMaxLevelDepth(
      final int nodeIndex, final int start, final int end, final OAtomicOperation atomicOperation)
      throws IOException {
    int currentIndex = -1;
    int maxDepth = 0;

    for (int i = start; i < end; i++) {
      final long nodePosition = directory.getNodePointer(nodeIndex, i, atomicOperation);
      if (nodePosition >= 0) {
        continue;
      }

      final int index = (int) ((nodePosition & Long.MAX_VALUE) >>> 8);
      if (index == currentIndex) {
        continue;
      }

      currentIndex = index;

      final int nodeLocalDepth = directory.getNodeLocalDepth(index, atomicOperation);
      if (maxDepth < nodeLocalDepth) {
        maxDepth = nodeLocalDepth;
      }
    }

    return maxDepth;
  }

  private void updateNodeAfterBucketSplit(
      final OHashTable.BucketPath bucketPath,
      final int bucketDepth,
      final long newBucketPointer,
      final long updatedBucketPointer,
      final OAtomicOperation atomicOperation)
      throws IOException {
    int offset = bucketPath.nodeGlobalDepth - (bucketDepth - 1);
    OHashTable.BucketPath currentNode = bucketPath;
    int nodeLocalDepth = bucketPath.nodeLocalDepth;
    while (offset > 0) {
      offset -= nodeLocalDepth;
      //noinspection IfStatementMissingBreakInLoop
      if (offset > 0) {
        currentNode = bucketPath.parent;
        nodeLocalDepth = currentNode.nodeLocalDepth;
      }
    }

    final int diff = bucketDepth - 1 - (currentNode.nodeGlobalDepth - nodeLocalDepth);

    final int interval = (1 << (nodeLocalDepth - diff - 1));
    final int firstStartIndex =
        currentNode.itemIndex & ((LEVEL_MASK << (nodeLocalDepth - diff)) & LEVEL_MASK);
    final int firstEndIndex = firstStartIndex + interval;

    @SuppressWarnings("UnnecessaryLocalVariable")
    final int secondStartIndex = firstEndIndex;
    final int secondEndIndex = secondStartIndex + interval;

    for (int i = firstStartIndex; i < firstEndIndex; i++) {
      updateBucket(
          currentNode.nodeIndex,
          i,
          currentNode.hashMapOffset,
          updatedBucketPointer,
          atomicOperation);
    }

    for (int i = secondStartIndex; i < secondEndIndex; i++) {
      updateBucket(
          currentNode.nodeIndex, i, currentNode.hashMapOffset, newBucketPointer, atomicOperation);
    }
  }

  private static boolean checkAllMapsContainSameBucket(
      final long[] newNode, final int hashMapSize) {
    int n = 0;
    boolean allHashMapsEquals = true;
    while (n < newNode.length) {
      boolean allHashBucketEquals = true;
      for (int i = 0; i < hashMapSize - 1; i++) {
        if (newNode[i + n] != newNode[i + n + 1]) {
          allHashBucketEquals = false;
          break;
        }
      }
      n += hashMapSize;
      if (!allHashBucketEquals) {
        allHashMapsEquals = false;
        break;
      }
    }

    assert assertAllNodesAreFilePointers(allHashMapsEquals, newNode, hashMapSize);

    return allHashMapsEquals;
  }

  private static boolean assertAllNodesAreFilePointers(
      final boolean allHashMapsEquals, final long[] newNode, final int hashMapSize) {
    if (allHashMapsEquals) {
      int n = 0;
      while (n < newNode.length) {
        for (int i = 0; i < hashMapSize; i++) {
          if (newNode[i] < 0) {
            return false;
          }
        }
        n += hashMapSize;
      }
    }

    return true;
  }

  private OHashTable.NodeSplitResult splitNode(
      final OHashTable.BucketPath bucketPath, final OAtomicOperation atomicOperation)
      throws IOException {
    final long[] newNode = new long[MAX_LEVEL_SIZE];
    final int hashMapSize = 1 << (bucketPath.nodeLocalDepth + 1);

    boolean hashMapItemsAreEqual = true;
    final boolean allLeftItemsAreEqual;
    final boolean allRightItemsAreEqual;

    int mapCounter = 0;
    long firstPosition = -1;

    final long[] node = directory.getNode(bucketPath.nodeIndex, atomicOperation);

    for (int i = MAX_LEVEL_SIZE / 2; i < MAX_LEVEL_SIZE; i++) {
      final long position = node[i];
      if (hashMapItemsAreEqual && mapCounter == 0) {
        firstPosition = position;
      }

      newNode[2 * (i - MAX_LEVEL_SIZE / 2)] = position;
      newNode[2 * (i - MAX_LEVEL_SIZE / 2) + 1] = position;

      if (hashMapItemsAreEqual) {
        hashMapItemsAreEqual = firstPosition == position;
        mapCounter += 2;

        if (mapCounter >= hashMapSize) {
          mapCounter = 0;
        }
      }
    }

    mapCounter = 0;
    allRightItemsAreEqual = hashMapItemsAreEqual;

    hashMapItemsAreEqual = true;
    final long[] updatedNode = new long[node.length];
    for (int i = 0; i < MAX_LEVEL_SIZE / 2; i++) {
      final long position = node[i];
      if (hashMapItemsAreEqual && mapCounter == 0) {
        firstPosition = position;
      }

      updatedNode[2 * i] = position;
      updatedNode[2 * i + 1] = position;

      if (hashMapItemsAreEqual) {
        hashMapItemsAreEqual = firstPosition == position;

        mapCounter += 2;

        if (mapCounter >= hashMapSize) {
          mapCounter = 0;
        }
      }
    }

    allLeftItemsAreEqual = hashMapItemsAreEqual;

    directory.setNode(bucketPath.nodeIndex, updatedNode, atomicOperation);
    directory.setNodeLocalDepth(
        bucketPath.nodeIndex,
        (byte) (directory.getNodeLocalDepth(bucketPath.nodeIndex, atomicOperation) + 1),
        atomicOperation);

    return new OHashTable.NodeSplitResult(newNode, allLeftItemsAreEqual, allRightItemsAreEqual);
  }

  private void splitBucketContent(
      final HashIndexBucketV2<K, V> bucket,
      final HashIndexBucketV2<K, V> newBucket,
      final int newBucketDepth) {
    assert checkBucketDepth(bucket);

    final List<RawEntry> entries = new ArrayList<>(bucket.size());
    final Iterator<RawEntry> entryIterator =
        bucket.iterator(keySerializer, valueSerializer, encryption);
    while (entryIterator.hasNext()) {
      final RawEntry entry = entryIterator.next();
      entries.add(entry);
    }

    bucket.setDepth(newBucketDepth);

    final List<ORawPair<Integer, RawEntry>> entriesToRemove = new ArrayList<>();

    for (int i = 0; i < entries.size(); i++) {
      final RawEntry entry = entries.get(i);
      if (((entry.hashCode >>> (HASH_CODE_SIZE - newBucketDepth)) & 1) == 1) {
        entriesToRemove.add(new ORawPair<>(i, entry));
        newBucket.addEntry(newBucket.size(), entry.hashCode, entry.key, entry.value);
      }
    }

    for (int i = entriesToRemove.size() - 1; i >= 0; i--) {
      final ORawPair<Integer, RawEntry> pair = entriesToRemove.get(i);
      final RawEntry entry = pair.second;
      bucket.deleteEntry(pair.first, entry.hashCode, entry.key, entry.value);
    }

    assert checkBucketDepth(bucket);
    assert checkBucketDepth(newBucket);
  }

  private OHashTable.BucketSplitResult splitBucket(
      final HashIndexBucketV2<K, V> bucket,
      final long pageIndex,
      final OAtomicOperation atomicOperation)
      throws IOException {
    final int bucketDepth = bucket.getDepth();
    final int newBucketDepth = bucketDepth + 1;

    @SuppressWarnings("UnnecessaryLocalVariable")
    final long updatedBucketIndex = pageIndex;

    try (final OCacheEntry newBucketCacheEntry = addPage(atomicOperation, fileId)) {
      final HashIndexBucketV2<K, V> newBucket = new HashIndexBucketV2<>(newBucketCacheEntry);
      newBucket.init(newBucketDepth);

      splitBucketContent(bucket, newBucket, newBucketDepth);

      final long updatedBucketPointer = createBucketPointer(updatedBucketIndex);
      final long newBucketPointer = createBucketPointer(newBucketCacheEntry.getPageIndex());

      return new OHashTable.BucketSplitResult(
          updatedBucketPointer, newBucketPointer, newBucketDepth);
    }
  }

  private boolean checkBucketDepth(final HashIndexBucketV2<K, V> bucket) {
    final int bucketDepth = bucket.getDepth();

    if (bucket.size() == 0) {
      return true;
    }

    final Iterator<RawEntry> positionIterator =
        bucket.iterator(keySerializer, valueSerializer, encryption);

    final long firstValue = positionIterator.next().hashCode >>> (HASH_CODE_SIZE - bucketDepth);
    while (positionIterator.hasNext()) {
      final long value = positionIterator.next().hashCode >>> (HASH_CODE_SIZE - bucketDepth);
      if (value != firstValue) {
        return false;
      }
    }

    return true;
  }

  private void updateBucket(
      final int nodeIndex,
      final int itemIndex,
      final int offset,
      final long newBucketPointer,
      final OAtomicOperation atomicOperation)
      throws IOException {
    final long position = directory.getNodePointer(nodeIndex, itemIndex + offset, atomicOperation);
    if (position >= 0) {
      directory.setNodePointer(nodeIndex, itemIndex + offset, newBucketPointer, atomicOperation);
    } else {
      final int childNodeIndex = (int) ((position & Long.MAX_VALUE) >>> 8);
      final int childOffset = (int) (position & 0xFF);
      final int childNodeDepth = directory.getNodeLocalDepth(childNodeIndex, atomicOperation);
      final int interval = 1 << childNodeDepth;
      for (int i = 0; i < interval; i++) {
        updateBucket(childNodeIndex, i, childOffset, newBucketPointer, atomicOperation);
      }
    }
  }

  private void initHashTreeState(final OAtomicOperation atomicOperation) throws IOException {
    truncateFile(atomicOperation, fileId);

    for (long pageIndex = 0; pageIndex < MAX_LEVEL_SIZE; pageIndex++) {

      try (final OCacheEntry cacheEntry = addPage(atomicOperation, fileId)) {
        assert cacheEntry.getPageIndex() == pageIndex;
        @SuppressWarnings("unused")
        final HashIndexBucketV2<K, V> emptyBucket = new HashIndexBucketV2<>(cacheEntry);
        emptyBucket.init(MAX_LEVEL_DEPTH);
      }
    }

    final long[] rootTree = new long[MAX_LEVEL_SIZE];
    for (int pageIndex = 0; pageIndex < MAX_LEVEL_SIZE; pageIndex++) {
      rootTree[pageIndex] = createBucketPointer(pageIndex);
    }

    directory.clear(atomicOperation);
    directory.addNewNode((byte) 0, (byte) 0, (byte) MAX_LEVEL_DEPTH, rootTree, atomicOperation);

    try (final OCacheEntry hashStateEntry =
        loadPageForWrite(atomicOperation, fileStateId, hashStateEntryIndex, true)) {
      final HashIndexMetadataPageV2 metadataPage = new HashIndexMetadataPageV2(hashStateEntry);
      metadataPage.setRecordsCount(0);
    }
  }

  private static long createBucketPointer(final long pageIndex) {
    return pageIndex + 1;
  }

  private static long getPageIndex(final long bucketPointer) {
    return bucketPointer - 1;
  }

  private OHashTable.BucketPath getBucket(
      final long hashCode, final OAtomicOperation atomicOperation) throws IOException {
    int localNodeDepth = directory.getNodeLocalDepth(0, atomicOperation);
    int nodeDepth = localNodeDepth;
    OHashTable.BucketPath parentNode;
    int nodeIndex = 0;
    int offset = 0;

    int index =
        (int)
            ((hashCode >>> (HASH_CODE_SIZE - nodeDepth))
                & (LEVEL_MASK >>> (MAX_LEVEL_DEPTH - localNodeDepth)));
    OHashTable.BucketPath currentNode =
        new OHashTable.BucketPath(null, 0, index, 0, localNodeDepth, nodeDepth);
    do {
      final long position = directory.getNodePointer(nodeIndex, index + offset, atomicOperation);
      if (position >= 0) {
        return currentNode;
      }

      nodeIndex = (int) ((position & Long.MAX_VALUE) >>> 8);
      offset = (int) (position & OFFSET_MASK);

      localNodeDepth = directory.getNodeLocalDepth(nodeIndex, atomicOperation);
      nodeDepth += localNodeDepth;

      index =
          (int)
              ((hashCode >>> (HASH_CODE_SIZE - nodeDepth))
                  & (LEVEL_MASK >>> (MAX_LEVEL_DEPTH - localNodeDepth)));

      parentNode = currentNode;
      currentNode =
          new OHashTable.BucketPath(
              parentNode, offset, index, nodeIndex, localNodeDepth, nodeDepth);
    } while (nodeDepth <= HASH_CODE_SIZE);

    throw new IllegalStateException("Extendible hashing tree in corrupted state.");
  }
}
