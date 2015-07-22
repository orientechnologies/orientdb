package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTreeException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of hash index which is based on <a href="http://en.wikipedia.org/wiki/Extendible_hashing">extendible hashing
 * algorithm</a>. The directory for extindible hashing is implemented in
 * {@link com.orientechnologies.orient.core.index.hashindex.local.OHashTableDirectory} class. Directory is not implemented according
 * to classic algorithm because of its big memory consumption in case of non-uniform data distribution instead it is implemented
 * according too "Multilevel Extendible Hashing Sven Helmer, Thomas Neumann, Guido Moerkotte April 17, 2002". Which has much less
 * memory consumption in case of nonuniform data distribution.
 * 
 * Index itself uses so called "muiltilevel  schema" when first level contains 256 buckets, when bucket is split it is put at the
 * end of other file which represents second level. So if data which are put has distribution close to uniform (this index was
 * designed to be use as rid index for DHT storage) buckets split will be preformed in append only manner to speed up index write
 * speed.
 * 
 * So hash index bucket itself has following structure:
 * <ol>
 * <li>Bucket depth - 1 byte.</li>
 * <li>Bucket's size - amount of entities (key, value) in one bucket, 4 bytes</li>
 * <li>Page indexes of parents of this bucket, page indexes of buckets split of which created current bucket - 64*8 bytes.</li>
 * <li>Offsets of entities stored in this bucket relatively to it's beginning. It is array of int values of undefined size.</li>
 * <li>Entities itself</li>
 * </ol>
 * 
 * So if 1-st and 2-nd fields are clear. We should discuss the last ones.
 * 
 * 
 * Entities in bucket are sorted by key's hash code so each entity has following storage format in bucket: key's hash code (8
 * bytes), key, value. Because entities are stored in sorted order it means that every time when we insert new entity old ones
 * should be moved.
 * 
 * There are 2 reasons why it is bad:
 * <ol>
 * <li>It will generate write ahead log of enormous size.</li>
 * <li>The more amount of memory is affected in operation the less speed we will have. In worst case 60 kb of memory should be
 * moved.</li>
 * </ol>
 * 
 * To avoid disadvantages listed above entries ara appended to the end of bucket, but their offsets are stored at the beginning of
 * bucket. Offsets are stored in sorted order (ordered by hash code of entity's key) so we need to move only small amount of memory
 * to store entities in sorted order.
 * 
 * About indexes of parents of current bucket. When item is removed from bucket we check space which is needed to store all entities
 * of this bucket, it's buddy bucket (bucket which was also created from parent bucket during split) and if space of single bucket
 * is enough to save all entities from both buckets we remove these buckets and put all content in parent bucket. That is why we
 * need indexes of parents of current bucket.
 * 
 * Also hash index has special file of one page long which contains information about state of each level of buckets in index. This
 * information is stored as array index of which equals to file level. All array item has following structure:
 * <ol>
 * <li>Is level removed (in case all buckets are empty or level was not created yet) - 1 byte</li>
 * <li>File's level id - 8 bytes</li>
 * <li>Amount of buckets in given level - 8 bytes.</li>
 * <li>Index of page of first removed bucket (not splitted but removed) - 8 bytes</li>
 * </ol>
 * 
 * 
 * @author Andrey Lomakin
 * @since 12.03.13
 */
public class OLocalHashTable<K, V> extends ODurableComponent implements OHashTable<K, V> {
  private static final long                         HASH_CODE_MIN_VALUE = 0;
  private static final long                         HASH_CODE_MAX_VALUE = 0xFFFFFFFFFFFFFFFFL;

  private final String                              metadataConfigurationFileExtension;
  private final String                              treeStateFileExtension;

  public static final int                           HASH_CODE_SIZE      = 64;
  public static final int                           MAX_LEVEL_DEPTH     = 8;
  public static final int                           MAX_LEVEL_SIZE      = 1 << MAX_LEVEL_DEPTH;

  public static final int                           LEVEL_MASK          = Integer.MAX_VALUE >>> (31 - MAX_LEVEL_DEPTH);

  private final OHashFunction<K>                    keyHashFunction;

  private OBinarySerializer<K>                      keySerializer;
  private OBinarySerializer<V>                      valueSerializer;
  private OType[]                                   keyTypes;

  private final OHashTable.KeyHashCodeComparator<K> comparator;

  private boolean                                   nullKeyIsSupported;
  private long                                      nullBucketFileId    = -1;
  private final String                              nullBucketFileExtension;

  private long                                      fileStateId;
  private long                                      fileId;

  private long                                      hashStateEntryIndex;

  private OHashTableDirectory                       directory;

  private final boolean                             durableInNonTxMode;

  public OLocalHashTable(String name, String metadataConfigurationFileExtension, String treeStateFileExtension,
      String bucketFileExtension, String nullBucketFileExtension, OHashFunction<K> keyHashFunction, boolean durableInNonTxMode,
      OAbstractPaginatedStorage abstractPaginatedStorage) {
    super(abstractPaginatedStorage, name, bucketFileExtension);

    this.metadataConfigurationFileExtension = metadataConfigurationFileExtension;
    this.treeStateFileExtension = treeStateFileExtension;
    this.keyHashFunction = keyHashFunction;
    this.nullBucketFileExtension = nullBucketFileExtension;
    this.durableInNonTxMode = durableInNonTxMode;

    this.comparator = new OHashTable.KeyHashCodeComparator<K>(this.keyHashFunction);
  }

  @Override
  public void create(OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer, OType[] keyTypes,
      boolean nullKeyIsSupported) {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation();
    } catch (IOException e) {
      throw new OIndexException("Error during hash table creation.", e);
    }

    acquireExclusiveLock();
    try {
      try {

        this.keyTypes = keyTypes;
        this.nullKeyIsSupported = nullKeyIsSupported;

        this.directory = new OHashTableDirectory(treeStateFileExtension, getName(), durableInNonTxMode, storage);

        fileStateId = addFile(atomicOperation, getName() + metadataConfigurationFileExtension);

        directory.create();

        final OCacheEntry hashStateEntry = addPage(atomicOperation, fileStateId);
        pinPage(atomicOperation, hashStateEntry);

        hashStateEntry.acquireExclusiveLock();
        try {
          OHashIndexFileLevelMetadataPage page = new OHashIndexFileLevelMetadataPage(hashStateEntry, getChangesTree(
              atomicOperation, hashStateEntry), true);

          hashStateEntryIndex = hashStateEntry.getPageIndex();
        } finally {
          hashStateEntry.releaseExclusiveLock();
          releasePage(atomicOperation, hashStateEntry);
        }

        final String fileName = getFullName();
        fileId = addFile(atomicOperation, fileName);

        setKeySerializer(keySerializer);
        setValueSerializer(valueSerializer);

        initHashTreeState(atomicOperation);

        if (nullKeyIsSupported)
          nullBucketFileId = addFile(atomicOperation, getName() + nullBucketFileExtension);

        endAtomicOperation(false, null);
      } catch (IOException e) {
        endAtomicOperation(true, e);
        throw e;
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw new OStorageException(null, e);
      }
    } catch (IOException e) {
      throw new OIndexException("Error during local hash table creation.", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  protected OAtomicOperation startAtomicOperation() throws IOException {
    return atomicOperationsManager.startAtomicOperation(this, !durableInNonTxMode);
  }

  @Override
  public OBinarySerializer<K> getKeySerializer() {
    acquireSharedLock();
    try {
      return keySerializer;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void setKeySerializer(OBinarySerializer<K> keySerializer) {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation();
    } catch (IOException e) {
      throw new OIndexException("Error during hash set serializer for index keys.", e);
    }

    acquireExclusiveLock();
    try {
      this.keySerializer = keySerializer;
      OCacheEntry hashStateEntry = loadPage(atomicOperation, fileStateId, hashStateEntryIndex, true);
      hashStateEntry.acquireExclusiveLock();
      try {
        OHashIndexFileLevelMetadataPage metadataPage = new OHashIndexFileLevelMetadataPage(hashStateEntry, getChangesTree(
            atomicOperation, hashStateEntry), false);

        metadataPage.setKeySerializerId(keySerializer.getId());
      } finally {
        hashStateEntry.releaseExclusiveLock();
        releasePage(atomicOperation, hashStateEntry);
      }

      endAtomicOperation(false, null);
    } catch (IOException e) {
      rollback(e);

      throw new OIndexException("Can not set serializer for index keys", e);
    } catch (Exception e) {
      rollback(e);
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void rollback(Exception e) {
    try {
      endAtomicOperation(true, e);
    } catch (IOException ioe) {
      throw new OIndexException("Error during operation roolback", ioe);
    }
  }

  @Override
  public OBinarySerializer<V> getValueSerializer() {
    acquireSharedLock();
    try {
      return valueSerializer;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void setValueSerializer(OBinarySerializer<V> valueSerializer) {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation();
    } catch (IOException e) {
      throw new OIndexException("Error during hash table set serializer for index values", e);
    }

    acquireExclusiveLock();
    try {
      this.valueSerializer = valueSerializer;

      final OCacheEntry hashStateEntry = loadPage(atomicOperation, fileStateId, hashStateEntryIndex, true);
      hashStateEntry.acquireExclusiveLock();
      try {
        OHashIndexFileLevelMetadataPage metadataPage = new OHashIndexFileLevelMetadataPage(hashStateEntry, getChangesTree(
            atomicOperation, hashStateEntry), false);

        metadataPage.setValueSerializerId(valueSerializer.getId());
      } finally {
        hashStateEntry.releaseExclusiveLock();
        releasePage(atomicOperation, hashStateEntry);
      }

      endAtomicOperation(false, null);
    } catch (IOException e) {
      rollback(e);
      throw new OIndexException("Can not set serializer for index values", e);
    } catch (Exception e) {
      rollback(e);
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public V get(K key) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        checkNullSupport(key);
        if (key == null) {
          if (getFilledUpTo(atomicOperation, nullBucketFileId) == 0)
            return null;

          V result = null;
          OCacheEntry cacheEntry = loadPage(atomicOperation, nullBucketFileId, 0, false);
          try {
            ONullBucket<V> nullBucket = new ONullBucket<V>(cacheEntry, getChangesTree(atomicOperation, cacheEntry),
                valueSerializer, false);
            result = nullBucket.getValue();
          } finally {
            releasePage(atomicOperation, cacheEntry);
          }

          return result;
        } else {
          key = keySerializer.preprocess(key, (Object[]) keyTypes);

          final long hashCode = keyHashFunction.hashCode(key);

          OHashTable.BucketPath bucketPath = getBucket(hashCode);
          final long bucketPointer = directory
              .getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

          if (bucketPointer == 0)
            return null;

          final long pageIndex = getPageIndex(bucketPointer);

          OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
          try {
            final OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
                getChangesTree(atomicOperation, cacheEntry));

            OHashIndexBucket.Entry<K, V> entry = bucket.find(key, hashCode);
            if (entry == null)
              return null;

            return entry.value;
          } finally {
            releasePage(atomicOperation, cacheEntry);
          }
        }

      } finally {
        releaseSharedLock();
      }
    } catch (IOException e) {
      throw new OIndexException("Exception during index value retrieval", e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public void put(K key, V value) {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation();
    } catch (IOException e) {
      throw new OIndexException("Error during hash table entry put", e);
    }
    acquireExclusiveLock();
    try {

      checkNullSupport(key);

      key = keySerializer.preprocess(key, (Object[]) keyTypes);

      doPut(key, value, atomicOperation);

      endAtomicOperation(false, null);
    } catch (IOException e) {
      rollback(e);
      throw new OIndexException("Error during index update", e);
    } catch (Exception e) {
      rollback(e);
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public V remove(K key) {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation();
    } catch (IOException e) {
      throw new OIndexException("Error during hash table entry deletion.", e);
    }

    acquireExclusiveLock();
    try {
      checkNullSupport(key);

      int sizeDiff = 0;
      if (key != null) {
        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        final long hashCode = keyHashFunction.hashCode(key);

        final OHashTable.BucketPath nodePath = getBucket(hashCode);
        final long bucketPointer = directory.getNodePointer(nodePath.nodeIndex, nodePath.itemIndex + nodePath.hashMapOffset);

        final long pageIndex = getPageIndex(bucketPointer);
        final V removed;

        final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
        cacheEntry.acquireExclusiveLock();
        try {
          final OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
              getChangesTree(atomicOperation, cacheEntry));
          final int positionIndex = bucket.getIndex(hashCode, key);
          if (positionIndex < 0) {
            endAtomicOperation(false, null);
            return null;
          }

          removed = bucket.deleteEntry(positionIndex).value;
          sizeDiff--;
        } finally {
          cacheEntry.releaseExclusiveLock();
          releasePage(atomicOperation, cacheEntry);
        }

        if (nodePath.parent != null) {
          final int hashMapSize = 1 << nodePath.nodeLocalDepth;

          final boolean allMapsContainSameBucket = checkAllMapsContainSameBucket(directory.getNode(nodePath.nodeIndex), hashMapSize);
          if (allMapsContainSameBucket)
            mergeNodeToParent(nodePath);
        }

        changeSize(sizeDiff, atomicOperation);

        endAtomicOperation(false, null);
        return removed;
      } else {
        if (getFilledUpTo(atomicOperation, nullBucketFileId) == 0) {
          endAtomicOperation(false, null);
          return null;
        }

        V removed = null;

        OCacheEntry cacheEntry = loadPage(atomicOperation, nullBucketFileId, 0, false);
        if (cacheEntry == null)
          cacheEntry = addPage(atomicOperation, nullBucketFileId);

        cacheEntry.acquireExclusiveLock();
        try {
          final ONullBucket<V> nullBucket = new ONullBucket<V>(cacheEntry, getChangesTree(atomicOperation, cacheEntry),
              valueSerializer, false);

          removed = nullBucket.getValue();
          if (removed != null) {
            nullBucket.removeValue();
            sizeDiff--;
          }
        } finally {
          cacheEntry.releaseExclusiveLock();
          releasePage(atomicOperation, cacheEntry);
        }

        changeSize(sizeDiff, atomicOperation);

        endAtomicOperation(false, null);
        return removed;
      }
    } catch (IOException e) {
      rollback(e);
      throw new OIndexException("Error during index removal", e);
    } catch (Exception e) {
      rollback(e);
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void changeSize(int sizeDiff, OAtomicOperation atomicOperation) throws IOException {
    if (sizeDiff != 0) {
      OCacheEntry hashStateEntry = loadPage(atomicOperation, fileStateId, hashStateEntryIndex, true);
      hashStateEntry.acquireExclusiveLock();
      try {
        OHashIndexFileLevelMetadataPage page = new OHashIndexFileLevelMetadataPage(hashStateEntry, getChangesTree(atomicOperation,
            hashStateEntry), false);

        page.setRecordsCount(page.getRecordsCount() + sizeDiff);
      } finally {
        hashStateEntry.releaseExclusiveLock();
        releasePage(atomicOperation, hashStateEntry);
      }
    }
  }

  @Override
  public void clear() {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation();
    } catch (IOException e) {
      throw new OIndexException("Error during hash table clear", e);
    }

    acquireExclusiveLock();
    try {
      if (nullKeyIsSupported)
        truncateFile(atomicOperation, nullBucketFileId);

      initHashTreeState(atomicOperation);

      endAtomicOperation(false, null);
    } catch (IOException e) {
      rollback(e);
      throw new OIndexException("Error during hash table clear", e);
    } catch (Exception e) {
      rollback(e);
      throw new OSBTreeException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public OHashIndexBucket.Entry<K, V>[] higherEntries(K key) {
    return higherEntries(key, -1);
  }

  @Override
  public OHashIndexBucket.Entry<K, V>[] higherEntries(K key, int limit) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        final long hashCode = keyHashFunction.hashCode(key);
        OHashTable.BucketPath bucketPath = getBucket(hashCode);
        long bucketPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

        long pageIndex = getPageIndex(bucketPointer);

        OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
        try {
          OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
              getChangesTree(atomicOperation, cacheEntry));

          while (bucket.size() == 0 || comparator.compare(bucket.getKey(bucket.size() - 1), key) <= 0) {
            bucketPath = nextBucketToFind(bucketPath, bucket.getDepth());
            if (bucketPath == null)
              return OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;

            releasePage(atomicOperation, cacheEntry);

            final long nextPointer = directory
                .getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

            pageIndex = getPageIndex(nextPointer);

            cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
            bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes, getChangesTree(
                atomicOperation, cacheEntry));
          }

          final int index = bucket.getIndex(hashCode, key);
          final int startIndex;
          if (index >= 0)
            startIndex = index + 1;
          else
            startIndex = -index - 1;

          final int endIndex;
          if (limit <= 0)
            endIndex = bucket.size();
          else
            endIndex = Math.min(bucket.size(), startIndex + limit);

          return convertBucketToEntries(bucket, startIndex, endIndex);
        } finally {
          releasePage(atomicOperation, cacheEntry);
        }

      } finally {
        releaseSharedLock();
      }
    } catch (IOException ioe) {
      throw new OIndexException("Exception during data retrieval", ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public void load(String name, OType[] keyTypes, boolean nullKeyIsSupported) {
    acquireExclusiveLock();
    try {
      this.keyTypes = keyTypes;
      this.nullKeyIsSupported = nullKeyIsSupported;

      OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

      fileStateId = openFile(atomicOperation, name + metadataConfigurationFileExtension);
      final OCacheEntry hashStateEntry = loadPage(atomicOperation, fileStateId, 0, true);
      hashStateEntryIndex = hashStateEntry.getPageIndex();

      directory = new OHashTableDirectory(treeStateFileExtension, name, durableInNonTxMode, storage);
      directory.open();

      pinPage(atomicOperation, hashStateEntry);
      try {
        OHashIndexFileLevelMetadataPage page = new OHashIndexFileLevelMetadataPage(hashStateEntry, getChangesTree(atomicOperation,
            hashStateEntry), false);
        keySerializer = (OBinarySerializer<K>) OBinarySerializerFactory.getInstance()
            .getObjectSerializer(page.getKeySerializerId());
        valueSerializer = (OBinarySerializer<V>) OBinarySerializerFactory.getInstance().getObjectSerializer(
            page.getValueSerializerId());
      } finally {
        releasePage(atomicOperation, hashStateEntry);
      }

      if (nullKeyIsSupported)
        nullBucketFileId = openFile(atomicOperation, name + nullBucketFileExtension);

      fileId = openFile(atomicOperation, getFullName());
    } catch (IOException e) {
      throw new OIndexException("Exception during hash table loading", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void deleteWithoutLoad(String name, OAbstractPaginatedStorage storageLocal) {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation();
    } catch (IOException e) {
      throw new OIndexException("Error during hash table deletion.", e);
    }

    acquireExclusiveLock();
    try {

      if (isFileExists(atomicOperation, name + metadataConfigurationFileExtension)) {
        fileStateId = openFile(atomicOperation, name + metadataConfigurationFileExtension);
        deleteFile(atomicOperation, fileStateId);
      }

      directory = new OHashTableDirectory(treeStateFileExtension, name, durableInNonTxMode, storage);
      directory.deleteWithoutOpen();

      if (isFileExists(atomicOperation, name + nullBucketFileExtension)) {
        final long nullBucketId = openFile(atomicOperation, name + nullBucketFileExtension);
        deleteFile(atomicOperation, nullBucketId);
      }

      if (isFileExists(atomicOperation, getFullName())) {
        final long fileId = openFile(atomicOperation, getFullName());
        deleteFile(atomicOperation, fileId);
      }

      endAtomicOperation(false, null);
    } catch (IOException ioe) {
      rollback(ioe);
      throw new OIndexException("Can not delete hash table with name " + name, ioe);
    } catch (Exception e) {
      rollback(e);
      throw new OIndexException("Can not delete hash table with name " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private OHashIndexBucket.Entry<K, V>[] convertBucketToEntries(final OHashIndexBucket<K, V> bucket, int startIndex, int endIndex) {
    final OHashIndexBucket.Entry<K, V>[] entries = new OHashIndexBucket.Entry[endIndex - startIndex];
    final Iterator<OHashIndexBucket.Entry<K, V>> iterator = bucket.iterator(startIndex);

    for (int i = 0, k = startIndex; k < endIndex; i++, k++)
      entries[i] = iterator.next();

    return entries;
  }

  private OHashTable.BucketPath nextBucketToFind(final OHashTable.BucketPath bucketPath, int bucketDepth) throws IOException {
    int offset = bucketPath.nodeGlobalDepth - bucketDepth;

    OHashTable.BucketPath currentNode = bucketPath;
    int nodeLocalDepth = directory.getNodeLocalDepth(bucketPath.nodeIndex);

    assert directory.getNodeLocalDepth(bucketPath.nodeIndex) == bucketPath.nodeLocalDepth;

    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentNode = bucketPath.parent;
        nodeLocalDepth = currentNode.nodeLocalDepth;
        assert directory.getNodeLocalDepth(currentNode.nodeIndex) == currentNode.nodeLocalDepth;
      }
    }

    final int diff = bucketDepth - (currentNode.nodeGlobalDepth - nodeLocalDepth);
    final int interval = (1 << (nodeLocalDepth - diff));
    final int firstStartIndex = currentNode.itemIndex & ((LEVEL_MASK << (nodeLocalDepth - diff)) & LEVEL_MASK);

    final OHashTable.BucketPath bucketPathToFind;
    final int globalIndex = firstStartIndex + interval + currentNode.hashMapOffset;
    if (globalIndex >= MAX_LEVEL_SIZE)
      bucketPathToFind = nextLevelUp(currentNode);
    else {
      final int hashMapSize = 1 << currentNode.nodeLocalDepth;
      final int hashMapOffset = globalIndex / hashMapSize * hashMapSize;

      final int startIndex = globalIndex - hashMapOffset;

      bucketPathToFind = new OHashTable.BucketPath(currentNode.parent, hashMapOffset, startIndex, currentNode.nodeIndex,
          currentNode.nodeLocalDepth, currentNode.nodeGlobalDepth);
    }

    return nextNonEmptyNode(bucketPathToFind);
  }

  private OHashTable.BucketPath nextNonEmptyNode(OHashTable.BucketPath bucketPath) throws IOException {
    nextBucketLoop: while (bucketPath != null) {
      final long[] node = directory.getNode(bucketPath.nodeIndex);
      final int startIndex = bucketPath.itemIndex + bucketPath.hashMapOffset;
      final int endIndex = MAX_LEVEL_SIZE;

      for (int i = startIndex; i < endIndex; i++) {
        final long position = node[i];

        if (position > 0) {
          final int hashMapSize = 1 << bucketPath.nodeLocalDepth;
          final int hashMapOffset = (i / hashMapSize) * hashMapSize;
          final int itemIndex = i - hashMapOffset;

          return new OHashTable.BucketPath(bucketPath.parent, hashMapOffset, itemIndex, bucketPath.nodeIndex,
              bucketPath.nodeLocalDepth, bucketPath.nodeGlobalDepth);
        }

        if (position < 0) {
          final int childNodeIndex = (int) ((position & Long.MAX_VALUE) >> 8);
          final int childItemOffset = (int) position & 0xFF;

          final OHashTable.BucketPath parent = new OHashTable.BucketPath(bucketPath.parent, 0, i, bucketPath.nodeIndex,
              bucketPath.nodeLocalDepth, bucketPath.nodeGlobalDepth);

          final int childLocalDepth = directory.getNodeLocalDepth(childNodeIndex);
          bucketPath = new OHashTable.BucketPath(parent, childItemOffset, 0, childNodeIndex, childLocalDepth,
              bucketPath.nodeGlobalDepth + childLocalDepth);

          continue nextBucketLoop;
        }
      }

      bucketPath = nextLevelUp(bucketPath);
    }

    return null;
  }

  private OHashTable.BucketPath nextLevelUp(OHashTable.BucketPath bucketPath) throws IOException {
    if (bucketPath.parent == null)
      return null;

    final int nodeLocalDepth = bucketPath.nodeLocalDepth;

    assert directory.getNodeLocalDepth(bucketPath.nodeIndex) == bucketPath.nodeLocalDepth;

    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - nodeLocalDepth);

    final OHashTable.BucketPath parent = bucketPath.parent;

    if (parent.itemIndex < MAX_LEVEL_SIZE / 2) {
      final int nextParentIndex = (parent.itemIndex / pointersSize + 1) * pointersSize;
      return new OHashTable.BucketPath(parent.parent, 0, nextParentIndex, parent.nodeIndex, parent.nodeLocalDepth,
          parent.nodeGlobalDepth);
    }

    final int nextParentIndex = ((parent.itemIndex - MAX_LEVEL_SIZE / 2) / pointersSize + 1) * pointersSize + MAX_LEVEL_SIZE / 2;
    if (nextParentIndex < MAX_LEVEL_SIZE)
      return new OHashTable.BucketPath(parent.parent, 0, nextParentIndex, parent.nodeIndex, parent.nodeLocalDepth,
          parent.nodeGlobalDepth);

    return nextLevelUp(new OHashTable.BucketPath(parent.parent, 0, MAX_LEVEL_SIZE - 1, parent.nodeIndex, parent.nodeLocalDepth,
        parent.nodeGlobalDepth));
  }

  @Override
  public OHashIndexBucket.Entry<K, V>[] ceilingEntries(K key) {

    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        final long hashCode = keyHashFunction.hashCode(key);
        OHashTable.BucketPath bucketPath = getBucket(hashCode);

        long bucketPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

        long pageIndex = getPageIndex(bucketPointer);

        OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
        try {
          OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
              getChangesTree(atomicOperation, cacheEntry));
          while (bucket.size() == 0) {
            bucketPath = nextBucketToFind(bucketPath, bucket.getDepth());
            if (bucketPath == null)
              return OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;

            releasePage(atomicOperation, cacheEntry);
            final long nextPointer = directory
                .getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

            pageIndex = getPageIndex(nextPointer);

            cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
            bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes, getChangesTree(
                atomicOperation, cacheEntry));
          }

          final int index = bucket.getIndex(hashCode, key);
          final int startIndex;
          if (index >= 0)
            startIndex = index;
          else
            startIndex = -index - 1;

          final int endIndex = bucket.size();
          return convertBucketToEntries(bucket, startIndex, endIndex);
        } finally {
          releasePage(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException ioe) {
      throw new OIndexException("Error during data retrieval", ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OHashIndexBucket.Entry<K, V> firstEntry() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        OHashTable.BucketPath bucketPath = getBucket(HASH_CODE_MIN_VALUE);
        long bucketPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex);

        long pageIndex = getPageIndex(bucketPointer);
        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
        try {
          OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
              getChangesTree(atomicOperation, cacheEntry));

          while (bucket.size() == 0) {
            bucketPath = nextBucketToFind(bucketPath, bucket.getDepth());
            if (bucketPath == null)
              return null;

            releasePage(atomicOperation, cacheEntry);
            final long nextPointer = directory
                .getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

            pageIndex = getPageIndex(nextPointer);

            cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
            bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes, getChangesTree(
                atomicOperation, cacheEntry));
          }

          return bucket.getEntry(0);
        } finally {
          releasePage(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException ioe) {
      throw new OIndexException("Exception during data read", ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OHashIndexBucket.Entry<K, V> lastEntry() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        OHashTable.BucketPath bucketPath = getBucket(HASH_CODE_MAX_VALUE);
        long bucketPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

        long pageIndex = getPageIndex(bucketPointer);

        OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
        try {
          OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
              getChangesTree(atomicOperation, cacheEntry));

          while (bucket.size() == 0) {
            final OHashTable.BucketPath prevBucketPath = prevBucketToFind(bucketPath, bucket.getDepth());
            if (prevBucketPath == null)
              return null;

            releasePage(atomicOperation, cacheEntry);
            final long prevPointer = directory.getNodePointer(prevBucketPath.nodeIndex, prevBucketPath.itemIndex
                + prevBucketPath.hashMapOffset);

            pageIndex = getPageIndex(prevPointer);

            cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
            bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes, getChangesTree(
                atomicOperation, cacheEntry));

            bucketPath = prevBucketPath;
          }

          return bucket.getEntry(bucket.size() - 1);
        } finally {
          releasePage(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException ioe) {
      throw new OIndexException("Exception during data read", ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OHashIndexBucket.Entry<K, V>[] lowerEntries(K key) {

    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        final long hashCode = keyHashFunction.hashCode(key);
        OHashTable.BucketPath bucketPath = getBucket(hashCode);

        long bucketPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

        long pageIndex = getPageIndex(bucketPointer);
        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
        try {
          OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
              getChangesTree(atomicOperation, cacheEntry));
          while (bucket.size() == 0 || comparator.compare(bucket.getKey(0), key) >= 0) {
            final OHashTable.BucketPath prevBucketPath = prevBucketToFind(bucketPath, bucket.getDepth());
            if (prevBucketPath == null)
              return OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;

            releasePage(atomicOperation, cacheEntry);

            final long prevPointer = directory.getNodePointer(prevBucketPath.nodeIndex, prevBucketPath.itemIndex
                + prevBucketPath.hashMapOffset);

            pageIndex = getPageIndex(prevPointer);

            cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
            bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes, getChangesTree(
                atomicOperation, cacheEntry));

            bucketPath = prevBucketPath;
          }

          final int startIndex = 0;
          final int index = bucket.getIndex(hashCode, key);

          final int endIndex;
          if (index >= 0)
            endIndex = index;
          else
            endIndex = -index - 1;

          return convertBucketToEntries(bucket, startIndex, endIndex);
        } finally {
          releasePage(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException ioe) {
      throw new OIndexException("Exception during data read", ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OHashIndexBucket.Entry<K, V>[] floorEntries(K key) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        key = keySerializer.preprocess(key, (Object[]) keyTypes);

        final long hashCode = keyHashFunction.hashCode(key);
        OHashTable.BucketPath bucketPath = getBucket(hashCode);

        long bucketPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);

        long pageIndex = getPageIndex(bucketPointer);

        OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
        try {
          OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
              getChangesTree(atomicOperation, cacheEntry));
          while (bucket.size() == 0) {
            final OHashTable.BucketPath prevBucketPath = prevBucketToFind(bucketPath, bucket.getDepth());
            if (prevBucketPath == null)
              return OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;

            releasePage(atomicOperation, cacheEntry);

            final long prevPointer = directory.getNodePointer(prevBucketPath.nodeIndex, prevBucketPath.itemIndex
                + prevBucketPath.hashMapOffset);

            pageIndex = getPageIndex(prevPointer);

            cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);

            bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes, getChangesTree(
                atomicOperation, cacheEntry));

            bucketPath = prevBucketPath;
          }

          final int startIndex = 0;
          final int index = bucket.getIndex(hashCode, key);

          final int endIndex;
          if (index >= 0)
            endIndex = index + 1;
          else
            endIndex = -index - 1;

          return convertBucketToEntries(bucket, startIndex, endIndex);
        } finally {
          releasePage(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException ioe) {
      throw new OIndexException("Exception during data read", ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  private OHashTable.BucketPath prevBucketToFind(final OHashTable.BucketPath bucketPath, int bucketDepth) throws IOException {
    int offset = bucketPath.nodeGlobalDepth - bucketDepth;

    OHashTable.BucketPath currentBucket = bucketPath;
    int nodeLocalDepth = bucketPath.nodeLocalDepth;
    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentBucket = bucketPath.parent;
        nodeLocalDepth = currentBucket.nodeLocalDepth;
      }
    }

    final int diff = bucketDepth - (currentBucket.nodeGlobalDepth - nodeLocalDepth);
    final int firstStartIndex = currentBucket.itemIndex & ((LEVEL_MASK << (nodeLocalDepth - diff)) & LEVEL_MASK);
    final int globalIndex = firstStartIndex + currentBucket.hashMapOffset - 1;

    final OHashTable.BucketPath bucketPathToFind;
    if (globalIndex < 0)
      bucketPathToFind = prevLevelUp(bucketPath);
    else {
      final int hashMapSize = 1 << currentBucket.nodeLocalDepth;
      final int hashMapOffset = globalIndex / hashMapSize * hashMapSize;

      final int startIndex = globalIndex - hashMapOffset;

      bucketPathToFind = new OHashTable.BucketPath(currentBucket.parent, hashMapOffset, startIndex, currentBucket.nodeIndex,
          currentBucket.nodeLocalDepth, currentBucket.nodeGlobalDepth);
    }

    return prevNonEmptyNode(bucketPathToFind);
  }

  private OHashTable.BucketPath prevNonEmptyNode(OHashTable.BucketPath nodePath) throws IOException {
    prevBucketLoop: while (nodePath != null) {
      final long[] node = directory.getNode(nodePath.nodeIndex);
      final int startIndex = 0;
      final int endIndex = nodePath.itemIndex + nodePath.hashMapOffset;

      for (int i = endIndex; i >= startIndex; i--) {
        final long position = node[i];
        if (position > 0) {
          final int hashMapSize = 1 << nodePath.nodeLocalDepth;
          final int hashMapOffset = (i / hashMapSize) * hashMapSize;
          final int itemIndex = i - hashMapOffset;

          return new OHashTable.BucketPath(nodePath.parent, hashMapOffset, itemIndex, nodePath.nodeIndex, nodePath.nodeLocalDepth,
              nodePath.nodeGlobalDepth);
        }

        if (position < 0) {
          final int childNodeIndex = (int) ((position & Long.MAX_VALUE) >> 8);
          final int childItemOffset = (int) position & 0xFF;
          final int nodeLocalDepth = directory.getNodeLocalDepth(childNodeIndex);
          final int endChildIndex = (1 << nodeLocalDepth) - 1;

          final OHashTable.BucketPath parent = new OHashTable.BucketPath(nodePath.parent, 0, i, nodePath.nodeIndex,
              nodePath.nodeLocalDepth, nodePath.nodeGlobalDepth);
          nodePath = new OHashTable.BucketPath(parent, childItemOffset, endChildIndex, childNodeIndex, nodeLocalDepth,
              parent.nodeGlobalDepth + nodeLocalDepth);
          continue prevBucketLoop;
        }
      }

      nodePath = prevLevelUp(nodePath);
    }

    return null;
  }

  private OHashTable.BucketPath prevLevelUp(OHashTable.BucketPath bucketPath) {
    if (bucketPath.parent == null)
      return null;

    final int nodeLocalDepth = bucketPath.nodeLocalDepth;
    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - nodeLocalDepth);

    final OHashTable.BucketPath parent = bucketPath.parent;

    if (parent.itemIndex > MAX_LEVEL_SIZE / 2) {
      final int prevParentIndex = ((parent.itemIndex - MAX_LEVEL_SIZE / 2) / pointersSize) * pointersSize + MAX_LEVEL_SIZE / 2 - 1;
      return new OHashTable.BucketPath(parent.parent, 0, prevParentIndex, parent.nodeIndex, parent.nodeLocalDepth,
          parent.nodeGlobalDepth);
    }

    final int prevParentIndex = (parent.itemIndex / pointersSize) * pointersSize - 1;
    if (prevParentIndex >= 0)
      return new OHashTable.BucketPath(parent.parent, 0, prevParentIndex, parent.nodeIndex, parent.nodeLocalDepth,
          parent.nodeGlobalDepth);

    return prevLevelUp(new OHashTable.BucketPath(parent.parent, 0, 0, parent.nodeIndex, parent.nodeLocalDepth, -1));
  }

  @Override
  public long size() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final OCacheEntry hashStateEntry = loadPage(atomicOperation, fileStateId, hashStateEntryIndex, true);
        try {
          OHashIndexFileLevelMetadataPage metadataPage = new OHashIndexFileLevelMetadataPage(hashStateEntry, getChangesTree(
              atomicOperation, hashStateEntry), false);
          return metadataPage.getRecordsCount();
        } finally {
          releasePage(atomicOperation, hashStateEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException e) {
      throw new OIndexException("Error during index size request.", e);
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
    } catch (IOException e) {
      throw new OIndexException("Error during hash table close", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void delete() {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation();
    } catch (IOException e) {
      throw new OIndexException("Error during hash table deletion.", e);
    }

    acquireExclusiveLock();
    try {
      directory.delete();
      deleteFile(atomicOperation, fileStateId);
      deleteFile(atomicOperation, fileId);

      if (nullKeyIsSupported)
        deleteFile(atomicOperation, nullBucketFileId);

      endAtomicOperation(false, null);
    } catch (IOException e) {
      rollback(e);
      throw new OIndexException("Exception during index deletion", e);
    } catch (Exception e) {
      rollback(e);

      throw new OIndexException("Exception during index deletion", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void mergeNodeToParent(OHashTable.BucketPath nodePath) throws IOException {
    final int startIndex = findParentNodeStartIndex(nodePath);
    final int localNodeDepth = nodePath.nodeLocalDepth;
    final int hashMapSize = 1 << localNodeDepth;

    final int parentIndex = nodePath.parent.nodeIndex;
    for (int i = 0, k = startIndex; i < MAX_LEVEL_SIZE; i += hashMapSize, k++) {
      directory.setNodePointer(parentIndex, k, directory.getNodePointer(nodePath.nodeIndex, i));
    }

    directory.deleteNode(nodePath.nodeIndex);

    if (nodePath.parent.itemIndex < MAX_LEVEL_SIZE / 2) {
      final int maxChildDepth = directory.getMaxLeftChildDepth(parentIndex);
      if (maxChildDepth == localNodeDepth)
        directory.setMaxLeftChildDepth(parentIndex, (byte) getMaxLevelDepth(parentIndex, 0, MAX_LEVEL_SIZE / 2));
    } else {
      final int maxChildDepth = directory.getMaxRightChildDepth(parentIndex);
      if (maxChildDepth == localNodeDepth)
        directory.setMaxRightChildDepth(parentIndex, (byte) getMaxLevelDepth(parentIndex, MAX_LEVEL_SIZE / 2, MAX_LEVEL_SIZE));
    }
  }

  public void flush() {
    acquireExclusiveLock();
    try {
      writeCache.flush(fileStateId);
      writeCache.flush(fileId);

      directory.flush();

      if (nullKeyIsSupported)
        writeCache.flush(nullBucketFileId);
    } catch (IOException e) {
      throw new OIndexException("Error during hash table flush", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void doPut(K key, V value, OAtomicOperation atomicOperation) throws IOException {
    int sizeDiff = 0;

    if (key == null) {
      boolean isNew;
      OCacheEntry cacheEntry;
      if (getFilledUpTo(atomicOperation, nullBucketFileId) == 0) {
        cacheEntry = addPage(atomicOperation, nullBucketFileId);
        isNew = true;
      } else {
        cacheEntry = loadPage(atomicOperation, nullBucketFileId, 0, false);
        isNew = false;
      }

      cacheEntry.acquireExclusiveLock();
      try {
        ONullBucket<V> nullBucket = new ONullBucket<V>(cacheEntry, getChangesTree(atomicOperation, cacheEntry), valueSerializer,
            isNew);
        if (nullBucket.getValue() != null)
          sizeDiff--;

        nullBucket.setValue(value);
        sizeDiff++;
      } finally {
        cacheEntry.releaseExclusiveLock();
        releasePage(atomicOperation, cacheEntry);
      }

      changeSize(sizeDiff, atomicOperation);
    } else {
      final long hashCode = keyHashFunction.hashCode(key);

      final OHashTable.BucketPath bucketPath = getBucket(hashCode);
      final long bucketPointer = directory.getNodePointer(bucketPath.nodeIndex, bucketPath.itemIndex + bucketPath.hashMapOffset);
      if (bucketPointer == 0)
        throw new IllegalStateException("In this version of hash table buckets are added through split only.");

      final long pageIndex = getPageIndex(bucketPointer);

      final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
      cacheEntry.acquireExclusiveLock();
      try {
        final OHashIndexBucket<K, V> bucket = new OHashIndexBucket<K, V>(cacheEntry, keySerializer, valueSerializer, keyTypes,
            getChangesTree(atomicOperation, cacheEntry));
        final int index = bucket.getIndex(hashCode, key);

        if (index > -1) {
          final int updateResult = bucket.updateEntry(index, value);
          if (updateResult == 0) {
            changeSize(sizeDiff, atomicOperation);
            return;
          }

          if (updateResult == 1) {
            changeSize(sizeDiff, atomicOperation);
            return;
          }

          assert updateResult == -1;

          bucket.deleteEntry(index);
          sizeDiff--;
        }

        if (bucket.addEntry(hashCode, key, value)) {
          sizeDiff++;

          changeSize(sizeDiff, atomicOperation);
          return;
        }

        final OHashTable.BucketSplitResult splitResult = splitBucket(bucket, pageIndex, atomicOperation);

        final long updatedBucketPointer = splitResult.updatedBucketPointer;
        final long newBucketPointer = splitResult.newBucketPointer;
        final int bucketDepth = splitResult.newDepth;

        if (bucketDepth <= bucketPath.nodeGlobalDepth) {
          updateNodeAfterBucketSplit(bucketPath, bucketDepth, newBucketPointer, updatedBucketPointer);
        } else {
          if (bucketPath.nodeLocalDepth < MAX_LEVEL_DEPTH) {
            final OHashTable.NodeSplitResult nodeSplitResult = splitNode(bucketPath);

            assert !(nodeSplitResult.allLeftHashMapsEqual && nodeSplitResult.allRightHashMapsEqual);

            final long[] newNode = nodeSplitResult.newNode;

            final int nodeLocalDepth = bucketPath.nodeLocalDepth + 1;
            final int hashMapSize = 1 << nodeLocalDepth;

            assert nodeSplitResult.allRightHashMapsEqual == checkAllMapsContainSameBucket(newNode, hashMapSize);

            int newNodeIndex = -1;
            if (!nodeSplitResult.allRightHashMapsEqual || bucketPath.itemIndex >= MAX_LEVEL_SIZE / 2)
              newNodeIndex = directory.addNewNode((byte) 0, (byte) 0, (byte) nodeLocalDepth, newNode);

            final int updatedItemIndex = bucketPath.itemIndex << 1;
            final int updatedOffset = bucketPath.hashMapOffset << 1;
            final int updatedGlobalDepth = bucketPath.nodeGlobalDepth + 1;

            boolean allLeftHashMapsEqual = nodeSplitResult.allLeftHashMapsEqual;
            boolean allRightHashMapsEqual = nodeSplitResult.allRightHashMapsEqual;

            if (updatedOffset < MAX_LEVEL_SIZE) {
              allLeftHashMapsEqual = false;
              final OHashTable.BucketPath updatedBucketPath = new OHashTable.BucketPath(bucketPath.parent, updatedOffset,
                  updatedItemIndex, bucketPath.nodeIndex, nodeLocalDepth, updatedGlobalDepth);
              updateNodeAfterBucketSplit(updatedBucketPath, bucketDepth, newBucketPointer, updatedBucketPointer);
            } else {
              allRightHashMapsEqual = false;
              final OHashTable.BucketPath newBucketPath = new OHashTable.BucketPath(bucketPath.parent, updatedOffset
                  - MAX_LEVEL_SIZE, updatedItemIndex, newNodeIndex, nodeLocalDepth, updatedGlobalDepth);
              updateNodeAfterBucketSplit(newBucketPath, bucketDepth, newBucketPointer, updatedBucketPointer);
            }

            updateNodesAfterSplit(bucketPath, bucketPath.nodeIndex, newNode, nodeLocalDepth, hashMapSize, allLeftHashMapsEqual,
                allRightHashMapsEqual, newNodeIndex);

            if (allLeftHashMapsEqual)
              directory.deleteNode(bucketPath.nodeIndex);
          } else {
            addNewLevelNode(bucketPath, bucketPath.nodeIndex, newBucketPointer, updatedBucketPointer);
          }
        }
      } finally {
        cacheEntry.releaseExclusiveLock();
        releasePage(atomicOperation, cacheEntry);
      }

      changeSize(sizeDiff, atomicOperation);
      doPut(key, value, atomicOperation);
    }

  }

  private void checkNullSupport(K key) {
    if (key == null && !nullKeyIsSupported)
      throw new OIndexException("Null keys are not supported.");
  }

  private void updateNodesAfterSplit(OHashTable.BucketPath bucketPath, int nodeIndex, long[] newNode, int nodeLocalDepth,
      int hashMapSize, boolean allLeftHashMapEquals, boolean allRightHashMapsEquals, int newNodeIndex) throws IOException {

    final int startIndex = findParentNodeStartIndex(bucketPath);

    final int parentNodeIndex = bucketPath.parent.nodeIndex;
    assert assertParentNodeStartIndex(bucketPath, directory.getNode(parentNodeIndex), startIndex);

    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - nodeLocalDepth);
    if (allLeftHashMapEquals) {
      for (int i = 0; i < pointersSize; i++) {
        final long position = directory.getNodePointer(nodeIndex, i * hashMapSize);
        directory.setNodePointer(parentNodeIndex, startIndex + i, position);
      }
    } else {
      for (int i = 0; i < pointersSize; i++)
        directory.setNodePointer(parentNodeIndex, startIndex + i, (bucketPath.nodeIndex << 8) | (i * hashMapSize) | Long.MIN_VALUE);
    }

    if (allRightHashMapsEquals) {
      for (int i = 0; i < pointersSize; i++) {
        final long position = newNode[i * hashMapSize];
        directory.setNodePointer(parentNodeIndex, startIndex + pointersSize + i, position);
      }
    } else {
      for (int i = 0; i < pointersSize; i++)
        directory.setNodePointer(parentNodeIndex, startIndex + pointersSize + i, (newNodeIndex << 8) | (i * hashMapSize)
            | Long.MIN_VALUE);
    }

    updateMaxChildDepth(bucketPath.parent, bucketPath.nodeLocalDepth + 1);
  }

  private void updateMaxChildDepth(OHashTable.BucketPath parentPath, int childDepth) throws IOException {
    if (parentPath == null)
      return;

    if (parentPath.itemIndex < MAX_LEVEL_SIZE / 2) {
      final int maxChildDepth = directory.getMaxLeftChildDepth(parentPath.nodeIndex);
      if (childDepth > maxChildDepth)
        directory.setMaxLeftChildDepth(parentPath.nodeIndex, (byte) childDepth);
    } else {
      final int maxChildDepth = directory.getMaxRightChildDepth(parentPath.nodeIndex);
      if (childDepth > maxChildDepth)
        directory.setMaxRightChildDepth(parentPath.nodeIndex, (byte) childDepth);
    }
  }

  private boolean assertParentNodeStartIndex(OHashTable.BucketPath bucketPath, long[] parentNode, int calculatedIndex) {
    int startIndex = -1;
    for (int i = 0; i < parentNode.length; i++)
      if (parentNode[i] < 0 && (parentNode[i] & Long.MAX_VALUE) >>> 8 == bucketPath.nodeIndex) {
        startIndex = i;
        break;
      }

    return startIndex == calculatedIndex;
  }

  private int findParentNodeStartIndex(OHashTable.BucketPath bucketPath) {
    final OHashTable.BucketPath parentBucketPath = bucketPath.parent;
    final int pointersSize = 1 << (MAX_LEVEL_DEPTH - bucketPath.nodeLocalDepth);

    if (parentBucketPath.itemIndex < MAX_LEVEL_SIZE / 2)
      return (parentBucketPath.itemIndex / pointersSize) * pointersSize;

    return ((parentBucketPath.itemIndex - MAX_LEVEL_SIZE / 2) / pointersSize) * pointersSize + MAX_LEVEL_SIZE / 2;
  }

  private void addNewLevelNode(OHashTable.BucketPath bucketPath, int nodeIndex, long newBucketPointer, long updatedBucketPointer)
      throws IOException {
    final int newNodeDepth;
    final int newNodeStartIndex;
    final int mapInterval;

    if (bucketPath.itemIndex < MAX_LEVEL_SIZE / 2) {
      final int maxDepth = directory.getMaxLeftChildDepth(bucketPath.nodeIndex);

      assert getMaxLevelDepth(bucketPath.nodeIndex, 0, MAX_LEVEL_SIZE / 2) == maxDepth;

      if (maxDepth > 0)
        newNodeDepth = maxDepth;
      else
        newNodeDepth = 1;

      mapInterval = 1 << (MAX_LEVEL_DEPTH - newNodeDepth);
      newNodeStartIndex = (bucketPath.itemIndex / mapInterval) * mapInterval;
    } else {
      final int maxDepth = directory.getMaxRightChildDepth(bucketPath.nodeIndex);
      assert getMaxLevelDepth(bucketPath.nodeIndex, MAX_LEVEL_SIZE / 2, MAX_LEVEL_SIZE) == maxDepth;
      if (maxDepth > 0)
        newNodeDepth = maxDepth;
      else
        newNodeDepth = 1;

      mapInterval = 1 << (MAX_LEVEL_DEPTH - newNodeDepth);
      newNodeStartIndex = ((bucketPath.itemIndex - MAX_LEVEL_SIZE / 2) / mapInterval) * mapInterval + MAX_LEVEL_SIZE / 2;
    }

    final int newNodeIndex = directory.addNewNode((byte) 0, (byte) 0, (byte) newNodeDepth, new long[MAX_LEVEL_SIZE]);

    final int mapSize = 1 << newNodeDepth;
    for (int i = 0; i < mapInterval; i++) {
      final int nodeOffset = i + newNodeStartIndex;
      final long bucketPointer = directory.getNodePointer(nodeIndex, nodeOffset);

      if (nodeOffset != bucketPath.itemIndex) {
        for (int n = i << newNodeDepth; n < (i + 1) << newNodeDepth; n++)
          directory.setNodePointer(newNodeIndex, n, bucketPointer);
      } else {
        for (int n = i << newNodeDepth; n < (2 * i + 1) << (newNodeDepth - 1); n++)
          directory.setNodePointer(newNodeIndex, n, updatedBucketPointer);

        for (int n = (2 * i + 1) << (newNodeDepth - 1); n < (i + 1) << newNodeDepth; n++)
          directory.setNodePointer(newNodeIndex, n, newBucketPointer);
      }

      directory.setNodePointer(nodeIndex, nodeOffset, (newNodeIndex << 8) | (i * mapSize) | Long.MIN_VALUE);
    }

    updateMaxChildDepth(bucketPath, newNodeDepth);
  }

  private int getMaxLevelDepth(int nodeIndex, int start, int end) throws IOException {
    int currentIndex = -1;
    int maxDepth = 0;

    for (int i = start; i < end; i++) {
      final long nodePosition = directory.getNodePointer(nodeIndex, i);
      if (nodePosition >= 0)
        continue;

      final int index = (int) ((nodePosition & Long.MAX_VALUE) >>> 8);
      if (index == currentIndex)
        continue;

      currentIndex = index;

      final int nodeLocalDepth = directory.getNodeLocalDepth(index);
      if (maxDepth < nodeLocalDepth)
        maxDepth = nodeLocalDepth;
    }

    return maxDepth;
  }

  private void updateNodeAfterBucketSplit(OHashTable.BucketPath bucketPath, int bucketDepth, long newBucketPointer,
      long updatedBucketPointer) throws IOException {
    int offset = bucketPath.nodeGlobalDepth - (bucketDepth - 1);
    OHashTable.BucketPath currentNode = bucketPath;
    int nodeLocalDepth = bucketPath.nodeLocalDepth;
    while (offset > 0) {
      offset -= nodeLocalDepth;
      if (offset > 0) {
        currentNode = bucketPath.parent;
        nodeLocalDepth = currentNode.nodeLocalDepth;
      }
    }

    final int diff = bucketDepth - 1 - (currentNode.nodeGlobalDepth - nodeLocalDepth);

    final int interval = (1 << (nodeLocalDepth - diff - 1));
    final int firstStartIndex = currentNode.itemIndex & ((LEVEL_MASK << (nodeLocalDepth - diff)) & LEVEL_MASK);
    final int firstEndIndex = firstStartIndex + interval;

    final int secondStartIndex = firstEndIndex;
    final int secondEndIndex = secondStartIndex + interval;

    for (int i = firstStartIndex; i < firstEndIndex; i++)
      updateBucket(currentNode.nodeIndex, i, currentNode.hashMapOffset, updatedBucketPointer);

    for (int i = secondStartIndex; i < secondEndIndex; i++)
      updateBucket(currentNode.nodeIndex, i, currentNode.hashMapOffset, newBucketPointer);
  }

  private boolean checkAllMapsContainSameBucket(long[] newNode, int hashMapSize) {
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

  private boolean assertAllNodesAreFilePointers(boolean allHashMapsEquals, long[] newNode, int hashMapSize) {
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

  private OHashTable.NodeSplitResult splitNode(OHashTable.BucketPath bucketPath) throws IOException {
    final long[] newNode = new long[MAX_LEVEL_SIZE];
    final int hashMapSize = 1 << (bucketPath.nodeLocalDepth + 1);

    boolean hashMapItemsAreEqual = true;
    boolean allLeftItemsAreEqual;
    boolean allRightItemsAreEqual;

    int mapCounter = 0;
    long firstPosition = -1;

    long[] node = directory.getNode(bucketPath.nodeIndex);

    for (int i = MAX_LEVEL_SIZE / 2; i < MAX_LEVEL_SIZE; i++) {
      final long position = node[i];
      if (hashMapItemsAreEqual && mapCounter == 0)
        firstPosition = position;

      newNode[2 * (i - MAX_LEVEL_SIZE / 2)] = position;
      newNode[2 * (i - MAX_LEVEL_SIZE / 2) + 1] = position;

      if (hashMapItemsAreEqual) {
        hashMapItemsAreEqual = firstPosition == position;
        mapCounter += 2;

        if (mapCounter >= hashMapSize)
          mapCounter = 0;
      }
    }

    mapCounter = 0;
    allRightItemsAreEqual = hashMapItemsAreEqual;

    hashMapItemsAreEqual = true;
    final long[] updatedNode = new long[node.length];
    for (int i = 0; i < MAX_LEVEL_SIZE / 2; i++) {
      final long position = node[i];
      if (hashMapItemsAreEqual && mapCounter == 0)
        firstPosition = position;

      updatedNode[2 * i] = position;
      updatedNode[2 * i + 1] = position;

      if (hashMapItemsAreEqual) {
        hashMapItemsAreEqual = firstPosition == position;

        mapCounter += 2;

        if (mapCounter >= hashMapSize)
          mapCounter = 0;
      }
    }

    allLeftItemsAreEqual = hashMapItemsAreEqual;

    directory.setNode(bucketPath.nodeIndex, updatedNode);
    directory.setNodeLocalDepth(bucketPath.nodeIndex, (byte) (directory.getNodeLocalDepth(bucketPath.nodeIndex) + 1));

    return new OHashTable.NodeSplitResult(newNode, allLeftItemsAreEqual, allRightItemsAreEqual);
  }

  private void splitBucketContent(OHashIndexBucket<K, V> bucket, OHashIndexBucket<K, V> newBucket, int newBucketDepth)
      throws IOException {
    assert checkBucketDepth(bucket);

    List<OHashIndexBucket.Entry<K, V>> entries = new ArrayList<OHashIndexBucket.Entry<K, V>>(bucket.size());
    for (OHashIndexBucket.Entry<K, V> entry : bucket) {
      entries.add(entry);
    }

    bucket.init(newBucketDepth);

    for (OHashIndexBucket.Entry<K, V> entry : entries) {
      if (((keyHashFunction.hashCode(entry.key) >>> (HASH_CODE_SIZE - newBucketDepth)) & 1) == 0)
        bucket.appendEntry(entry.hashCode, entry.key, entry.value);
      else
        newBucket.appendEntry(entry.hashCode, entry.key, entry.value);
    }

    assert checkBucketDepth(bucket);
    assert checkBucketDepth(newBucket);
  }

  private OHashTable.BucketSplitResult splitBucket(OHashIndexBucket<K, V> bucket, long pageIndex, OAtomicOperation atomicOperation)
      throws IOException {
    int bucketDepth = bucket.getDepth();
    int newBucketDepth = bucketDepth + 1;

    final long updatedBucketIndex = pageIndex;
    final OCacheEntry newBucketCacheEntry = addPage(atomicOperation, fileId);

    newBucketCacheEntry.acquireExclusiveLock();
    try {
      final OHashIndexBucket<K, V> newBucket = new OHashIndexBucket<K, V>(newBucketDepth, newBucketCacheEntry, keySerializer,
          valueSerializer, keyTypes, getChangesTree(atomicOperation, newBucketCacheEntry));

      splitBucketContent(bucket, newBucket, newBucketDepth);

      final long updatedBucketPointer = createBucketPointer(updatedBucketIndex);
      final long newBucketPointer = createBucketPointer(newBucketCacheEntry.getPageIndex());

      return new OHashTable.BucketSplitResult(updatedBucketPointer, newBucketPointer, newBucketDepth);
    } finally {
      newBucketCacheEntry.releaseExclusiveLock();
      releasePage(atomicOperation, newBucketCacheEntry);
    }
  }

  private boolean checkBucketDepth(OHashIndexBucket<K, V> bucket) {
    int bucketDepth = bucket.getDepth();

    if (bucket.size() == 0)
      return true;

    final Iterator<OHashIndexBucket.Entry<K, V>> positionIterator = bucket.iterator();

    long firstValue = keyHashFunction.hashCode(positionIterator.next().key) >>> (HASH_CODE_SIZE - bucketDepth);
    while (positionIterator.hasNext()) {
      final long value = keyHashFunction.hashCode(positionIterator.next().key) >>> (HASH_CODE_SIZE - bucketDepth);
      if (value != firstValue)
        return false;
    }

    return true;
  }

  private void updateBucket(int nodeIndex, int itemIndex, int offset, long newBucketPointer) throws IOException {
    final long position = directory.getNodePointer(nodeIndex, itemIndex + offset);
    if (position >= 0)
      directory.setNodePointer(nodeIndex, itemIndex + offset, newBucketPointer);
    else {
      final int childNodeIndex = (int) ((position & Long.MAX_VALUE) >>> 8);
      final int childOffset = (int) (position & 0xFF);
      final int childNodeDepth = directory.getNodeLocalDepth(childNodeIndex);
      final int interval = 1 << childNodeDepth;
      for (int i = 0; i < interval; i++) {
        updateBucket(childNodeIndex, i, childOffset, newBucketPointer);
      }
    }
  }

  private void initHashTreeState(OAtomicOperation atomicOperation) throws IOException {
    truncateFile(atomicOperation, fileId);

    for (long pageIndex = 0; pageIndex < MAX_LEVEL_SIZE; pageIndex++) {
      final OCacheEntry cacheEntry = addPage(atomicOperation, fileId);
      assert cacheEntry.getPageIndex() == pageIndex;

      cacheEntry.acquireExclusiveLock();
      try {
        final OHashIndexBucket<K, V> emptyBucket = new OHashIndexBucket<K, V>(MAX_LEVEL_DEPTH, cacheEntry, keySerializer,
            valueSerializer, keyTypes, getChangesTree(atomicOperation, cacheEntry));
      } finally {
        cacheEntry.releaseExclusiveLock();
        releasePage(atomicOperation, cacheEntry);
      }
    }

    final long[] rootTree = new long[MAX_LEVEL_SIZE];
    for (int pageIndex = 0; pageIndex < MAX_LEVEL_SIZE; pageIndex++)
      rootTree[pageIndex] = createBucketPointer(pageIndex);

    directory.clear();
    directory.addNewNode((byte) 0, (byte) 0, (byte) MAX_LEVEL_DEPTH, rootTree);

    OCacheEntry hashStateEntry = loadPage(atomicOperation, fileStateId, hashStateEntryIndex, true);
    hashStateEntry.acquireExclusiveLock();
    try {
      OHashIndexFileLevelMetadataPage metadataPage = new OHashIndexFileLevelMetadataPage(hashStateEntry, getChangesTree(
          atomicOperation, hashStateEntry), false);
      metadataPage.setRecordsCount(0);
    } finally {
      hashStateEntry.releaseExclusiveLock();
      releasePage(atomicOperation, hashStateEntry);
    }
  }

  private long createBucketPointer(long pageIndex) {
    return pageIndex + 1;
  }

  private long getPageIndex(long bucketPointer) {
    return bucketPointer - 1;
  }

  private OHashTable.BucketPath getBucket(final long hashCode) throws IOException {
    int localNodeDepth = directory.getNodeLocalDepth(0);
    int nodeDepth = localNodeDepth;
    OHashTable.BucketPath parentNode = null;
    int nodeIndex = 0;
    int offset = 0;

    int index = (int) ((hashCode >>> (HASH_CODE_SIZE - nodeDepth)) & (LEVEL_MASK >>> (MAX_LEVEL_DEPTH - localNodeDepth)));
    OHashTable.BucketPath currentNode = new OHashTable.BucketPath(parentNode, 0, index, 0, localNodeDepth, nodeDepth);
    do {
      final long position = directory.getNodePointer(nodeIndex, index + offset);
      if (position >= 0)
        return currentNode;

      nodeIndex = (int) ((position & Long.MAX_VALUE) >>> 8);
      offset = (int) (position & 0xFF);

      localNodeDepth = directory.getNodeLocalDepth(nodeIndex);
      nodeDepth += localNodeDepth;

      index = (int) ((hashCode >>> (HASH_CODE_SIZE - nodeDepth)) & (LEVEL_MASK >>> (MAX_LEVEL_DEPTH - localNodeDepth)));

      parentNode = currentNode;
      currentNode = new OHashTable.BucketPath(parentNode, offset, index, nodeIndex, localNodeDepth, nodeDepth);
    } while (nodeDepth <= HASH_CODE_SIZE);

    throw new IllegalStateException("Extendible hashing tree in corrupted state.");
  }

}
