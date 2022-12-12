package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.EntryPoint;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.IntSerializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class BTree extends ODurableComponent {

  private static final int MAX_PATH_LENGTH =
      OGlobalConfiguration.SBTREE_MAX_DEPTH.getValueAsInteger();

  private static final int ENTRY_POINT_INDEX = 0;
  private static final int ROOT_INDEX = 1;

  private volatile long fileId;

  public BTree(
      final OAbstractPaginatedStorage storage, final String name, final String fileExtension) {
    super(storage, name, fileExtension, name + fileExtension);
  }

  public long getFileId() {
    return fileId;
  }

  public void create(final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        (operation) -> {
          acquireExclusiveLock();
          try {
            fileId = addFile(atomicOperation, getFullName());

            try (final OCacheEntry entryPointCacheEntry = addPage(atomicOperation, fileId)) {
              final EntryPoint entryPoint = new EntryPoint(entryPointCacheEntry);
              entryPoint.init();
            }

            try (final OCacheEntry rootCacheEntry = addPage(atomicOperation, fileId)) {
              final Bucket rootBucket = new Bucket(rootCacheEntry);
              rootBucket.init(true);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public void load() {
    acquireExclusiveLock();
    try {
      final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

      fileId = openFile(atomicOperation, getFullName());
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException("Exception during loading of rid bag " + getFullName()), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void delete(final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            deleteFile(atomicOperation, fileId);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public int get(final EdgeKey key) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
        if (bucketSearchResult.getItemIndex() < 0) {
          return -1;
        }

        final long pageIndex = bucketSearchResult.getPageIndex();

        try (final OCacheEntry keyBucketCacheEntry =
            loadPageForRead(atomicOperation, fileId, pageIndex)) {
          final Bucket keyBucket = new Bucket(keyBucketCacheEntry);
          return keyBucket.getValue(bucketSearchResult.getItemIndex());
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException(
              "Error during retrieving  of value for rid bag with name " + getName()),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public boolean put(final OAtomicOperation atomicOperation, final EdgeKey key, final int value) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          final boolean result;
          acquireExclusiveLock();
          try {
            final byte[] serializedKey =
                EdgeKeySerializer.INSTANCE.serializeNativeAsWhole(key, (Object[]) null);
            UpdateBucketSearchResult bucketSearchResult = findBucketForUpdate(key, atomicOperation);

            OCacheEntry keyBucketCacheEntry =
                loadPageForWrite(
                    atomicOperation, fileId, bucketSearchResult.getLastPathItem(), true);
            Bucket keyBucket = new Bucket(keyBucketCacheEntry);
            final byte[] oldRawValue;

            if (bucketSearchResult.getItemIndex() > -1) {
              oldRawValue = keyBucket.getRawValue(bucketSearchResult.getItemIndex());
              result = false;
            } else {
              oldRawValue = null;
              result = true;
            }

            final byte[] serializedValue =
                IntSerializer.INSTANCE.serializeNativeAsWhole(value, (Object[]) null);

            int insertionIndex;
            final int sizeDiff;
            if (bucketSearchResult.getItemIndex() >= 0) {
              assert oldRawValue != null;

              if (oldRawValue.length == serializedValue.length) {
                keyBucket.updateValue(
                    bucketSearchResult.getItemIndex(), serializedValue, serializedKey.length);
                keyBucketCacheEntry.close();
                return false;
              } else {
                keyBucket.removeLeafEntry(
                    bucketSearchResult.getItemIndex(), serializedKey.length, oldRawValue.length);
                insertionIndex = bucketSearchResult.getItemIndex();
                sizeDiff = 0;
              }
            } else {
              insertionIndex = -bucketSearchResult.getItemIndex() - 1;
              sizeDiff = 1;
            }

            while (!keyBucket.addLeafEntry(insertionIndex, serializedKey, serializedValue)) {
              bucketSearchResult =
                  splitBucket(
                      keyBucket,
                      keyBucketCacheEntry,
                      bucketSearchResult.getPath(),
                      bucketSearchResult.getInsertionIndexes(),
                      insertionIndex,
                      atomicOperation);

              insertionIndex = bucketSearchResult.getItemIndex();

              final long pageIndex = bucketSearchResult.getLastPathItem();

              if (pageIndex != keyBucketCacheEntry.getPageIndex()) {
                keyBucketCacheEntry.close();

                keyBucketCacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true);
              }

              //noinspection ObjectAllocationInLoop
              keyBucket = new Bucket(keyBucketCacheEntry);
            }

            keyBucketCacheEntry.close();

            if (sizeDiff != 0) {
              updateSize(sizeDiff, atomicOperation);
            }
          } finally {
            releaseExclusiveLock();
          }

          return result;
        });
  }

  public EdgeKey firstKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final Optional<BucketSearchResult> searchResult = firstItem(atomicOperation);
        if (!searchResult.isPresent()) {
          return null;
        }

        final BucketSearchResult result = searchResult.get();

        try (final OCacheEntry cacheEntry =
            loadPageForRead(atomicOperation, fileId, result.getPageIndex())) {
          final Bucket bucket = new Bucket(cacheEntry);
          return bucket.getKey(result.getItemIndex());
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during finding first key in btree [" + getName() + "]"), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  private Optional<BucketSearchResult> firstItem(final OAtomicOperation atomicOperation)
      throws IOException {
    final LinkedList<PagePathItemUnit> path = new LinkedList<>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex);
    int itemIndex = 0;
    try {
      Bucket bucket = new Bucket(cacheEntry);

      while (true) {
        if (!bucket.isLeaf()) {
          if (bucket.isEmpty() || itemIndex > bucket.size()) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.getPageIndex();
              itemIndex = pagePathItemUnit.getItemIndex() + 1;
            } else {
              return Optional.empty();
            }
          } else {
            //noinspection ObjectAllocationInLoop
            path.add(new PagePathItemUnit(bucketIndex, itemIndex));

            if (itemIndex < bucket.size()) {
              bucketIndex = bucket.getLeft(itemIndex);
            } else {
              bucketIndex = bucket.getRight(itemIndex - 1);
            }

            itemIndex = 0;
          }
        } else {
          if (bucket.isEmpty()) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.getPageIndex();
              itemIndex = pagePathItemUnit.getItemIndex() + 1;
            } else {
              return Optional.empty();
            }
          } else {
            return Optional.of(new BucketSearchResult(0, bucketIndex));
          }
        }

        cacheEntry.close();

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex);
        //noinspection ObjectAllocationInLoop
        bucket = new Bucket(cacheEntry);
      }
    } finally {
      cacheEntry.close();
    }
  }

  public EdgeKey lastKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final Optional<BucketSearchResult> searchResult = lastItem(atomicOperation);
        if (!searchResult.isPresent()) {
          return null;
        }

        final BucketSearchResult result = searchResult.get();

        try (final OCacheEntry cacheEntry =
            loadPageForRead(atomicOperation, fileId, result.getPageIndex())) {
          final Bucket bucket = new Bucket(cacheEntry);
          return bucket.getKey(result.getItemIndex());
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during finding last key in btree [" + getName() + "]"), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  private Optional<BucketSearchResult> lastItem(final OAtomicOperation atomicOperation)
      throws IOException {
    final LinkedList<PagePathItemUnit> path = new LinkedList<>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex);

    Bucket bucket = new Bucket(cacheEntry);

    int itemIndex = bucket.size() - 1;
    try {
      while (true) {
        if (!bucket.isLeaf()) {
          if (itemIndex < -1) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.getPageIndex();
              itemIndex = pagePathItemUnit.getItemIndex() - 1;
            } else {
              return Optional.empty();
            }
          } else {
            //noinspection ObjectAllocationInLoop
            path.add(new PagePathItemUnit(bucketIndex, itemIndex));

            if (itemIndex > -1) {
              bucketIndex = bucket.getRight(itemIndex);
            } else {
              bucketIndex = bucket.getLeft(0);
            }

            itemIndex = Bucket.MAX_PAGE_SIZE_BYTES + 1;
          }
        } else {
          if (bucket.isEmpty()) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.getPageIndex();
              itemIndex = pagePathItemUnit.getItemIndex() - 1;
            } else {
              return Optional.empty();
            }
          } else {
            return Optional.of(new BucketSearchResult(bucket.size() - 1, bucketIndex));
          }
        }

        cacheEntry.close();

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex);

        //noinspection ObjectAllocationInLoop
        bucket = new Bucket(cacheEntry);
        if (itemIndex == Bucket.MAX_PAGE_SIZE_BYTES + 1) {
          itemIndex = bucket.size() - 1;
        }
      }
    } finally {
      cacheEntry.close();
    }
  }

  private UpdateBucketSearchResult splitBucket(
      final Bucket bucketToSplit,
      final OCacheEntry entryToSplit,
      final List<Integer> path,
      final List<Integer> itemPointers,
      final int keyIndex,
      final OAtomicOperation atomicOperation)
      throws IOException {
    final boolean splitLeaf = bucketToSplit.isLeaf();
    final int bucketSize = bucketToSplit.size();

    final int indexToSplit = bucketSize >>> 1;
    final EdgeKey separationKey = bucketToSplit.getKey(indexToSplit);
    final List<byte[]> rightEntries = new ArrayList<>(indexToSplit);

    final int startRightIndex = splitLeaf ? indexToSplit : indexToSplit + 1;

    for (int i = startRightIndex; i < bucketSize; i++) {
      rightEntries.add(bucketToSplit.getRawEntry(i));
    }

    if (entryToSplit.getPageIndex() != ROOT_INDEX) {
      return splitNonRootBucket(
          path,
          itemPointers,
          keyIndex,
          entryToSplit.getPageIndex(),
          bucketToSplit,
          splitLeaf,
          indexToSplit,
          separationKey,
          rightEntries,
          atomicOperation);
    } else {
      return splitRootBucket(
          keyIndex,
          entryToSplit,
          bucketToSplit,
          splitLeaf,
          indexToSplit,
          separationKey,
          rightEntries,
          atomicOperation);
    }
  }

  private UpdateBucketSearchResult splitNonRootBucket(
      final List<Integer> path,
      final List<Integer> itemPointers,
      final int keyIndex,
      final int pageIndex,
      final Bucket bucketToSplit,
      final boolean splitLeaf,
      final int indexToSplit,
      final EdgeKey separationKey,
      final List<byte[]> rightEntries,
      final OAtomicOperation atomicOperation)
      throws IOException {

    final OCacheEntry rightBucketEntry;
    try (final OCacheEntry entryPointCacheEntry =
        loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, true)) {
      final EntryPoint entryPoint = new EntryPoint(entryPointCacheEntry);
      int pageSize = entryPoint.getPagesSize();

      if (pageSize < getFilledUpTo(atomicOperation, fileId) - 1) {
        pageSize++;
        rightBucketEntry = loadPageForWrite(atomicOperation, fileId, pageSize, false);
        entryPoint.setPagesSize(pageSize);
      } else {
        assert pageSize == getFilledUpTo(atomicOperation, fileId) - 1;

        rightBucketEntry = addPage(atomicOperation, fileId);
        entryPoint.setPagesSize(rightBucketEntry.getPageIndex());
      }
    }

    try {
      final Bucket newRightBucket = new Bucket(rightBucketEntry);
      newRightBucket.init(splitLeaf);
      newRightBucket.addAll(rightEntries);

      bucketToSplit.shrink(indexToSplit);

      if (splitLeaf) {
        final long rightSiblingPageIndex = bucketToSplit.getRightSibling();

        newRightBucket.setRightSibling(rightSiblingPageIndex);
        newRightBucket.setLeftSibling(pageIndex);

        bucketToSplit.setRightSibling(rightBucketEntry.getPageIndex());

        if (rightSiblingPageIndex >= 0) {

          try (final OCacheEntry rightSiblingBucketEntry =
              loadPageForWrite(atomicOperation, fileId, rightSiblingPageIndex, true)) {
            final Bucket rightSiblingBucket = new Bucket(rightSiblingBucketEntry);
            rightSiblingBucket.setLeftSibling(rightBucketEntry.getPageIndex());
          }
        }
      }

      long parentIndex = path.get(path.size() - 2);
      OCacheEntry parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentIndex, true);
      try {
        Bucket parentBucket = new Bucket(parentCacheEntry);
        int insertionIndex = itemPointers.get(itemPointers.size() - 2);
        while (!parentBucket.addNonLeafEntry(
            insertionIndex,
            pageIndex,
            rightBucketEntry.getPageIndex(),
            EdgeKeySerializer.INSTANCE.serializeNativeAsWhole(separationKey, (Object[]) null),
            true)) {
          final UpdateBucketSearchResult bucketSearchResult =
              splitBucket(
                  parentBucket,
                  parentCacheEntry,
                  path.subList(0, path.size() - 1),
                  itemPointers.subList(0, itemPointers.size() - 1),
                  insertionIndex,
                  atomicOperation);

          parentIndex = bucketSearchResult.getLastPathItem();
          insertionIndex = bucketSearchResult.getItemIndex();

          if (parentIndex != parentCacheEntry.getPageIndex()) {
            parentCacheEntry.close();

            parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentIndex, true);
          }

          //noinspection ObjectAllocationInLoop
          parentBucket = new Bucket(parentCacheEntry);
        }

      } finally {
        parentCacheEntry.close();
      }

    } finally {
      rightBucketEntry.close();
    }

    final ArrayList<Integer> resultPath = new ArrayList<>(path.subList(0, path.size() - 1));
    final ArrayList<Integer> resultItemPointers =
        new ArrayList<>(itemPointers.subList(0, itemPointers.size() - 1));

    if (keyIndex <= indexToSplit) {
      resultPath.add(pageIndex);
      resultItemPointers.add(keyIndex);

      return new UpdateBucketSearchResult(resultItemPointers, resultPath, keyIndex);
    }

    final int parentIndex = resultItemPointers.size() - 1;
    resultItemPointers.set(parentIndex, resultItemPointers.get(parentIndex) + 1);
    resultPath.add(rightBucketEntry.getPageIndex());

    if (splitLeaf) {
      resultItemPointers.add(keyIndex - indexToSplit);
      return new UpdateBucketSearchResult(resultItemPointers, resultPath, keyIndex - indexToSplit);
    }

    resultItemPointers.add(keyIndex - indexToSplit - 1);
    return new UpdateBucketSearchResult(
        resultItemPointers, resultPath, keyIndex - indexToSplit - 1);
  }

  private UpdateBucketSearchResult splitRootBucket(
      final int keyIndex,
      final OCacheEntry bucketEntry,
      Bucket bucketToSplit,
      final boolean splitLeaf,
      final int indexToSplit,
      final EdgeKey separationKey,
      final List<byte[]> rightEntries,
      final OAtomicOperation atomicOperation)
      throws IOException {
    final List<byte[]> leftEntries = new ArrayList<>(indexToSplit);

    for (int i = 0; i < indexToSplit; i++) {
      leftEntries.add(bucketToSplit.getRawEntry(i));
    }

    final OCacheEntry leftBucketEntry;
    final OCacheEntry rightBucketEntry;

    try (final OCacheEntry entryPointCacheEntry =
        loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, true)) {
      final EntryPoint entryPoint = new EntryPoint(entryPointCacheEntry);
      int pageSize = entryPoint.getPagesSize();

      final int filledUpTo = (int) getFilledUpTo(atomicOperation, fileId);

      if (pageSize < filledUpTo - 1) {
        pageSize++;
        leftBucketEntry = loadPageForWrite(atomicOperation, fileId, pageSize, false);
      } else {
        assert pageSize == filledUpTo - 1;
        leftBucketEntry = addPage(atomicOperation, fileId);
        pageSize = leftBucketEntry.getPageIndex();
      }

      if (pageSize < filledUpTo) {
        pageSize++;
        rightBucketEntry = loadPageForWrite(atomicOperation, fileId, pageSize, false);
      } else {
        assert pageSize == filledUpTo;
        rightBucketEntry = addPage(atomicOperation, fileId);
        pageSize = rightBucketEntry.getPageIndex();
      }

      entryPoint.setPagesSize(pageSize);
    }

    try {
      final Bucket newLeftBucket = new Bucket(leftBucketEntry);
      newLeftBucket.init(splitLeaf);
      newLeftBucket.addAll(leftEntries);

      if (splitLeaf) {
        newLeftBucket.setRightSibling(rightBucketEntry.getPageIndex());
      }

    } finally {
      leftBucketEntry.close();
    }

    try {
      final Bucket newRightBucket = new Bucket(rightBucketEntry);
      newRightBucket.init(splitLeaf);
      newRightBucket.addAll(rightEntries);

      if (splitLeaf) {
        newRightBucket.setLeftSibling(leftBucketEntry.getPageIndex());
      }
    } finally {
      rightBucketEntry.close();
    }

    bucketToSplit = new Bucket(bucketEntry);
    bucketToSplit.shrink(0);
    if (splitLeaf) {
      bucketToSplit.switchBucketType();
    }

    bucketToSplit.addNonLeafEntry(
        0,
        leftBucketEntry.getPageIndex(),
        rightBucketEntry.getPageIndex(),
        EdgeKeySerializer.INSTANCE.serializeNativeAsWhole(separationKey, (Object[]) null),
        true);

    final ArrayList<Integer> resultPath = new ArrayList<>(8);
    resultPath.add(ROOT_INDEX);

    final ArrayList<Integer> itemPointers = new ArrayList<>(8);

    if (keyIndex <= indexToSplit) {
      itemPointers.add(-1);
      itemPointers.add(keyIndex);

      resultPath.add(leftBucketEntry.getPageIndex());
      return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex);
    }

    resultPath.add(rightBucketEntry.getPageIndex());
    itemPointers.add(0);

    if (splitLeaf) {
      itemPointers.add(keyIndex - indexToSplit);
      return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex - indexToSplit);
    }

    itemPointers.add(keyIndex - indexToSplit - 1);
    return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex - indexToSplit - 1);
  }

  private void updateSize(final long diffSize, final OAtomicOperation atomicOperation)
      throws IOException {
    try (final OCacheEntry entryPointCacheEntry =
        loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, true)) {
      final EntryPoint entryPoint = new EntryPoint(entryPointCacheEntry);
      entryPoint.setTreeSize(entryPoint.getTreeSize() + diffSize);
    }
  }

  private UpdateBucketSearchResult findBucketForUpdate(
      final EdgeKey key, final OAtomicOperation atomicOperation) throws IOException {
    int pageIndex = ROOT_INDEX;

    final ArrayList<Integer> path = new ArrayList<>(8);
    final ArrayList<Integer> itemIndexes = new ArrayList<>(8);

    while (true) {
      if (path.size() > MAX_PATH_LENGTH) {
        throw new OStorageException(
            "We reached max level of depth of SBTree but still found nothing, seems like tree is in corrupted state. You should rebuild index related to given query.");
      }

      path.add(pageIndex);

      try (final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
        final Bucket keyBucket = new Bucket(bucketEntry);
        final int index = keyBucket.find(key);

        if (keyBucket.isLeaf()) {
          itemIndexes.add(index);
          return new UpdateBucketSearchResult(itemIndexes, path, index);
        }

        if (index >= 0) {
          pageIndex = keyBucket.getRight(index);
          itemIndexes.add(index + 1);
        } else {
          final int insertionIndex = -index - 1;

          if (insertionIndex >= keyBucket.size()) {
            pageIndex = keyBucket.getRight(insertionIndex - 1);
          } else {
            pageIndex = keyBucket.getLeft(insertionIndex);
          }

          itemIndexes.add(insertionIndex);
        }
      }
    }
  }

  private BucketSearchResult findBucket(final EdgeKey key, final OAtomicOperation atomicOperation)
      throws IOException {
    long pageIndex = ROOT_INDEX;

    int depth = 0;
    while (true) {
      depth++;
      if (depth > MAX_PATH_LENGTH) {
        throw new OStorageException(
            "We reached max level of depth of SBTree but still found nothing, seems like tree is in corrupted state. You should rebuild index related to given query.");
      }

      try (final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
        final Bucket keyBucket = new Bucket(bucketEntry);
        final int index = keyBucket.find(key);

        if (keyBucket.isLeaf()) {
          return new BucketSearchResult(index, pageIndex);
        }

        if (index >= 0) {
          pageIndex = keyBucket.getRight(index);
        } else {
          final int insertionIndex = -index - 1;
          if (insertionIndex >= keyBucket.size()) {
            pageIndex = keyBucket.getRight(insertionIndex - 1);
          } else {
            pageIndex = keyBucket.getLeft(insertionIndex);
          }
        }
      }
    }
  }

  public int remove(final OAtomicOperation atomicOperation, final EdgeKey key) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final int removedValue;
            final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);

            if (bucketSearchResult.getItemIndex() < 0) {
              return -1;
            }

            final byte[] serializedKey = EdgeKeySerializer.INSTANCE.serializeNativeAsWhole(key);
            final byte[] rawValue;
            try (final OCacheEntry keyBucketCacheEntry =
                loadPageForWrite(
                    atomicOperation, fileId, bucketSearchResult.getPageIndex(), true)) {
              final Bucket keyBucket = new Bucket(keyBucketCacheEntry);
              rawValue = keyBucket.getRawValue(bucketSearchResult.getItemIndex());
              keyBucket.removeLeafEntry(
                  bucketSearchResult.getItemIndex(), serializedKey.length, rawValue.length);
              updateSize(-1, atomicOperation);
            }

            removedValue = IntSerializer.INSTANCE.deserializeNativeObject(rawValue, 0);
            return removedValue;
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public Stream<ORawPair<EdgeKey, Integer>> iterateEntriesMinor(
      final EdgeKey key, final boolean inclusive, final boolean ascSortOrder) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (!ascSortOrder) {
          return StreamSupport.stream(iterateEntriesMinorDesc(key, inclusive), false);
        }

        return StreamSupport.stream(iterateEntriesMinorAsc(key, inclusive), false);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public Stream<ORawPair<EdgeKey, Integer>> iterateEntriesMajor(
      final EdgeKey key, final boolean inclusive, final boolean ascSortOrder) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (ascSortOrder) {
          return StreamSupport.stream(iterateEntriesMajorAsc(key, inclusive), false);
        }
        return StreamSupport.stream(iterateEntriesMajorDesc(key, inclusive), false);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public Stream<ORawPair<EdgeKey, Integer>> iterateEntriesBetween(
      final EdgeKey keyFrom,
      final boolean fromInclusive,
      final EdgeKey keyTo,
      final boolean toInclusive,
      final boolean ascSortOrder) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (ascSortOrder) {
          return StreamSupport.stream(
              iterateEntriesBetweenAscOrder(keyFrom, fromInclusive, keyTo, toInclusive), false);
        } else {
          return StreamSupport.stream(
              iterateEntriesBetweenDescOrder(keyFrom, fromInclusive, keyTo, toInclusive), false);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  private Spliterator<ORawPair<EdgeKey, Integer>> iterateEntriesMinorDesc(
      EdgeKey key, final boolean inclusive) {
    return new SpliteratorBackward(this, null, key, false, inclusive);
  }

  private Spliterator<ORawPair<EdgeKey, Integer>> iterateEntriesMinorAsc(
      EdgeKey key, final boolean inclusive) {
    return new SpliteratorForward(this, null, key, false, inclusive);
  }

  private Spliterator<ORawPair<EdgeKey, Integer>> iterateEntriesMajorAsc(
      EdgeKey key, final boolean inclusive) {
    return new SpliteratorForward(this, key, null, inclusive, false);
  }

  private Spliterator<ORawPair<EdgeKey, Integer>> iterateEntriesMajorDesc(
      EdgeKey key, final boolean inclusive) {
    return new SpliteratorBackward(this, key, null, inclusive, false);
  }

  private Spliterator<ORawPair<EdgeKey, Integer>> iterateEntriesBetweenAscOrder(
      EdgeKey keyFrom, final boolean fromInclusive, EdgeKey keyTo, final boolean toInclusive) {
    return new SpliteratorForward(this, keyFrom, keyTo, fromInclusive, toInclusive);
  }

  private Spliterator<ORawPair<EdgeKey, Integer>> iterateEntriesBetweenDescOrder(
      EdgeKey keyFrom, final boolean fromInclusive, EdgeKey keyTo, final boolean toInclusive) {
    return new SpliteratorBackward(this, keyFrom, keyTo, fromInclusive, toInclusive);
  }

  public void fetchNextCachePortionForward(SpliteratorForward iter) {
    final EdgeKey lastKey;
    if (!iter.getDataCache().isEmpty()) {
      lastKey = iter.getDataCache().get(iter.getDataCache().size() - 1).first;
    } else {
      lastKey = null;
    }

    iter.clearCache();

    atomicOperationsManager.acquireReadLock(BTree.this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        if (iter.getPageIndex() > -1) {
          if (readKeysFromBucketsForward(iter, atomicOperation)) {
            return;
          }
        }

        // this can only happen if page LSN does not equal to stored LSN or index of current
        // iterated page equals to -1
        // so we only started iteration
        if (iter.getDataCache().isEmpty()) {
          // iteration just started
          if (lastKey == null) {
            if (iter.getFromKey() != null) {
              final BucketSearchResult searchResult =
                  findBucket(iter.getFromKey(), atomicOperation);
              iter.setPageIndex((int) searchResult.getPageIndex());

              if (searchResult.getItemIndex() >= 0) {
                if (iter.isFromKeyInclusive()) {
                  iter.setItemIndex(searchResult.getItemIndex());
                } else {
                  iter.setItemIndex(searchResult.getItemIndex() + 1);
                }
              } else {
                iter.setItemIndex(-searchResult.getItemIndex() - 1);
              }
            } else {
              final Optional<BucketSearchResult> bucketSearchResult = firstItem(atomicOperation);
              if (bucketSearchResult.isPresent()) {
                final BucketSearchResult searchResult = bucketSearchResult.get();
                iter.setPageIndex((int) searchResult.getPageIndex());
                iter.setItemIndex(searchResult.getItemIndex());
              } else {
                return;
              }
            }

          } else {
            final BucketSearchResult bucketSearchResult = findBucket(lastKey, atomicOperation);

            iter.setPageIndex((int) bucketSearchResult.getPageIndex());
            if (bucketSearchResult.getItemIndex() >= 0) {
              iter.setItemIndex(bucketSearchResult.getItemIndex() + 1);
            } else {
              iter.setItemIndex(-bucketSearchResult.getItemIndex() - 1);
            }
          }
          iter.setLastLSN(null);
          readKeysFromBucketsForward(iter, atomicOperation);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Error during element iteration"), e);
    } finally {
      atomicOperationsManager.releaseReadLock(BTree.this);
    }
  }

  private boolean readKeysFromBucketsForward(
      SpliteratorForward iter, OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, iter.getPageIndex());
    try {
      Bucket bucket = new Bucket(cacheEntry);
      if (iter.getLastLSN() == null
          || bucket.getLSN().equals(iter.getLastLSN()) && atomicOperation == null) {
        while (true) {
          int bucketSize = bucket.size();
          if (iter.getItemIndex() >= bucketSize) {
            iter.setPageIndex((int) bucket.getRightSibling());

            if (iter.getPageIndex() < 0) {
              return true;
            }

            iter.setItemIndex(0);
            cacheEntry.close();

            cacheEntry = loadPageForRead(atomicOperation, fileId, iter.getPageIndex());
            bucket = new Bucket(cacheEntry);

            bucketSize = bucket.size();
          }

          iter.setLastLSN(bucket.getLSN());

          for (;
              iter.getItemIndex() < bucketSize && iter.getDataCache().size() < 10;
              iter.incrementItemIndex()) {
            @SuppressWarnings("ObjectAllocationInLoop")
            TreeEntry entry = bucket.getEntry(iter.getItemIndex());

            if (iter.getToKey() != null) {
              if (iter.isToKeyInclusive()) {
                if (entry.getKey().compareTo(iter.getToKey()) > 0) {
                  return true;
                }
              } else if (entry.getKey().compareTo(iter.getToKey()) >= 0) {
                return true;
              }
            }

            //noinspection ObjectAllocationInLoop
            iter.getDataCache().add(new ORawPair<>(entry.getKey(), entry.getValue()));
          }

          if (iter.getDataCache().size() >= 10) {
            return true;
          }
        }
      }
    } finally {
      cacheEntry.close();
    }

    return false;
  }

  public void fetchNextCachePortionBackward(SpliteratorBackward iter) {
    final EdgeKey lastKey;
    if (iter.getDataCache().isEmpty()) {
      lastKey = null;
    } else {
      lastKey = iter.getDataCache().get(iter.getDataCache().size() - 1).first;
    }

    iter.clearCache();

    atomicOperationsManager.acquireReadLock(BTree.this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        if (iter.getPageIndex() > -1) {
          if (readKeysFromBucketsBackward(iter, atomicOperation)) {
            return;
          }
        }

        // this can only happen if page LSN does not equal to stored LSN or index of current
        // iterated page equals to -1
        // so we only started iteration
        if (iter.getDataCache().isEmpty()) {
          // iteration just started
          if (lastKey == null) {
            if (iter.getToKey() != null) {
              final BucketSearchResult searchResult = findBucket(iter.getToKey(), atomicOperation);
              iter.setPageIndex((int) searchResult.getPageIndex());

              if (searchResult.getItemIndex() >= 0) {
                if (iter.isToKeyInclusive()) {
                  iter.setItemIndex(searchResult.getItemIndex());
                } else {
                  iter.setItemIndex(searchResult.getItemIndex() - 1);
                }
              } else {
                iter.setItemIndex(-searchResult.getItemIndex() - 2);
              }
            } else {
              final Optional<BucketSearchResult> bucketSearchResult = lastItem(atomicOperation);
              if (bucketSearchResult.isPresent()) {
                final BucketSearchResult searchResult = bucketSearchResult.get();
                iter.setPageIndex((int) searchResult.getPageIndex());
                iter.setItemIndex(searchResult.getItemIndex());
              } else {
                return;
              }
            }

          } else {
            final BucketSearchResult bucketSearchResult = findBucket(lastKey, atomicOperation);

            iter.setPageIndex((int) bucketSearchResult.getPageIndex());
            if (bucketSearchResult.getItemIndex() >= 0) {
              iter.setItemIndex(bucketSearchResult.getItemIndex() - 1);
            } else {
              iter.setPageIndex(-bucketSearchResult.getItemIndex() - 2);
            }
          }
          iter.setLastLSN(null);
          readKeysFromBucketsBackward(iter, atomicOperation);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(new OStorageException("Error during element iteration"), e);
    } finally {
      atomicOperationsManager.releaseReadLock(BTree.this);
    }
  }

  private boolean readKeysFromBucketsBackward(
      SpliteratorBackward iter, OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, iter.getPageIndex());
    try {
      Bucket bucket = new Bucket(cacheEntry);
      if (iter.getLastLSN() == null
          || bucket.getLSN().equals(iter.getLastLSN()) && atomicOperation == null) {
        while (true) {
          if (iter.getItemIndex() < 0) {
            iter.setPageIndex((int) bucket.getLeftSibling());

            if (iter.getPageIndex() < 0) {
              return true;
            }

            cacheEntry.close();

            cacheEntry = loadPageForRead(atomicOperation, fileId, iter.getPageIndex());
            bucket = new Bucket(cacheEntry);
            final int bucketSize = bucket.size();
            iter.setItemIndex(bucketSize - 1);
          }

          iter.setLastLSN(bucket.getLSN());

          for (; iter.getItemIndex() >= 0 && iter.getDataCache().size() < 10; iter.decItemIndex()) {
            @SuppressWarnings("ObjectAllocationInLoop")
            TreeEntry entry = bucket.getEntry(iter.getItemIndex());

            if (iter.getFromKey() != null) {
              if (iter.isFromKeyInclusive()) {
                if (entry.getKey().compareTo(iter.getFromKey()) < 0) {
                  return true;
                }
              } else if (entry.getKey().compareTo(iter.getFromKey()) <= 0) {
                return true;
              }
            }

            //noinspection ObjectAllocationInLoop
            iter.getDataCache().add(new ORawPair<>(entry.getKey(), entry.getValue()));
          }

          if (iter.getDataCache().size() >= 10) {
            return true;
          }
        }
      }
    } finally {
      cacheEntry.close();
    }

    return false;
  }
}
