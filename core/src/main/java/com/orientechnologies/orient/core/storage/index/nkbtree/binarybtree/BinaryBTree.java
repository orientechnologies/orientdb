package com.orientechnologies.orient.core.storage.index.nkbtree.binarybtree;

import com.carrotsearch.hppc.ByteArrayList;
import com.orientechnologies.common.comparator.OComparatorFactory;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.exception.NotEmptyComponentCanNotBeRemovedException;
import com.orientechnologies.orient.core.exception.OTooBigIndexKeyException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class BinaryBTree extends ODurableComponent {
  private static final int ENTRY_POINT_INDEX = 0;
  private static final long ROOT_INDEX = 1;

  public static final byte ALWAYS_LESS_PREFIX = 0;
  public static final byte DATA_PREFIX = 1;
  public static final byte NULL_PREFIX = 2;
  public static final byte ALWAYS_GREATER_PREFIX = 3;

  private static final byte[] ROOT_LARGEST_LOWER_BOUND = new byte[] {DATA_PREFIX};
  private static final byte[] ROOT_SMALLEST_UPPER_BOUND = new byte[] {ALWAYS_GREATER_PREFIX};

  private static final Comparator<byte[]> COMPARATOR =
      OComparatorFactory.INSTANCE.getComparator(byte[].class);

  private final int spliteratorCacheSize;
  private final int maxKeySize;
  private final int maxPathLength;

  private long fileId;

  public BinaryBTree(
      final int spliteratorCacheSize,
      final int maxKeySize,
      final int maxPathLength,
      final OAbstractPaginatedStorage storage,
      final String name,
      final String extension) {
    super(storage, name, extension, name + extension);

    this.spliteratorCacheSize = spliteratorCacheSize;
    this.maxKeySize = maxKeySize;
    this.maxPathLength = maxPathLength;
  }

  public void create(final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            fileId = addFile(atomicOperation, getFullName());

            final OCacheEntry entryPointCacheEntry = addPage(atomicOperation, fileId);
            try {
              final EntryPoint entryPoint = new EntryPoint(entryPointCacheEntry);
              entryPoint.init();
            } finally {
              releasePageFromWrite(atomicOperation, entryPointCacheEntry);
            }

            final OCacheEntry rootCacheEntry = addPage(atomicOperation, fileId);
            try {
              final Bucket rootBucket = new Bucket(rootCacheEntry);
              rootBucket.init(true);
            } finally {
              releasePageFromWrite(atomicOperation, rootCacheEntry);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public ORID get(byte[] key) {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
        final BucketSearchResult bucketSearchResult = findBucket(key, atomicOperation);
        if (bucketSearchResult.itemIndex < 0) {
          return null;
        }

        final long pageIndex = bucketSearchResult.pageIndex;
        final OCacheEntry keyBucketCacheEntry =
            loadPageForRead(atomicOperation, fileId, pageIndex, false);
        try {
          final Bucket keyBucket = new Bucket(keyBucketCacheEntry);
          return keyBucket.getValue(bucketSearchResult.itemIndex);
        } finally {
          releasePageFromRead(atomicOperation, keyBucketCacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new BinaryBTreeException(
              "Error during retrieving  of binary btree with name " + getName(), this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  private BucketSearchResult findBucket(final byte[] key, final OAtomicOperation atomicOperation)
      throws IOException {
    long pageIndex = ROOT_INDEX;

    int depth = 0;

    byte[] bucketLLB = ROOT_LARGEST_LOWER_BOUND;

    byte[] bucketSUB = ROOT_SMALLEST_UPPER_BOUND;

    byte[] keyPrefix = new byte[0];
    while (true) {
      depth++;
      if (depth > maxPathLength) {
        throw new BinaryBTreeException(
            "We reached max level of depth of binary BTree but still found nothing, "
                + "seems like tree is in corrupted state. You should rebuild index related to given query.",
            this);
      }

      final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      try {
        @SuppressWarnings("ObjectAllocationInLoop")
        final Bucket bucket = new Bucket(bucketEntry);
        final int index = bucket.find(key, keyPrefix.length, key.length - keyPrefix.length);

        if (bucket.isLeaf()) {
          return new BucketSearchResult(index, pageIndex);
        }

        if (index >= 0) {
          pageIndex = bucket.getRight(index);
          byte[] bucketKey = bucket.getKey(index);
          bucketLLB = new byte[keyPrefix.length + bucketKey.length];

          System.arraycopy(keyPrefix, 0, bucketLLB, 0, keyPrefix.length);
          System.arraycopy(bucketKey, 0, bucketLLB, keyPrefix.length, bucketKey.length);

          if (index < bucket.size() - 1) {
            bucketKey = bucket.getKey(index + 1);
            bucketSUB = new byte[bucketKey.length + keyPrefix.length];

            System.arraycopy(keyPrefix, 0, bucketSUB, 0, keyPrefix.length);
            System.arraycopy(bucketKey, 0, bucketSUB, keyPrefix.length, bucketKey.length);
          }

          bucketLLB = calculateLargestLowerBoundary(bucket, index, false, bucketLLB);
        } else {
          final int insertionIndex = -index - 1;
          if (insertionIndex >= bucket.size()) {
            pageIndex = bucket.getRight(insertionIndex - 1);

            final byte[] bucketKey = bucket.getKey(insertionIndex - 1);
            bucketLLB = new byte[bucketKey.length + keyPrefix.length];

            System.arraycopy(keyPrefix, 0, bucketLLB, 0, keyPrefix.length);
            System.arraycopy(bucketKey, 0, bucketLLB, keyPrefix.length, bucketKey.length);
          } else {
            pageIndex = bucket.getLeft(insertionIndex);
            byte[] bucketKey = bucket.getKey(insertionIndex);

            bucketSUB = new byte[bucketKey.length + keyPrefix.length];
            System.arraycopy(keyPrefix, 0, bucketSUB, 0, keyPrefix.length);
            System.arraycopy(bucketKey, 0, bucketSUB, keyPrefix.length, bucketKey.length);

            if (insertionIndex > 0) {
              bucketKey = bucket.getKey(insertionIndex - 1);

              bucketLLB = new byte[bucketKey.length + keyPrefix.length];
              System.arraycopy(keyPrefix, 0, bucketLLB, 0, keyPrefix.length);
              System.arraycopy(bucketKey, 0, bucketLLB, keyPrefix.length, bucketKey.length);
            }
          }
        }

        keyPrefix = extractCommonPrefix(bucketLLB, bucketSUB);
      } finally {
        releasePageFromRead(atomicOperation, bucketEntry);
      }
    }
  }

  private byte[] extractCommonPrefix(
      final byte[] largestLowerBound, final byte[] smallestUpperBound) {
    final int commonLen = Math.min(largestLowerBound.length, smallestUpperBound.length);
    int keyPrefixLen = 0;
    for (int i = 0; i < commonLen; i++) {
      if (largestLowerBound[i] == smallestUpperBound[i]) {
        keyPrefixLen++;
      } else {
        break;
      }
    }

    if (keyPrefixLen < commonLen
        && smallestUpperBound.length == keyPrefixLen + 1
        && smallestUpperBound[keyPrefixLen] - largestLowerBound[keyPrefixLen] == 1) {
      keyPrefixLen++;
    }

    final byte[] keyPrefix = new byte[keyPrefixLen];
    System.arraycopy(smallestUpperBound, 0, keyPrefix, 0, keyPrefixLen);
    return keyPrefix;
  }

  private UpdateBucketSearchResult findBucketForUpdate(
      final byte[] key, final OAtomicOperation atomicOperation) throws IOException {
    long pageIndex = ROOT_INDEX;

    final ArrayList<Long> path = new ArrayList<>(8);
    final ArrayList<Integer> itemIndexes = new ArrayList<>(8);
    final ArrayList<byte[]> keyPrefixes = new ArrayList<>(8);
    final ArrayList<byte[]> largestLowerBounds = new ArrayList<>(8);
    final ArrayList<byte[]> smallestUpperBounds = new ArrayList<>(8);

    byte[] bucketLLB = ROOT_LARGEST_LOWER_BOUND;
    byte[] bucketSUB = ROOT_SMALLEST_UPPER_BOUND;

    byte[] keyPrefix = new byte[0];

    while (true) {
      if (path.size() > maxPathLength) {
        throw new BinaryBTreeException(
            "We reached max level of depth of binary BTree but still found nothing,"
                + " seems like tree is in corrupted state. You should rebuild index related to given query.",
            this);
      }

      path.add(pageIndex);

      keyPrefixes.add(keyPrefix);
      largestLowerBounds.add(bucketLLB);
      smallestUpperBounds.add(bucketSUB);

      final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      try {
        final Bucket bucket = new Bucket(bucketEntry);
        final int index = bucket.find(key, keyPrefix.length, key.length - keyPrefix.length);

        if (bucket.isLeaf()) {
          itemIndexes.add(index);
          return new UpdateBucketSearchResult(
              itemIndexes, keyPrefixes, largestLowerBounds, smallestUpperBounds, path, index);
        }

        if (index >= 0) {
          pageIndex = bucket.getRight(index);

          byte[] bucketKey = bucket.getKey(index);
          bucketLLB = new byte[keyPrefix.length + bucketKey.length];

          System.arraycopy(keyPrefix, 0, bucketLLB, 0, keyPrefix.length);
          System.arraycopy(bucketKey, 0, bucketLLB, keyPrefix.length, bucketKey.length);

          if (index < bucket.size() - 1) {
            bucketKey = bucket.getKey(index + 1);

            bucketSUB = new byte[keyPrefix.length + bucketKey.length];
            System.arraycopy(keyPrefix, 0, bucketSUB, 0, keyPrefix.length);
            System.arraycopy(bucketKey, 0, bucketSUB, keyPrefix.length, bucketKey.length);
          }

          itemIndexes.add(index + 1);
        } else {
          final int insertionIndex = -index - 1;

          if (insertionIndex >= bucket.size()) {
            pageIndex = bucket.getRight(insertionIndex - 1);

            byte[] bucketKey = bucket.getKey(insertionIndex - 1);
            bucketLLB = new byte[bucketKey.length + keyPrefix.length];

            System.arraycopy(keyPrefix, 0, bucketLLB, 0, keyPrefix.length);
            System.arraycopy(bucketKey, 0, bucketLLB, keyPrefix.length, bucketKey.length);
          } else {
            pageIndex = bucket.getLeft(insertionIndex);

            byte[] bucketKey = bucket.getKey(insertionIndex);
            bucketSUB = new byte[bucketKey.length + keyPrefix.length];

            System.arraycopy(keyPrefix, 0, bucketSUB, 0, keyPrefix.length);
            System.arraycopy(bucketKey, 0, bucketSUB, keyPrefix.length, bucketKey.length);

            if (insertionIndex > 0) {
              bucketKey = bucket.getKey(insertionIndex - 1);

              bucketLLB = new byte[bucketKey.length + keyPrefix.length];
              System.arraycopy(keyPrefix, 0, bucketLLB, 0, keyPrefix.length);
              System.arraycopy(bucketKey, 0, bucketLLB, keyPrefix.length, bucketKey.length);
            }
          }

          itemIndexes.add(insertionIndex);
        }

        keyPrefix = extractCommonPrefix(bucketLLB, bucketSUB);
      } finally {
        releasePageFromRead(atomicOperation, bucketEntry);
      }
    }
  }

  private Optional<RemoveSearchResult> findBucketForRemove(
      final byte[] key, final OAtomicOperation atomicOperation) throws IOException {

    final ArrayList<RemovalPathItem> path = new ArrayList<>(8);
    final ArrayList<byte[]> keyPrefixes = new ArrayList<>(8);

    ByteArrayList bucketLLB = new ByteArrayList();
    bucketLLB.add(ROOT_LARGEST_LOWER_BOUND);

    ByteArrayList bucketSUB = new ByteArrayList();
    bucketSUB.add(ROOT_SMALLEST_UPPER_BOUND);

    ByteArrayList keyPrefix = new ByteArrayList();

    long pageIndex = ROOT_INDEX;

    int depth = 0;
    while (true) {
      depth++;
      if (depth > maxPathLength) {
        throw new BinaryBTreeException(
            "We reached max level of depth of BTree but still found nothing, seems like tree is in corrupted state."
                + " You should rebuild index related to given query.",
            this);
      }

      final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      try {
        final Bucket bucket = new Bucket(bucketEntry);

        final int index = bucket.find(key, keyPrefix.size(), key.length - keyPrefix.size());

        if (bucket.isLeaf()) {
          if (index < 0) {
            return Optional.empty();
          }

          return Optional.of(new RemoveSearchResult(pageIndex, index, path, keyPrefixes));
        }

        if (index >= 0) {
          path.add(new RemovalPathItem(pageIndex, index, false));

          pageIndex = bucket.getRight(index);

          bucketLLB = new ByteArrayList();
          bucketLLB.addAll(keyPrefix);
          bucketLLB.add(bucket.getKey(index));

          if (index < bucket.size() - 1) {
            bucketSUB = new ByteArrayList();
            bucketSUB.addAll(keyPrefix);

            bucketSUB.add(bucket.getKey(index + 1));
          }
        } else {
          final int insertionIndex = -index - 1;
          if (insertionIndex >= bucket.size()) {
            path.add(new RemovalPathItem(pageIndex, insertionIndex - 1, false));

            pageIndex = bucket.getRight(insertionIndex - 1);

            bucketLLB = new ByteArrayList();

            bucketLLB.addAll(keyPrefix);
            bucketLLB.add(bucket.getKey(insertionIndex - 1));
          } else {
            path.add(new RemovalPathItem(pageIndex, insertionIndex, true));

            pageIndex = bucket.getLeft(insertionIndex);

            bucketSUB = new ByteArrayList();
            bucketSUB.addAll(keyPrefix);
            bucketSUB.add(bucket.getKey(insertionIndex));

            if (insertionIndex > 0) {
              bucketLLB = new ByteArrayList();
              bucketLLB.addAll(keyPrefix);
              bucketLLB.add(bucket.getKey(insertionIndex - 1));
            }
          }
        }

        keyPrefix = new ByteArrayList();
        final int commonLen = Math.min(bucketLLB.size(), bucketSUB.size());
        for (int i = 0; i < commonLen; i++) {
          if (bucketLLB.get(i) == bucketSUB.get(i)) {
            keyPrefix.add(bucketSUB.get(i));
          }
        }

        final int keyPrefixSize = keyPrefix.size();

        if (keyPrefixSize < commonLen
            && bucketSUB.size() == keyPrefixSize + 1
            && bucketSUB.get(keyPrefixSize) - bucketLLB.get(keyPrefixSize) == 1) {
          keyPrefix.add(bucketLLB.get(keyPrefixSize));
        }
      } finally {
        releasePageFromRead(atomicOperation, bucketEntry);
      }
    }
  }

  public boolean validatedPut(
      OAtomicOperation atomicOperation,
      final byte[] key,
      final ORID value,
      final OBaseIndexEngine.Validator<byte[], ORID> validator) {
    return update(atomicOperation, key, value, validator);
  }

  public void put(final OAtomicOperation atomicOperation, final byte[] key, final ORID value) {
    update(atomicOperation, key, value, null);
  }

  private boolean update(
      final OAtomicOperation atomicOperation,
      final byte[] key,
      final ORID rid,
      final OBaseIndexEngine.Validator<byte[], ORID> validator) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            ORID value = rid;

            if (key.length + OShortSerializer.SHORT_SIZE > maxKeySize) {
              throw new OTooBigIndexKeyException(
                  "Key size is more than allowed, operation was canceled. Current key size "
                      + (key.length + OShortSerializer.SHORT_SIZE)
                      + ", allowed  "
                      + maxKeySize,
                  getName());
            }
            UpdateBucketSearchResult bucketSearchResult = findBucketForUpdate(key, atomicOperation);

            OCacheEntry keyBucketCacheEntry =
                loadPageForWrite(
                    atomicOperation, fileId, bucketSearchResult.getLastPathItem(), false, true);
            Bucket keyBucket = new Bucket(keyBucketCacheEntry);
            final byte[] oldRawValue;
            if (bucketSearchResult.itemIndex > -1) {
              oldRawValue = keyBucket.getRawValue(bucketSearchResult.itemIndex);
            } else {
              oldRawValue = null;
            }
            final ORID oldValue;
            if (oldRawValue == null) {
              oldValue = null;
            } else {
              final int clusterId = OShortSerializer.INSTANCE.deserializeNative(oldRawValue, 0);
              final long clusterPosition =
                  OLongSerializer.INSTANCE.deserializeNative(
                      oldRawValue, OShortSerializer.SHORT_SIZE);
              oldValue = new ORecordId(clusterId, clusterPosition);
            }

            if (validator != null) {
              boolean failure = true; // assuming validation throws by default
              boolean ignored = false;

              try {

                final Object result = validator.validate(key, oldValue, value);
                if (result == OBaseIndexEngine.Validator.IGNORE) {
                  ignored = true;
                  failure = false;
                  return false;
                }

                value = (ORID) result;
                failure = false;
              } finally {
                if (failure || ignored) {
                  releasePageFromWrite(atomicOperation, keyBucketCacheEntry);
                }
              }
            }

            final byte[] serializedValue = serializeValue(value);

            int insertionIndex;
            final int sizeDiff;
            if (bucketSearchResult.itemIndex >= 0) {
              assert oldRawValue != null;

              final int keyPrefixLen =
                  bucketSearchResult.keyPrefixes.get(bucketSearchResult.keyPrefixes.size() - 1)
                      .length;

              if (oldRawValue.length == serializedValue.length) {
                keyBucket.updateValue(
                    bucketSearchResult.itemIndex, serializedValue, key.length - keyPrefixLen);
                releasePageFromWrite(atomicOperation, keyBucketCacheEntry);
                return true;
              } else {
                keyBucket.removeLeafEntry(bucketSearchResult.itemIndex, key.length - keyPrefixLen);
                insertionIndex = bucketSearchResult.itemIndex;
                sizeDiff = 0;
              }
            } else {
              insertionIndex = -bucketSearchResult.itemIndex - 1;
              sizeDiff = 1;
            }

            byte[] keyPrefix =
                bucketSearchResult.keyPrefixes.get(bucketSearchResult.keyPrefixes.size() - 1);
            if (!keyBucket.addLeafEntry(
                insertionIndex,
                key,
                keyPrefix.length,
                key.length - keyPrefix.length,
                serializedValue)) {
              final SplitBucketResult splitBucketResult =
                  splitBucket(
                      keyBucket,
                      keyBucketCacheEntry,
                      bucketSearchResult.path,
                      bucketSearchResult.insertionIndexes,
                      bucketSearchResult.keyPrefixes,
                      bucketSearchResult.largestLowerBounds,
                      bucketSearchResult.smallestUpperBounds,
                      insertionIndex,
                      key,
                      atomicOperation);

              insertionIndex = splitBucketResult.insertionIndex;
              keyPrefix = splitBucketResult.keyPrefix;

              final long pageIndex = splitBucketResult.pageIndex;

              if (pageIndex != keyBucketCacheEntry.getPageIndex()) {
                releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

                keyBucketCacheEntry =
                    loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
              }

              keyBucket = new Bucket(keyBucketCacheEntry);
              final boolean added =
                  keyBucket.addLeafEntry(
                      insertionIndex,
                      key,
                      keyPrefix.length,
                      key.length - keyPrefix.length,
                      serializedValue);
              if (!added) {
                throw new BinaryBTreeException("Index is broken and needs to be rebuilt", this);
              }
            }

            releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

            if (sizeDiff != 0) {
              updateSize(sizeDiff, atomicOperation);
            }

            return true;
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  private byte[] serializeValue(ORID value) {
    final byte[] serializedValue =
        new byte[OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE];
    OShortSerializer.INSTANCE.serializeNative((short) value.getClusterId(), serializedValue, 0);
    OLongSerializer.INSTANCE.serializeNative(
        value.getClusterPosition(), serializedValue, OShortSerializer.SHORT_SIZE);
    return serializedValue;
  }

  private SplitBucketResult splitBucket(
      final Bucket bucketToSplit,
      final OCacheEntry entryToSplit,
      final List<Long> path,
      final List<Integer> insertionIndexes,
      final List<byte[]> keyPrefixes,
      final List<byte[]> largestLowerBounds,
      final List<byte[]> smallestUpperBounds,
      final int keyIndex,
      final byte[] key,
      final OAtomicOperation atomicOperation)
      throws IOException {
    final boolean splitLeaf = bucketToSplit.isLeaf();
    final int bucketSize = bucketToSplit.size();

    final int indexToSplit;

    byte[] separationKey;
    final byte[] bucketPrefix = keyPrefixes.get(keyPrefixes.size() - 1);

    if (splitLeaf) {
      final int median = bucketSize >>> 1;
      int minLen = Integer.MAX_VALUE;
      int minIndex = Integer.MIN_VALUE;

      for (int i = -1; i < 2; i++) {
        int index = median + i;

        final byte[] keyOne;
        final int keyOneLen;
        final int keyOneOffset;

        if (keyIndex == index) {
          keyOne = key;
          keyOneLen = key.length - bucketPrefix.length;
          keyOneOffset = bucketPrefix.length;
        } else {
          keyOne = bucketToSplit.getKey(index - 1);
          keyOneLen = key.length;
          keyOneOffset = 0;
        }

        final byte[] keyTwo = bucketToSplit.getKey(index);

        final int commonLen = Math.min(keyOneLen, keyTwo.length);

        boolean keyFound = false;
        for (int k = 0; k < commonLen; k++) {
          if (keyOne[k + keyOneOffset] < keyTwo[k]) {
            if (minLen > k + 1) {
              minLen = k + 1;
              minIndex = index;
            }

            keyFound = true;
            break;
          }
        }

        if (!keyFound && minLen > commonLen + 1) {
          assert keyOneLen > keyTwo.length;

          minLen = commonLen + 1;
          minIndex = index;
        }
      }

      if (minIndex == Integer.MIN_VALUE) {
        throw new IllegalStateException("Separation key was not found");
      }

      indexToSplit = minIndex;
      separationKey = new byte[minLen];
      System.arraycopy(bucketToSplit.getKey(indexToSplit), 0, separationKey, 0, minLen);
    } else {
      indexToSplit = bucketSize >>> 1;
      separationKey = bucketToSplit.getKey(indexToSplit);
    }

    Objects.requireNonNull(separationKey);

    final int startRightIndex = splitLeaf ? indexToSplit : indexToSplit + 1;

    final ArrayList<Bucket.Entry> leftEntries = new ArrayList<>(indexToSplit);
    final ArrayList<Bucket.Entry> rightEntries = new ArrayList<>(bucketSize - startRightIndex);

    for (int i = 0; i < indexToSplit; i++) {
      leftEntries.add(bucketToSplit.getEntry(i));
    }

    for (int i = startRightIndex; i < bucketSize; i++) {
      rightEntries.add(bucketToSplit.getEntry(i));
    }

    if (entryToSplit.getPageIndex() != ROOT_INDEX) {
      return splitNonRootBucket(
          path,
          insertionIndexes,
          leftEntries,
          rightEntries,
          keyPrefixes,
          largestLowerBounds,
          smallestUpperBounds,
          keyIndex,
          entryToSplit.getPageIndex(),
          bucketToSplit,
          splitLeaf,
          indexToSplit,
          separationKey,
          atomicOperation);
    } else {
      return splitRootBucket(
          keyIndex,
          entryToSplit,
          splitLeaf,
          indexToSplit,
          separationKey,
          leftEntries,
          rightEntries,
          atomicOperation);
    }
  }

  private SplitBucketResult splitNonRootBucket(
      final List<Long> path,
      final List<Integer> insertionIndexes,
      final List<Bucket.Entry> leftEntries,
      final List<Bucket.Entry> rightEntries,
      final List<byte[]> keyPrefixes,
      final List<byte[]> largestLowerBounds,
      final List<byte[]> smallestUpperBounds,
      final int keyIndex,
      final long pageIndex,
      final Bucket bucketToSplit,
      final boolean splitLeaf,
      final int indexToSplit,
      final byte[] separationKeyBase,
      final OAtomicOperation atomicOperation)
      throws IOException {

    final byte[] rightBucketPrefix;
    final byte[] leftBucketPrefix;

    final byte[] leftLLB;
    final byte[] leftSUB;

    final byte[] rightLLB;
    final byte[] rightSUB;

    final OCacheEntry rightBucketEntry = allocateNewPage(atomicOperation);
    try {
      final Bucket newRightBucket = new Bucket(rightBucketEntry);
      newRightBucket.init(splitLeaf);
      bucketToSplit.shrink(0);

      if (splitLeaf) {
        final long rightSiblingPageIndex = bucketToSplit.getRightSibling();

        newRightBucket.setRightSibling(rightSiblingPageIndex);
        newRightBucket.setLeftSibling(pageIndex);

        bucketToSplit.setRightSibling(rightBucketEntry.getPageIndex());

        if (rightSiblingPageIndex >= 0) {
          final OCacheEntry rightSiblingBucketEntry =
              loadPageForWrite(atomicOperation, fileId, rightSiblingPageIndex, false, true);
          final Bucket rightSiblingBucket = new Bucket(rightSiblingBucketEntry);
          try {
            rightSiblingBucket.setLeftSibling(rightBucketEntry.getPageIndex());
          } finally {
            releasePageFromWrite(atomicOperation, rightSiblingBucketEntry);
          }
        }
      }

      long parentIndex = path.get(path.size() - 2);
      OCacheEntry parentCacheEntry =
          loadPageForWrite(atomicOperation, fileId, parentIndex, false, true);
      try {
        Bucket parentBucket = new Bucket(parentCacheEntry);
        int parentInsertionIndex = insertionIndexes.get(insertionIndexes.size() - 2);

        final byte[] commonPrefix = keyPrefixes.get(keyPrefixes.size() - 1);

        byte[] parentLLB = largestLowerBounds.get(largestLowerBounds.size() - 2);
        byte[] parentSUB = smallestUpperBounds.get(smallestUpperBounds.size() - 2);
        byte[] parentKeyPrefix = keyPrefixes.get(keyPrefixes.size() - 2);

        // parent prefix is typically smaller than child prefix so we need to add portion of the
        // child prefix
        // into the begging
        byte[] separationKey =
            calculateSeparationKey(separationKeyBase, commonPrefix, parentKeyPrefix);

        if (!parentBucket.addNonLeafEntry(
            parentInsertionIndex,
            (int) pageIndex,
            rightBucketEntry.getPageIndex(),
            separationKey)) {
          final SplitBucketResult splitBucketResult =
              splitBucket(
                  parentBucket,
                  parentCacheEntry,
                  path.subList(0, path.size() - 1),
                  insertionIndexes.subList(0, insertionIndexes.size() - 1),
                  keyPrefixes.subList(0, keyPrefixes.size() - 1),
                  largestLowerBounds.subList(0, largestLowerBounds.size() - 1),
                  smallestUpperBounds.subList(0, smallestUpperBounds.size() - 1),
                  parentInsertionIndex,
                  separationKeyBase,
                  atomicOperation);

          parentIndex = splitBucketResult.pageIndex;
          parentInsertionIndex = splitBucketResult.insertionIndex;

          parentLLB = splitBucketResult.largestLowerBound;
          parentSUB = splitBucketResult.smallestUpperBound;
          parentKeyPrefix = splitBucketResult.keyPrefix;

          separationKey = calculateSeparationKey(separationKeyBase, commonPrefix, parentKeyPrefix);

          if (parentIndex != parentCacheEntry.getPageIndex()) {
            releasePageFromWrite(atomicOperation, parentCacheEntry);

            parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentIndex, false, true);
          }

          parentBucket = new Bucket(parentCacheEntry);
          final boolean added =
              parentBucket.addNonLeafEntry(
                  parentInsertionIndex,
                  (int) pageIndex,
                  rightBucketEntry.getPageIndex(),
                  separationKey);
          if (!added) {
            throw new BinaryBTreeException("Index is broken and needs to be rebuilt", this);
          }
        }

        if (parentInsertionIndex > 0) {
          final byte[] key = parentBucket.getKey(parentInsertionIndex - 1);
          leftLLB = new byte[parentKeyPrefix.length + key.length];

          System.arraycopy(parentKeyPrefix, 0, leftLLB, 0, parentKeyPrefix.length);
          System.arraycopy(key, 0, leftLLB, parentKeyPrefix.length, key.length);
        } else {
          leftLLB = parentLLB;
        }

        leftSUB = new byte[parentKeyPrefix.length + separationKey.length];
        System.arraycopy(parentKeyPrefix, 0, leftSUB, 0, parentKeyPrefix.length);
        System.arraycopy(separationKey, 0, leftSUB, parentKeyPrefix.length, separationKey.length);

        rightLLB = leftSUB;

        if (parentInsertionIndex < parentBucket.size() - 1) {
          final byte[] key = parentBucket.getKey(parentInsertionIndex + 1);
          rightSUB = new byte[key.length + parentKeyPrefix.length];

          System.arraycopy(parentKeyPrefix, 0, rightSUB, 0, parentKeyPrefix.length);
          System.arraycopy(key, 0, rightSUB, parentKeyPrefix.length, key.length);
        } else {
          rightSUB = parentSUB;
        }

        assert COMPARATOR.compare(rightSUB, rightLLB) > 0;
        assert COMPARATOR.compare(leftSUB, leftLLB) > 0;

        rightBucketPrefix = extractCommonPrefix(rightLLB, rightSUB);
        leftBucketPrefix = extractCommonPrefix(leftLLB, leftSUB);

        assert commonPrefix.length <= leftBucketPrefix.length;
        assert commonPrefix.length <= rightBucketPrefix.length;

        final int leftKeyOffset = leftBucketPrefix.length - commonPrefix.length;
        final int rightKeyOffset = rightBucketPrefix.length - commonPrefix.length;

        for (int i = 0; i < leftEntries.size(); i++) {
          final Bucket.Entry entry = leftEntries.get(i);

          if (splitLeaf) {
            bucketToSplit.addLeafEntry(
                i,
                entry.key,
                leftKeyOffset,
                entry.key.length - leftKeyOffset,
                serializeValue(entry.value));
          } else {
            bucketToSplit.addNonLeafEntry(
                i,
                entry.leftChild,
                entry.rightChild,
                entry.key,
                leftKeyOffset,
                entry.key.length - leftKeyOffset);
          }
        }

        for (int i = 0; i < rightEntries.size(); i++) {
          final Bucket.Entry entry = rightEntries.get(i);
          if (splitLeaf) {
            newRightBucket.addLeafEntry(
                i,
                entry.key,
                rightKeyOffset,
                entry.key.length - rightKeyOffset,
                serializeValue(entry.value));
          } else {
            newRightBucket.addNonLeafEntry(
                i,
                entry.leftChild,
                entry.rightChild,
                entry.key,
                rightKeyOffset,
                entry.key.length - rightKeyOffset);
          }
        }
      } finally {
        releasePageFromWrite(atomicOperation, parentCacheEntry);
      }
    } finally {
      releasePageFromWrite(atomicOperation, rightBucketEntry);
    }

    // handle case when we split the leaf bucket
    if (splitLeaf) {
      // key is going to be inserted into the left bucket
      if (keyIndex <= indexToSplit) {
        return new SplitBucketResult(pageIndex, keyIndex, leftLLB, leftSUB, leftBucketPrefix);
      }

      // key to insert is bigger than separation key and is going to be inserted into the right
      // bucket
      return new SplitBucketResult(
          rightBucketEntry.getPageIndex(),
          keyIndex - indexToSplit,
          rightLLB,
          rightSUB,
          rightBucketPrefix);
    }

    // key to insert is smaller than separation key, entry is going to be inserted into the left
    // bucket
    if (keyIndex <= indexToSplit) {
      return new SplitBucketResult(pageIndex, keyIndex, leftLLB, leftSUB, leftBucketPrefix);
    }

    // key to insert is bigger than separation key and is going to be inserted into the right bucket
    return new SplitBucketResult(
        rightBucketEntry.getPageIndex(),
        keyIndex - indexToSplit - 1,
        rightLLB,
        rightSUB,
        rightBucketPrefix);
  }

  private byte[] calculateSeparationKey(
      byte[] separationKeyBase, byte[] commonPrefix, byte[] parentKeyPrefix) {
    byte[] separationKey;
    if (commonPrefix.length > parentKeyPrefix.length) {
      separationKey =
          new byte[separationKeyBase.length + commonPrefix.length - parentKeyPrefix.length];
      System.arraycopy(
          commonPrefix,
          parentKeyPrefix.length,
          separationKey,
          0,
          commonPrefix.length - parentKeyPrefix.length);
      System.arraycopy(
          separationKeyBase,
          0,
          separationKey,
          commonPrefix.length - parentKeyPrefix.length,
          separationKeyBase.length);
    } else {
      separationKey = separationKeyBase;
    }
    return separationKey;
  }

  private OCacheEntry allocateNewPage(OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry rightBucketEntry;
    final OCacheEntry entryPointCacheEntry =
        loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, false, true);
    try {
      final EntryPoint entryPoint = new EntryPoint(entryPointCacheEntry);
      final int freeListHead = entryPoint.getFreeListHead();
      if (freeListHead > -1) {
        rightBucketEntry = loadPageForWrite(atomicOperation, fileId, freeListHead, false, false);
        final Bucket bucket = new Bucket(rightBucketEntry);
        entryPoint.setFreeListHead(bucket.getNextFreeListPage());
      } else {
        int pageSize = entryPoint.getPagesSize();
        if (pageSize < getFilledUpTo(atomicOperation, fileId) - 1) {
          pageSize++;
          rightBucketEntry = loadPageForWrite(atomicOperation, fileId, pageSize, false, false);
          entryPoint.setPagesSize(pageSize);
        } else {
          assert pageSize == getFilledUpTo(atomicOperation, fileId) - 1;

          rightBucketEntry = addPage(atomicOperation, fileId);
          entryPoint.setPagesSize(rightBucketEntry.getPageIndex());
        }
      }
    } finally {
      releasePageFromWrite(atomicOperation, entryPointCacheEntry);
    }

    return rightBucketEntry;
  }

  private void updateSize(final long diffSize, final OAtomicOperation atomicOperation)
      throws IOException {
    if (diffSize == 0) {
      return;
    }

    final OCacheEntry entryPointCacheEntry =
        loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, false, true);
    try {
      final EntryPoint entryPoint = new EntryPoint(entryPointCacheEntry);
      entryPoint.setTreeSize(entryPoint.getTreeSize() + diffSize);
    } finally {
      releasePageFromWrite(atomicOperation, entryPointCacheEntry);
    }
  }

  private SplitBucketResult splitRootBucket(
      final int keyIndex,
      final OCacheEntry bucketEntry,
      final boolean splitLeaf,
      final int indexToSplit,
      final byte[] separationKey,
      final List<Bucket.Entry> leftEntries,
      final List<Bucket.Entry> rightEntries,
      final OAtomicOperation atomicOperation)
      throws IOException {

    final OCacheEntry leftBucketEntry;
    final OCacheEntry rightBucketEntry;

    leftBucketEntry = allocateNewPage(atomicOperation);
    rightBucketEntry = allocateNewPage(atomicOperation);

    final byte[] leftLLB = ROOT_LARGEST_LOWER_BOUND;
    @SuppressWarnings("UnnecessaryLocalVariable")
    final byte[] leftSUB = separationKey;

    final byte[] rightLLB = separationKey;
    @SuppressWarnings("UnnecessaryLocalVariable")
    final byte[] rightSUB = ROOT_SMALLEST_UPPER_BOUND;

    assert COMPARATOR.compare(rightSUB, rightLLB) > 0;
    assert COMPARATOR.compare(leftSUB, leftLLB) > 0;

    final byte[] leftPrefix = extractCommonPrefix(leftLLB, leftSUB);
    final byte[] rightPrefix = extractCommonPrefix(rightLLB, rightSUB);
    try {
      final Bucket newLeftBucket = new Bucket(leftBucketEntry);
      newLeftBucket.init(splitLeaf);

      for (int i = 0; i < leftEntries.size(); i++) {
        final Bucket.Entry entry = leftEntries.get(i);
        if (splitLeaf) {
          newLeftBucket.addLeafEntry(
              i,
              entry.key,
              leftPrefix.length,
              entry.key.length - leftPrefix.length,
              serializeValue(entry.value));
        } else {
          newLeftBucket.addNonLeafEntry(
              i,
              entry.leftChild,
              entry.rightChild,
              entry.key,
              leftPrefix.length,
              entry.key.length - leftPrefix.length);
        }
      }

      if (splitLeaf) {
        newLeftBucket.setRightSibling(rightBucketEntry.getPageIndex());
      }

    } finally {
      releasePageFromWrite(atomicOperation, leftBucketEntry);
    }

    try {
      final Bucket newRightBucket = new Bucket(rightBucketEntry);
      newRightBucket.init(splitLeaf);

      for (int i = 0; i < rightEntries.size(); i++) {
        final Bucket.Entry entry = rightEntries.get(i);
        if (splitLeaf) {
          newRightBucket.addLeafEntry(
              i,
              entry.key,
              rightPrefix.length,
              entry.key.length - rightPrefix.length,
              serializeValue(entry.value));
        } else {
          newRightBucket.addNonLeafEntry(
              i,
              entry.leftChild,
              entry.rightChild,
              entry.key,
              rightPrefix.length,
              entry.key.length - rightPrefix.length);
        }
      }

      if (splitLeaf) {
        newRightBucket.setLeftSibling(leftBucketEntry.getPageIndex());
      }
    } finally {
      releasePageFromWrite(atomicOperation, rightBucketEntry);
    }

    final Bucket bucketToSplit = new Bucket(bucketEntry);
    bucketToSplit.shrink(0);
    if (splitLeaf) {
      bucketToSplit.switchBucketType();
    }
    bucketToSplit.addNonLeafEntry(
        0, leftBucketEntry.getPageIndex(), rightBucketEntry.getPageIndex(), separationKey);

    if (splitLeaf) {
      if (keyIndex <= indexToSplit) {
        return new SplitBucketResult(
            leftBucketEntry.getPageIndex(), keyIndex, leftLLB, leftSUB, leftPrefix);
      }

      return new SplitBucketResult(
          rightBucketEntry.getPageIndex(),
          keyIndex - indexToSplit,
          rightLLB,
          rightSUB,
          rightPrefix);
    }

    if (keyIndex <= indexToSplit) {
      return new SplitBucketResult(
          leftBucketEntry.getPageIndex(), keyIndex, leftLLB, leftSUB, leftPrefix);
    }

    return new SplitBucketResult(
        rightBucketEntry.getPageIndex(),
        keyIndex - indexToSplit - 1,
        rightLLB,
        rightSUB,
        rightPrefix);
  }

  public long size() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

        final OCacheEntry entryPointCacheEntry =
            loadPageForRead(atomicOperation, fileId, ENTRY_POINT_INDEX, false);
        try {
          final EntryPoint entryPoint = new EntryPoint(entryPointCacheEntry);
          return entryPoint.getTreeSize();
        } finally {
          releasePageFromRead(atomicOperation, entryPointCacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new BinaryBTreeException("Error during retrieving of size of index " + getName(), this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public void delete(final OAtomicOperation atomicOperation) {
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

            deleteFile(atomicOperation, fileId);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public void load(final String name) {
    acquireExclusiveLock();
    try {
      final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

      fileId = openFile(atomicOperation, getFullName());
    } catch (final IOException e) {
      throw OException.wrapException(
          new BinaryBTreeException("Exception during loading of binary BTree " + name, this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public ORID remove(final OAtomicOperation atomicOperation, final byte[] key) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final ORID removedValue;

            final Optional<RemoveSearchResult> bucketSearchResult =
                findBucketForRemove(key, atomicOperation);

            if (bucketSearchResult.isPresent()) {
              final RemoveSearchResult removeSearchResult = bucketSearchResult.get();
              final OCacheEntry keyBucketCacheEntry =
                  loadPageForWrite(
                      atomicOperation, fileId, removeSearchResult.leafPageIndex, false, true);

              final byte[] rawValue;
              final int bucketSize;
              try {
                final Bucket keyBucket = new Bucket(keyBucketCacheEntry);
                rawValue = keyBucket.getRawValue(removeSearchResult.leafEntryPageIndex);
                bucketSize =
                    keyBucket.removeLeafEntry(removeSearchResult.leafEntryPageIndex, key.length);
                updateSize(-1, atomicOperation);

                final int clusterId = OShortSerializer.INSTANCE.deserializeNative(rawValue, 0);
                final long clusterPosition =
                    OLongSerializer.INSTANCE.deserializeNative(
                        rawValue, OShortSerializer.SHORT_SIZE);

                removedValue = new ORecordId(clusterId, clusterPosition);

                // skip balancing of the tree if leaf is a root.
                if (bucketSize == 0 && removeSearchResult.path.size() > 0) {
                  if (balanceLeafNodeAfterItemDelete(
                      atomicOperation, removeSearchResult, keyBucket)) {
                    addToFreeList(atomicOperation, (int) removeSearchResult.leafPageIndex);
                  }
                }
              } finally {
                releasePageFromWrite(atomicOperation, keyBucketCacheEntry);
              }
            } else {
              return null;
            }

            return removedValue;
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public byte[] firstKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

        final Optional<BucketSearchResult> searchResult = firstItem(atomicOperation);
        if (!searchResult.isPresent()) {
          return null;
        }

        final BucketSearchResult result = searchResult.get();
        final OCacheEntry cacheEntry =
            loadPageForRead(atomicOperation, fileId, result.pageIndex, false);
        try {
          final Bucket bucket = new Bucket(cacheEntry);
          return bucket.getKey(result.itemIndex);
        } finally {
          releasePageFromRead(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new BinaryBTreeException("Error during finding first key", this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  private Optional<BucketSearchResult> firstItem(final OAtomicOperation atomicOperation)
      throws IOException {
    final LinkedList<PagePathItemUnit> path = new LinkedList<>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);
    int itemIndex = 0;
    try {
      Bucket bucket = new Bucket(cacheEntry);

      while (true) {
        if (!bucket.isLeaf()) {
          if (bucket.isEmpty() || itemIndex > bucket.size()) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex + 1;
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

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex + 1;
            } else {
              return Optional.empty();
            }
          } else {
            return Optional.of(new BucketSearchResult(0, bucketIndex));
          }
        }

        releasePageFromRead(atomicOperation, cacheEntry);

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);
        //noinspection ObjectAllocationInLoop
        bucket = new Bucket(cacheEntry);
      }
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }

  public byte[] lastKey() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

        final Optional<BucketSearchResult> searchResult = lastItem(atomicOperation);
        if (!searchResult.isPresent()) {
          return null;
        }

        final BucketSearchResult result = searchResult.get();
        final OCacheEntry cacheEntry =
            loadPageForRead(atomicOperation, fileId, result.pageIndex, false);
        try {
          final Bucket bucket = new Bucket(cacheEntry);
          return bucket.getKey(result.itemIndex);
        } finally {
          releasePageFromRead(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new BinaryBTreeException("Error during finding last key", this), e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  private Optional<BucketSearchResult> lastItem(final OAtomicOperation atomicOperation)
      throws IOException {
    final LinkedList<PagePathItemUnit> path = new LinkedList<>();

    long bucketIndex = ROOT_INDEX;

    OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);

    Bucket bucket = new Bucket(cacheEntry);

    int itemIndex = bucket.size() - 1;
    try {
      while (true) {
        if (!bucket.isLeaf()) {
          if (itemIndex < -1) {
            if (!path.isEmpty()) {
              final PagePathItemUnit pagePathItemUnit = path.removeLast();

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex - 1;
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

              bucketIndex = pagePathItemUnit.pageIndex;
              itemIndex = pagePathItemUnit.itemIndex - 1;
            } else {
              return Optional.empty();
            }
          } else {
            return Optional.of(new BucketSearchResult(bucket.size() - 1, bucketIndex));
          }
        }

        releasePageFromRead(atomicOperation, cacheEntry);

        cacheEntry = loadPageForRead(atomicOperation, fileId, bucketIndex, false);

        //noinspection ObjectAllocationInLoop
        bucket = new Bucket(cacheEntry);
        if (itemIndex == Bucket.MAX_PAGE_SIZE_BYTES + 1) {
          itemIndex = bucket.size() - 1;
        }
      }
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }

  public Stream<ORawPair<byte[], ORID>> allEntries() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        //noinspection resource
        return StreamSupport.stream(new SpliteratorForward(null, null, false, false), false);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public Stream<ORawPair<byte[], ORID>> iterateEntriesMinor(
      final byte[] key, final boolean inclusive, final boolean ascSortOrder) {
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

  private Spliterator<ORawPair<byte[], ORID>> iterateEntriesMinorDesc(
      byte[] key, final boolean inclusive) {
    return new SpliteratorBackward(null, key, false, inclusive);
  }

  private Spliterator<ORawPair<byte[], ORID>> iterateEntriesMinorAsc(
      byte[] key, final boolean inclusive) {
    return new SpliteratorForward(null, key, false, inclusive);
  }

  public Stream<ORawPair<byte[], ORID>> iterateEntriesMajor(
      final byte[] key, final boolean inclusive, final boolean ascSortOrder) {
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

  private Spliterator<ORawPair<byte[], ORID>> iterateEntriesMajorAsc(
      byte[] key, final boolean inclusive) {
    return new SpliteratorForward(key, null, inclusive, false);
  }

  private Spliterator<ORawPair<byte[], ORID>> iterateEntriesMajorDesc(
      byte[] key, final boolean inclusive) {
    acquireSharedLock();
    try {
      return new SpliteratorBackward(key, null, inclusive, false);
    } finally {
      releaseSharedLock();
    }
  }

  public Stream<ORawPair<byte[], ORID>> iterateEntriesBetween(
      final byte[] keyFrom,
      final boolean fromInclusive,
      final byte[] keyTo,
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

  private Spliterator<ORawPair<byte[], ORID>> iterateEntriesBetweenAscOrder(
      byte[] keyFrom, final boolean fromInclusive, byte[] keyTo, final boolean toInclusive) {
    return new SpliteratorForward(keyFrom, keyTo, fromInclusive, toInclusive);
  }

  private Spliterator<ORawPair<byte[], ORID>> iterateEntriesBetweenDescOrder(
      byte[] keyFrom, final boolean fromInclusive, byte[] keyTo, final boolean toInclusive) {
    return new SpliteratorBackward(keyFrom, keyTo, fromInclusive, toInclusive);
  }

  private boolean balanceLeafNodeAfterItemDelete(
      final OAtomicOperation atomicOperation,
      final RemoveSearchResult removeSearchResult,
      final Bucket keyBucket)
      throws IOException {
    final RemovalPathItem parentItem =
        removeSearchResult.path.get(removeSearchResult.path.size() - 1);

    final OCacheEntry parentCacheEntry =
        loadPageForWrite(atomicOperation, fileId, parentItem.pageIndex, true, true);
    try {
      final Bucket parentBucket = new Bucket(parentCacheEntry);

      if (parentItem.leftChild) {
        final int rightSiblingPageIndex = parentBucket.getRight(parentItem.indexInsidePage);
        // merge with left sibling
        final OCacheEntry rightSiblingEntry =
            loadPageForWrite(atomicOperation, fileId, rightSiblingPageIndex, true, true);
        try {
          final Bucket rightSiblingBucket = new Bucket(rightSiblingEntry);
          final boolean deletionSuccessful =
              deleteFromNonLeafNode(
                  atomicOperation, parentBucket, rightSiblingBucket, removeSearchResult.path);

          if (deletionSuccessful) {
            final long leftSiblingIndex = keyBucket.getLeftSibling();
            assert rightSiblingBucket.getLeftSibling() == keyBucket.getCacheEntry().getPageIndex();

            rightSiblingBucket.setLeftSibling(leftSiblingIndex);

            if (leftSiblingIndex > 0) {
              final OCacheEntry leftSiblingEntry =
                  loadPageForWrite(atomicOperation, fileId, leftSiblingIndex, true, true);
              try {
                final Bucket leftSiblingBucket = new Bucket(leftSiblingEntry);

                leftSiblingBucket.setRightSibling(rightSiblingPageIndex);
              } finally {
                releasePageFromWrite(atomicOperation, leftSiblingEntry);
              }
            }
          }

          return deletionSuccessful;
        } finally {
          releasePageFromWrite(atomicOperation, rightSiblingEntry);
        }
      }

      final int leftSiblingPageIndex = parentBucket.getLeft(parentItem.indexInsidePage);
      final OCacheEntry leftSiblingEntry =
          loadPageForWrite(atomicOperation, fileId, leftSiblingPageIndex, true, true);
      try {
        // merge with right sibling
        final Bucket leftSiblingBucket = new Bucket(leftSiblingEntry);
        final boolean deletionSuccessful =
            deleteFromNonLeafNode(
                atomicOperation, parentBucket, leftSiblingBucket, removeSearchResult.path);

        final long rightSiblingIndex = keyBucket.getRightSibling();

        assert leftSiblingBucket.getRightSibling() == keyBucket.getCacheEntry().getPageIndex();
        leftSiblingBucket.setRightSibling(rightSiblingIndex);

        if (rightSiblingIndex > 0) {
          final OCacheEntry rightSiblingEntry =
              loadPageForWrite(atomicOperation, fileId, rightSiblingIndex, true, true);
          try {
            final Bucket rightSibling = new Bucket(rightSiblingEntry);
            rightSibling.setLeftSibling(leftSiblingPageIndex);
          } finally {
            releasePageFromWrite(atomicOperation, rightSiblingEntry);
          }
        }

        return deletionSuccessful;
      } finally {
        releasePageFromWrite(atomicOperation, leftSiblingEntry);
      }
    } finally {
      releasePageFromWrite(atomicOperation, parentCacheEntry);
    }
  }

  private Optional<NonLeafItemDeletionResult> deleteFromNonLeafNode(
      final OAtomicOperation atomicOperation,
      final Bucket bucket,
      final Bucket mergedChildNode,
      final List<RemovalPathItem> path)
      throws IOException {
    final RemovalPathItem currentItem = path.get(path.size() - 1);

    final int bucketSize = bucket.size();
    assert bucketSize > 0;

    // currently processed node is a root node
    if (path.size() == 1) {
      bucket.removeNonLeafEntry(currentItem.indexInsidePage, currentItem.leftChild);

      if (bucketSize == 1) {
        final int childNodeSize = mergedChildNode.size();
        if (childNodeSize == 0) {
          throw new BinaryBTreeException("BTree is broken", this);
        }

        if (mergedChildNode.isLeaf()) {
          bucket.switchBucketType();
        }

        final ArrayList<byte[]> entries = new ArrayList<>();

        for (int i = 0; i < childNodeSize; i++) {
          final byte[] rawEntry = mergedChildNode.getRawEntry(i);
          entries.add(rawEntry);
        }

        bucket.addAll(entries);

        addToFreeList(atomicOperation, mergedChildNode.getCacheEntry().getPageIndex());
      }

      return true;
    }

    if (bucketSize > 1) {
      bucket.removeNonLeafEntry(currentItem.indexInsidePage, currentItem.leftChild);
      return true;
    }

    final RemovalPathItem parentItem = path.get(path.size() - 2);
    final OCacheEntry parentCacheEntry =
        loadPageForWrite(atomicOperation, fileId, parentItem.pageIndex, true, true);
    try {
      final Bucket parentBucket = new Bucket(parentCacheEntry);

      final int orphanPointer;
      // left child is merged with sibling
      if (currentItem.leftChild) {
        orphanPointer = bucket.getRight(currentItem.indexInsidePage);
      } else {
        orphanPointer = bucket.getLeft(currentItem.indexInsidePage);
      }

      if (parentItem.leftChild) {
        final int rightSiblingPageIndex = parentBucket.getRight(parentItem.indexInsidePage);
        final OCacheEntry rightSiblingEntry =
            loadPageForWrite(atomicOperation, fileId, rightSiblingPageIndex, true, true);
        try {
          final Bucket rightSiblingBucket = new Bucket(rightSiblingEntry);

          final int rightSiblingBucketSize = rightSiblingBucket.size();
          if (rightSiblingBucketSize > 1) {
            return rotateNoneLeafLeftAndRemoveItem(
                parentItem, parentBucket, bucket, rightSiblingBucket, orphanPointer);
          } else if (rightSiblingBucketSize == 1) {
            return mergeNoneLeafWithRightSiblingAndDeleteItem(
                atomicOperation,
                parentItem,
                parentBucket,
                bucket,
                rightSiblingBucket,
                orphanPointer,
                path);
          }
          return false;
        } finally {
          releasePageFromWrite(atomicOperation, rightSiblingEntry);
        }
      } else {
        final int leftSiblingPageIndex = parentBucket.getLeft(parentItem.indexInsidePage);

        final OCacheEntry leftSiblingEntry =
            loadPageForWrite(atomicOperation, fileId, leftSiblingPageIndex, true, true);
        try {
          final Bucket leftSiblingBucket = new Bucket(leftSiblingEntry);
          assert leftSiblingBucket.size() > 0;

          final int leftSiblingBucketSize = leftSiblingBucket.size();
          if (leftSiblingBucketSize > 1) {
            return rotateNoneLeafRightAndRemoveItem(
                parentItem, parentBucket, bucket, leftSiblingBucket, orphanPointer);
          } else if (leftSiblingBucketSize == 1) {
            return mergeNoneLeafWithLeftSiblingAndDeleteItem(
                atomicOperation,
                parentItem,
                parentBucket,
                bucket,
                leftSiblingBucket,
                orphanPointer,
                path);
          }

          return false;
        } finally {
          releasePageFromWrite(atomicOperation, leftSiblingEntry);
        }
      }

    } finally {
      releasePageFromWrite(atomicOperation, parentCacheEntry);
    }
  }

  private Optional<NonLeafItemDeletionResult> rotateNoneLeafRightAndRemoveItem(
      final RemovalPathItem parentItem,
      final Bucket parentBucket,
      final byte[] parentKeyPrefix,
      final byte[] parentLargestLowerBound,
      final byte[] parentSmallestUpperBound,
      final Bucket bucket,
      final Bucket leftSibling,
      final byte[] leftSiblingKeyPrefix,
      final int orhanPointer) {
    if (bucket.size() != 1 || leftSibling.size() <= 1) {
      throw new BinaryBTreeException("BTree is broken", this);
    }

    final byte[] partialBucketKey = parentBucket.getKey(parentItem.indexInsidePage);
    final int leftSiblingSize = leftSibling.size();
    final byte[] partialSeparatorKey = leftSibling.getKey(leftSiblingSize - 1);

    assert leftSiblingKeyPrefix.length >= parentKeyPrefix.length;

    final byte[] separatorKey;
    if (leftSiblingKeyPrefix.length > parentKeyPrefix.length) {
      final int prefixDif = leftSiblingKeyPrefix.length - parentKeyPrefix.length;
      separatorKey = new byte[partialSeparatorKey.length + prefixDif];
      System.arraycopy(leftSiblingKeyPrefix, parentKeyPrefix.length, separatorKey, 0, prefixDif);
      System.arraycopy(partialSeparatorKey, 0, separatorKey, prefixDif, partialSeparatorKey.length);
    } else {
      separatorKey = partialSeparatorKey;
    }

    if (!parentBucket.updateKey(parentItem.indexInsidePage, separatorKey)) {
      return Optional.empty();
    }

    final int bucketLeft = leftSibling.getRight(leftSiblingSize - 1);

    leftSibling.removeNonLeafEntry(leftSiblingSize - 1, false);
    bucket.removeNonLeafEntry(0, true);

    final byte[] leftSiblingLargestLowerBound;
    if (parentItem.indexInsidePage > 0) {
      final byte[] partialKey = parentBucket.getKey(parentItem.indexInsidePage - 1);
      leftSiblingLargestLowerBound = new byte[partialKey.length + parentKeyPrefix.length];
      System.arraycopy(parentKeyPrefix, 0, leftSiblingLargestLowerBound, 0, parentKeyPrefix.length);
      System.arraycopy(
          partialKey, 0, leftSiblingLargestLowerBound, parentKeyPrefix.length, partialKey.length);
    } else {
      leftSiblingLargestLowerBound = parentLargestLowerBound;
    }

    final byte[] leftSiblingSmallestUpperBound =
        new byte[separatorKey.length + parentKeyPrefix.length];
    System.arraycopy(parentKeyPrefix, 0, leftSiblingSmallestUpperBound, 0, parentKeyPrefix.length);
    System.arraycopy(
        separatorKey,
        0,
        leftSiblingSmallestUpperBound,
        parentKeyPrefix.length,
        separatorKey.length);

    final byte[] newLeftSiblingKeyPrefix =
        extractCommonPrefix(leftSiblingLargestLowerBound, leftSiblingSmallestUpperBound);
    assert newLeftSiblingKeyPrefix.length >= leftSiblingKeyPrefix.length;

    if (newLeftSiblingKeyPrefix.length > leftSiblingKeyPrefix.length) {
      final int leftSiblingDiff = newLeftSiblingKeyPrefix.length - leftSiblingKeyPrefix.length;

      leftSibling.shrink(0);

      final ArrayList<Bucket.Entry> entries = new ArrayList<>(leftSiblingSize);
      for (int i = 0; i < leftSiblingSize; i++) {
        entries.add(leftSibling.getEntry(i));
      }

      for (int i = 0; i < leftSiblingSize; i++) {
        final Bucket.Entry entry = entries.get(i);
        leftSibling.addNonLeafEntry(
            i,
            entry.leftChild,
            entry.rightChild,
            entry.key,
            leftSiblingDiff,
            entry.key.length - leftSiblingDiff);
      }
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    final byte[] bucketLargestLowerBound = leftSiblingSmallestUpperBound;
    final byte[] bucketSmallestUpperBound;

    if (parentItem.indexInsidePage > parentBucket.size() - 1) {
      final byte[] partialKey = parentBucket.getKey(parentItem.indexInsidePage + 1);
      bucketSmallestUpperBound = new byte[partialKey.length + parentKeyPrefix.length];

      System.arraycopy(parentKeyPrefix, 0, bucketSmallestUpperBound, 0, parentKeyPrefix.length);
      System.arraycopy(
          partialKey, 0, bucketSmallestUpperBound, parentKeyPrefix.length, partialKey.length);
    } else {
      bucketSmallestUpperBound = parentSmallestUpperBound;
    }

    final byte[] bucketPrefix =
        extractCommonPrefix(bucketLargestLowerBound, bucketSmallestUpperBound);
    assert bucketPrefix.length >= parentKeyPrefix.length;
    final int bucketDiff = bucketPrefix.length - parentKeyPrefix.length;

    final boolean result =
        bucket.addNonLeafEntry(
            0,
            bucketLeft,
            orhanPointer,
            partialBucketKey,
            bucketDiff,
            partialBucketKey.length - bucketDiff);
    assert result;

    final byte[] rightChildrenLargestLowerBound =
        new byte[partialBucketKey.length + parentKeyPrefix.length];
    System.arraycopy(parentKeyPrefix, 0, rightChildrenLargestLowerBound, 0, parentKeyPrefix.length);
    System.arraycopy(
        partialBucketKey,
        0,
        rightChildrenLargestLowerBound,
        parentKeyPrefix.length,
        partialBucketKey.length);

    @SuppressWarnings("UnnecessaryLocalVariable")
    final byte[] rightChildrenSmallestUpperBound = bucketSmallestUpperBound;

    return Optional.of(
        new NonLeafItemDeletionResult(
            rightChildrenLargestLowerBound, rightChildrenSmallestUpperBound, false));
  }

  private Optional<NonLeafItemDeletionResult> rotateNoneLeafLeftAndRemoveItem(
      final RemovalPathItem parentItem,
      final Bucket parentBucket,
      final byte[] parentKeyPrefix,
      final byte[] parentLargestLowerBound,
      final byte[] parentSmallestUpperBound,
      final Bucket bucket,
      final Bucket rightSibling,
      final byte[] rightSiblingPrefix,
      final int orphanPointer) {
    if (bucket.size() != 1 || rightSibling.size() <= 1) {
      throw new BinaryBTreeException("BTree is broken", this);
    }

    final byte[] partialBucketKey = parentBucket.getKey(parentItem.indexInsidePage);
    final byte[] partialSeparatorKey = rightSibling.getKey(0);

    final byte[] separatorKey;

    assert rightSiblingPrefix.length >= parentKeyPrefix.length;
    if (rightSiblingPrefix.length > parentKeyPrefix.length) {
      final int separatorKeyDiff = rightSiblingPrefix.length - parentKeyPrefix.length;
      separatorKey = new byte[partialSeparatorKey.length + separatorKeyDiff];

      System.arraycopy(
          rightSiblingPrefix, parentKeyPrefix.length, separatorKey, 0, separatorKeyDiff);
      System.arraycopy(
          partialSeparatorKey, 0, separatorKey, separatorKeyDiff, partialSeparatorKey.length);
    } else {
      separatorKey = partialSeparatorKey;
    }

    if (!parentBucket.updateKey(parentItem.indexInsidePage, separatorKey)) {
      return Optional.empty();
    }

    final int bucketRight = rightSibling.getLeft(0);

    bucket.removeNonLeafEntry(0, true);

    final byte[] bucketLargestLowerBound;
    if (parentItem.indexInsidePage > 0) {
      bucketLargestLowerBound = parentBucket.getKey(parentItem.indexInsidePage - 1);
    } else {
      bucketLargestLowerBound = parentLargestLowerBound;
    }

    final byte[] bucketSmallestUpperBound = new byte[separatorKey.length + parentKeyPrefix.length];

    System.arraycopy(parentKeyPrefix, 0, bucketSmallestUpperBound, 0, parentKeyPrefix.length);
    System.arraycopy(
        separatorKey, 0, bucketSmallestUpperBound, parentKeyPrefix.length, separatorKey.length);

    final byte[] bucketKeyPrefix =
        extractCommonPrefix(bucketLargestLowerBound, bucketSmallestUpperBound);
    assert bucketKeyPrefix.length >= parentKeyPrefix.length;
    final int bucketKeyDiff = bucketKeyPrefix.length - parentKeyPrefix.length;

    final boolean result =
        bucket.addNonLeafEntry(
            0,
            orphanPointer,
            bucketRight,
            partialBucketKey,
            bucketKeyDiff,
            partialBucketKey.length - bucketKeyDiff);
    assert result;

    rightSibling.removeNonLeafEntry(0, true);

    @SuppressWarnings("UnnecessaryLocalVariable")
    final byte[] rightSiblingLargestLowerBound = bucketSmallestUpperBound;
    final byte[] rightSiblingSmallestUpperBound;

    if (parentItem.indexInsidePage < bucket.size() - 1) {
      final byte[] partialKey = parentBucket.getKey(parentItem.indexInsidePage + 1);
      rightSiblingSmallestUpperBound = new byte[partialKey.length + parentKeyPrefix.length];

      System.arraycopy(
          parentKeyPrefix, 0, rightSiblingSmallestUpperBound, 0, parentKeyPrefix.length);
      System.arraycopy(
          partialKey, 0, rightSiblingSmallestUpperBound, parentKeyPrefix.length, partialKey.length);
    } else {
      rightSiblingSmallestUpperBound = parentSmallestUpperBound;
    }

    final byte[] newRightSiblingPrefix =
        extractCommonPrefix(rightSiblingLargestLowerBound, rightSiblingSmallestUpperBound);
    assert newRightSiblingPrefix.length >= rightSiblingPrefix.length;

    if (newRightSiblingPrefix.length > rightSiblingPrefix.length) {
      final int siblingDiff = newRightSiblingPrefix.length - rightSiblingPrefix.length;
      final int bucketSize = rightSibling.size();

      final ArrayList<Bucket.Entry> entries = new ArrayList<>(bucketSize);

      for (int i = 0; i < bucketSize; i++) {
        entries.add(rightSibling.getEntry(i));
      }

      rightSibling.shrink(0);
      for (int i = 0; i < bucketSize; i++) {
        final Bucket.Entry entry = entries.get(i);
        rightSibling.addNonLeafEntry(
            i,
            entry.leftChild,
            entry.rightChild,
            entry.key,
            siblingDiff,
            entry.key.length - siblingDiff);
      }
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    final byte[] leftChildLargestLowerBound = bucketLargestLowerBound;
    final byte[] leftChildSmallestUpperBound =
        new byte[partialBucketKey.length + parentKeyPrefix.length];
    System.arraycopy(parentKeyPrefix, 0, leftChildSmallestUpperBound, 0, parentKeyPrefix.length);
    System.arraycopy(
        partialBucketKey,
        0,
        leftChildSmallestUpperBound,
        parentKeyPrefix.length,
        partialBucketKey.length);

    return Optional.of(
        new NonLeafItemDeletionResult(
            leftChildLargestLowerBound, leftChildSmallestUpperBound, false));
  }

  private Optional<NonLeafItemDeletionResult> mergeNoneLeafWithRightSiblingAndDeleteItem(
      final OAtomicOperation atomicOperation,
      final RemovalPathItem parentItem,
      final Bucket parentBucket,
      final byte[] parentBucketPrefix,
      final Bucket bucket,
      final Bucket rightSibling,
      final byte[] rightSiblingPrefix,
      final int orphanPointer,
      final List<RemovalPathItem> path)
      throws IOException {

    if (rightSibling.size() != 1 || bucket.size() != 1) {
      throw new BinaryBTreeException("BTree is broken", this);
    }

    final byte[] partialKey = parentBucket.getKey(parentItem.indexInsidePage);
    final int rightChild = rightSibling.getLeft(0);

    assert rightSiblingPrefix.length >= parentBucketPrefix.length;
    final int siblingPrefixDiff = rightSiblingPrefix.length - parentBucketPrefix.length;

    final boolean result =
        rightSibling.addNonLeafEntry(
            0,
            orphanPointer,
            rightChild,
            partialKey,
            siblingPrefixDiff,
            partialKey.length - siblingPrefixDiff);
    assert result;

    final Optional<NonLeafItemDeletionResult> oDeletionResult =
        deleteFromNonLeafNode(
            atomicOperation, parentBucket, rightSibling, path.subList(0, path.size() - 1));
    if (oDeletionResult.isPresent()) {
      final NonLeafItemDeletionResult deletionResult = oDeletionResult.get();
      if (!deletionResult.mergedWithRoot) {
        final byte[] newRightSiblingPrefix =
            extractCommonPrefix(
                deletionResult.largestLowerBoundary, deletionResult.smallestUpperBoundary);

        assert newRightSiblingPrefix.length <= rightSiblingPrefix.length;

        if (newRightSiblingPrefix.length < rightSiblingPrefix.length) {
          final int newSiblingPrefixDiff = rightSiblingPrefix.length - newRightSiblingPrefix.length;

          final ArrayList<Bucket.Entry> entries = new ArrayList<>(2);

          entries.add(rightSibling.getEntry(0));
          entries.add(rightSibling.getEntry(1));

          rightSibling.shrink(0);

          for (int i = 0; i < 2; i++) {
            final Bucket.Entry entry = entries.get(i);

            final byte[] oldKey = entry.key;
            final byte[] newKey = new byte[newSiblingPrefixDiff + oldKey.length];

            System.arraycopy(
                rightSiblingPrefix, newRightSiblingPrefix.length, newKey, 0, newSiblingPrefixDiff);
            System.arraycopy(oldKey, 0, newKey, newSiblingPrefixDiff, oldKey.length);

            rightSibling.addNonLeafEntry(i, entry.leftChild, entry.rightChild, newKey);
          }
        }
      }

      addToFreeList(atomicOperation, bucket.getCacheEntry().getPageIndex());

      final byte[]
      return Optional.of(new NonLeafItemDeletionResult());
    }

    rightSibling.removeNonLeafEntry(0, partialKey.length, true);
    return Optional.empty();
  }

  private Optional<NonLeafItemDeletionResult> mergeNoneLeafWithLeftSiblingAndDeleteItem(
      final OAtomicOperation atomicOperation,
      final RemovalPathItem parentItem,
      final Bucket parentBucket,
      final byte[] parentBucketPrefix,
      final byte[] parentSmallestUpperBoundary,
      final Bucket bucket,
      final Bucket leftSibling,
      final byte[] leftSiblingPrefix,
      final int orphanPointer,
      final List<RemovalPathItem> path)
      throws IOException {

    if (leftSibling.size() != 1 || bucket.size() != 1) {
      throw new BinaryBTreeException("BTree is broken", this);
    }

    final byte[] partialKey = parentBucket.getKey(parentItem.indexInsidePage);
    assert parentBucketPrefix.length <= leftSiblingPrefix.length;
    final int partialKeyDiff = leftSiblingPrefix.length - parentBucketPrefix.length;

    final int leftChild = leftSibling.getRight(0);
    final boolean result =
        leftSibling.addNonLeafEntry(
            1,
            leftChild,
            orphanPointer,
            partialKey,
            partialKeyDiff,
            partialKey.length - partialKeyDiff);
    assert result;

    final Optional<NonLeafItemDeletionResult> oDeletionResult =
        deleteFromNonLeafNode(
            atomicOperation, parentBucket, leftSibling, path.subList(0, path.size() - 1));
    if (oDeletionResult.isPresent()) {
      final NonLeafItemDeletionResult deletionResult = oDeletionResult.get();
      if (!deletionResult.mergedWithRoot) {
        final byte[] newLeftSiblingPrefix =
            extractCommonPrefix(
                deletionResult.largestLowerBoundary, deletionResult.smallestUpperBoundary);
        assert newLeftSiblingPrefix.length <= leftSiblingPrefix.length;

        if (newLeftSiblingPrefix.length < leftSiblingPrefix.length) {
          assert leftSibling.size() == 2;

          final ArrayList<Bucket.Entry> entries = new ArrayList<>(2);
          entries.add(leftSibling.getEntry(0));
          entries.add(leftSibling.getEntry(1));

          leftSibling.shrink(0);

          final int prefixDiff = leftSiblingPrefix.length - newLeftSiblingPrefix.length;
          for (int i = 0; i < 2; i++) {
            final Bucket.Entry entry = entries.get(i);
            final byte[] oldKey = entry.key;
            final byte[] newKey = new byte[oldKey.length + prefixDiff];

            System.arraycopy(leftSiblingPrefix, newLeftSiblingPrefix.length, newKey, 0, prefixDiff);
            System.arraycopy(oldKey, 0, newKey, prefixDiff, oldKey.length);

            leftSibling.addNonLeafEntry(0, entry.leftChild, entry.rightChild, newKey);
          }
        }
      }

      addToFreeList(atomicOperation, bucket.getCacheEntry().getPageIndex());

      final byte[] rightChildLargestLowerBoundary =
          new byte[parentBucketPrefix.length + partialKey.length];

      System.arraycopy(
          parentBucketPrefix, 0, rightChildLargestLowerBoundary, 0, parentBucketPrefix.length);
      System.arraycopy(
          partialKey,
          0,
          rightChildLargestLowerBoundary,
          parentBucketPrefix.length,
          partialKey.length);

      @SuppressWarnings("UnnecessaryLocalVariable")
      final byte[] rightChildSmallestUpperBoundary = parentSmallestUpperBoundary;

      return Optional.of(
          new NonLeafItemDeletionResult(
              rightChildLargestLowerBoundary, rightChildSmallestUpperBoundary, false));
    }

    leftSibling.removeNonLeafEntry(1, partialKey.length, false);
    return Optional.empty();
  }

  private byte[] calculateLargestLowerBoundary(final Bucket parent, final int parentIndex,
                                               final boolean leftChild, final byte[] parentLargestLowerBoundary) {

  }

  private byte[] calculateSmallestUpperBoundary(final Bucket parent, final int parentIndex, final boolean leftChild,
                                                final byte[] parentSmallestUpperBoundary) {

  }

  private byte[] restoreKey(final byte[] keyPrefix, final byte[] partialKey) {

  }

  private byte[] extendKey(final byte[] parentKeyPrefix, final byte[] keyPrefix, final byte[] partialKey) {

  }

  private void addToFreeList(OAtomicOperation atomicOperation, int pageIndex) throws IOException {
    final OCacheEntry cacheEntry =
        loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
    try {
      final Bucket bucket = new Bucket(cacheEntry);

      final OCacheEntry entryPointEntry =
          loadPageForWrite(atomicOperation, fileId, ENTRY_POINT_INDEX, false, true);
      try {
        final EntryPoint entryPoint = new EntryPoint(entryPointEntry);

        final int freeListHead = entryPoint.getFreeListHead();
        entryPoint.setFreeListHead(pageIndex);
        bucket.setNextFreeListPage(freeListHead);
      } finally {
        releasePageFromWrite(atomicOperation, entryPointEntry);
      }
    } finally {
      releasePageFromWrite(atomicOperation, cacheEntry);
    }
  }

  void assertFreePages() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

        final Set<Integer> pages = new HashSet<>();
        final int filledUpTo = (int) getFilledUpTo(atomicOperation, fileId);

        for (int i = 2; i < filledUpTo; i++) {
          pages.add(i);
        }

        removeUsedPages((int) ROOT_INDEX, pages, atomicOperation);

        removePagesStoredInFreeList(atomicOperation, pages, filledUpTo);

        assert pages.isEmpty();

      } finally {
        releaseSharedLock();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new BinaryBTreeException("Error during checking  of btree with name " + getName(), this),
          e);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  private void removePagesStoredInFreeList(
      OAtomicOperation atomicOperation, Set<Integer> pages, int filledUpTo) throws IOException {
    final int freeListHead;
    final OCacheEntry entryCacheEntry =
        loadPageForRead(atomicOperation, fileId, ENTRY_POINT_INDEX, true);
    try {
      final EntryPoint entryPoint = new EntryPoint(entryCacheEntry);
      assert entryPoint.getPagesSize() == filledUpTo - 1;
      freeListHead = entryPoint.getFreeListHead();
    } finally {
      releasePageFromRead(atomicOperation, entryCacheEntry);
    }

    int freePageIndex = freeListHead;
    while (freePageIndex >= 0) {
      pages.remove(freePageIndex);

      final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, freePageIndex, true);
      try {
        final Bucket bucket = new Bucket(cacheEntry);
        freePageIndex = bucket.getNextFreeListPage();
        pages.remove(freePageIndex);
      } finally {
        releasePageFromRead(atomicOperation, cacheEntry);
      }
    }
  }

  private void removeUsedPages(
      final int pageIndex, final Set<Integer> pages, final OAtomicOperation atomicOperation)
      throws IOException {
    final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, true);
    try {
      final Bucket bucket = new Bucket(cacheEntry);
      if (bucket.isLeaf()) {
        return;
      }

      final int bucketSize = bucket.size();
      final List<Integer> pagesToExplore = new ArrayList<>(bucketSize);

      if (bucketSize > 0) {
        final int leftPage = bucket.getLeft(0);
        pages.remove(leftPage);
        pagesToExplore.add(leftPage);

        for (int i = 0; i < bucketSize; i++) {
          final int rightPage = bucket.getRight(i);
          pages.remove(rightPage);
          pagesToExplore.add(rightPage);
        }
      }

      for (final int pageToExplore : pagesToExplore) {
        removeUsedPages(pageToExplore, pages, atomicOperation);
      }
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }

  private final class SpliteratorForward implements Spliterator<ORawPair<byte[], ORID>> {
    private final byte[] fromKey;
    private final byte[] toKey;
    private final boolean fromKeyInclusive;
    private final boolean toKeyInclusive;

    private int pageIndex = -1;
    private int itemIndex = -1;

    private OLogSequenceNumber lastLSN = null;

    private final List<ORawPair<byte[], ORID>> dataCache = new ArrayList<>();
    private Iterator<ORawPair<byte[], ORID>> cacheIterator = Collections.emptyIterator();

    private SpliteratorForward(
        final byte[] fromKey,
        final byte[] toKey,
        final boolean fromKeyInclusive,
        final boolean toKeyInclusive) {
      this.fromKey = fromKey;
      this.toKey = toKey;

      this.toKeyInclusive = toKeyInclusive;
      this.fromKeyInclusive = fromKeyInclusive;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<byte[], ORID>> action) {
      if (cacheIterator == null) {
        return false;
      }

      if (cacheIterator.hasNext()) {
        action.accept(cacheIterator.next());
        return true;
      }

      fetchNextCachePortion();

      cacheIterator = dataCache.iterator();

      if (cacheIterator.hasNext()) {
        action.accept(cacheIterator.next());
        return true;
      }

      cacheIterator = null;

      return false;
    }

    private void fetchNextCachePortion() {
      final byte[] lastKey;
      if (!dataCache.isEmpty()) {
        lastKey = dataCache.get(dataCache.size() - 1).first;
      } else {
        lastKey = null;
      }

      dataCache.clear();
      cacheIterator = Collections.emptyIterator();

      atomicOperationsManager.acquireReadLock(BinaryBTree.this);
      try {
        acquireSharedLock();
        try {
          final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
          if (pageIndex > -1) {
            if (readKeysFromBuckets(atomicOperation)) {
              return;
            }
          }

          // this can only happen if page LSN does not equal to stored LSN or index of current
          // iterated page equals to -1
          // so we only started iteration
          if (dataCache.isEmpty()) {
            // iteration just started
            if (lastKey == null) {
              if (this.fromKey != null) {
                final BucketSearchResult searchResult = findBucket(fromKey, atomicOperation);
                pageIndex = (int) searchResult.pageIndex;

                if (searchResult.itemIndex >= 0) {
                  if (fromKeyInclusive) {
                    itemIndex = searchResult.itemIndex;
                  } else {
                    itemIndex = searchResult.itemIndex + 1;
                  }
                } else {
                  itemIndex = -searchResult.itemIndex - 1;
                }
              } else {
                final Optional<BucketSearchResult> bucketSearchResult = firstItem(atomicOperation);
                if (bucketSearchResult.isPresent()) {
                  final BucketSearchResult searchResult = bucketSearchResult.get();
                  pageIndex = (int) searchResult.pageIndex;
                  itemIndex = searchResult.itemIndex;
                } else {
                  return;
                }
              }

            } else {
              final BucketSearchResult bucketSearchResult = findBucket(lastKey, atomicOperation);

              pageIndex = (int) bucketSearchResult.pageIndex;
              if (bucketSearchResult.itemIndex >= 0) {
                itemIndex = bucketSearchResult.itemIndex + 1;
              } else {
                itemIndex = -bucketSearchResult.itemIndex - 1;
              }
            }
            lastLSN = null;
            readKeysFromBuckets(atomicOperation);
          }
        } finally {
          releaseSharedLock();
        }
      } catch (final IOException e) {
        throw OException.wrapException(
            new BinaryBTreeException("Error during element iteration", BinaryBTree.this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(BinaryBTree.this);
      }
    }

    private boolean readKeysFromBuckets(OAtomicOperation atomicOperation) throws IOException {
      OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      try {
        Bucket bucket = new Bucket(cacheEntry);
        if (lastLSN == null || bucket.getLSN().equals(lastLSN)) {
          while (true) {
            int bucketSize = bucket.size();
            if (itemIndex >= bucketSize) {
              pageIndex = (int) bucket.getRightSibling();

              if (pageIndex < 0) {
                return true;
              }

              itemIndex = 0;
              releasePageFromRead(atomicOperation, cacheEntry);

              cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
              bucket = new Bucket(cacheEntry);

              bucketSize = bucket.size();
            }

            lastLSN = bucket.getLSN();

            for (; itemIndex < bucketSize && dataCache.size() < spliteratorCacheSize; itemIndex++) {
              final Bucket.Entry entry = bucket.getEntry(itemIndex);

              if (toKey != null) {
                if (toKeyInclusive) {
                  if (COMPARATOR.compare(entry.key, toKey) > 0) {
                    return true;
                  }
                } else if (COMPARATOR.compare(entry.key, toKey) >= 0) {
                  return true;
                }
              }

              dataCache.add(new ORawPair<>(entry.key, entry.value));
            }

            if (dataCache.size() >= spliteratorCacheSize) {
              return true;
            }
          }
        }
      } finally {
        releasePageFromRead(atomicOperation, cacheEntry);
      }

      return false;
    }

    @Override
    public Spliterator<ORawPair<byte[], ORID>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
      return SORTED | NONNULL | ORDERED;
    }

    @Override
    public Comparator<? super ORawPair<byte[], ORID>> getComparator() {
      return (pairOne, pairTwo) -> COMPARATOR.compare(pairOne.first, pairTwo.first);
    }
  }

  private final class SpliteratorBackward implements Spliterator<ORawPair<byte[], ORID>> {
    private final byte[] fromKey;
    private final byte[] toKey;
    private final boolean fromKeyInclusive;
    private final boolean toKeyInclusive;

    private int pageIndex = -1;
    private int itemIndex = -1;

    private OLogSequenceNumber lastLSN = null;

    private final List<ORawPair<byte[], ORID>> dataCache = new ArrayList<>();
    private Iterator<ORawPair<byte[], ORID>> cacheIterator = Collections.emptyIterator();

    private SpliteratorBackward(
        final byte[] fromKey,
        final byte[] toKey,
        final boolean fromKeyInclusive,
        final boolean toKeyInclusive) {
      this.fromKey = fromKey;
      this.toKey = toKey;
      this.fromKeyInclusive = fromKeyInclusive;
      this.toKeyInclusive = toKeyInclusive;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<byte[], ORID>> action) {
      if (cacheIterator == null) {
        return false;
      }

      if (cacheIterator.hasNext()) {
        action.accept(cacheIterator.next());
        return true;
      }

      fetchNextCachePortion();

      cacheIterator = dataCache.iterator();

      if (cacheIterator.hasNext()) {
        action.accept(cacheIterator.next());
        return true;
      }

      cacheIterator = null;

      return false;
    }

    private void fetchNextCachePortion() {
      final byte[] lastKey;
      if (dataCache.isEmpty()) {
        lastKey = null;
      } else {
        lastKey = dataCache.get(dataCache.size() - 1).first;
      }

      dataCache.clear();
      cacheIterator = Collections.emptyIterator();

      atomicOperationsManager.acquireReadLock(BinaryBTree.this);
      try {
        acquireSharedLock();
        try {
          final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
          if (pageIndex > -1) {
            if (readKeysFromBuckets(atomicOperation)) {
              return;
            }
          }

          // this can only happen if page LSN does not equal to stored LSN or index of current
          // iterated page equals to -1
          // so we only started iteration
          if (dataCache.isEmpty()) {
            // iteration just started
            if (lastKey == null) {
              if (this.toKey != null) {
                final BucketSearchResult searchResult = findBucket(toKey, atomicOperation);
                pageIndex = (int) searchResult.pageIndex;

                if (searchResult.itemIndex >= 0) {
                  if (toKeyInclusive) {
                    itemIndex = searchResult.itemIndex;
                  } else {
                    itemIndex = searchResult.itemIndex - 1;
                  }
                } else {
                  itemIndex = -searchResult.itemIndex - 2;
                }
              } else {
                final Optional<BucketSearchResult> bucketSearchResult = lastItem(atomicOperation);
                if (bucketSearchResult.isPresent()) {
                  final BucketSearchResult searchResult = bucketSearchResult.get();
                  pageIndex = (int) searchResult.pageIndex;
                  itemIndex = searchResult.itemIndex;
                } else {
                  return;
                }
              }

            } else {
              final BucketSearchResult bucketSearchResult = findBucket(lastKey, atomicOperation);

              pageIndex = (int) bucketSearchResult.pageIndex;
              if (bucketSearchResult.itemIndex >= 0) {
                itemIndex = bucketSearchResult.itemIndex - 1;
              } else {
                itemIndex = -bucketSearchResult.itemIndex - 2;
              }
            }
            lastLSN = null;
            readKeysFromBuckets(atomicOperation);
          }
        } finally {
          releaseSharedLock();
        }
      } catch (final IOException e) {
        throw OException.wrapException(
            new BinaryBTreeException("Error during element iteration", BinaryBTree.this), e);
      } finally {
        atomicOperationsManager.releaseReadLock(BinaryBTree.this);
      }
    }

    private boolean readKeysFromBuckets(OAtomicOperation atomicOperation) throws IOException {
      OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      try {
        Bucket bucket = new Bucket(cacheEntry);
        if (lastLSN == null || bucket.getLSN().equals(lastLSN)) {
          while (true) {
            if (itemIndex < 0) {
              pageIndex = (int) bucket.getLeftSibling();

              if (pageIndex < 0) {
                return true;
              }

              releasePageFromRead(atomicOperation, cacheEntry);

              cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
              bucket = new Bucket(cacheEntry);
              final int bucketSize = bucket.size();
              itemIndex = bucketSize - 1;
            }

            lastLSN = bucket.getLSN();

            for (; itemIndex >= 0 && dataCache.size() < spliteratorCacheSize; itemIndex--) {
              Bucket.Entry entry = bucket.getEntry(itemIndex);

              if (fromKey != null) {
                if (fromKeyInclusive) {
                  if (COMPARATOR.compare(entry.key, fromKey) < 0) {
                    return true;
                  }
                } else if (COMPARATOR.compare(entry.key, fromKey) <= 0) {
                  return true;
                }
              }

              //noinspection ObjectAllocationInLoop
              dataCache.add(new ORawPair<>(entry.key, entry.value));
            }

            if (dataCache.size() >= spliteratorCacheSize) {
              return true;
            }
          }
        }
      } finally {
        releasePageFromRead(atomicOperation, cacheEntry);
      }

      return false;
    }

    @Override
    public Spliterator<ORawPair<byte[], ORID>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
      return SORTED | NONNULL | ORDERED;
    }

    @Override
    public Comparator<? super ORawPair<byte[], ORID>> getComparator() {
      return (pairOne, pairTwo) -> -COMPARATOR.compare(pairOne.first, pairTwo.first);
    }
  }

  private static final class SplitBucketResult {
    private final long pageIndex;
    private final int insertionIndex;
    private final byte[] largestLowerBound;

    private final byte[] smallestUpperBound;
    private final byte[] keyPrefix;

    private SplitBucketResult(
        final long pageIndex,
        final int insertionIndex,
        final byte[] largestLowerBound,
        final byte[] smallestUpperBound,
        final byte[] keyPrefix) {
      this.pageIndex = pageIndex;
      this.insertionIndex = insertionIndex;
      this.largestLowerBound = largestLowerBound;
      this.smallestUpperBound = smallestUpperBound;
      this.keyPrefix = keyPrefix;
    }
  }

  private static final class NonLeafItemDeletionResult {
    private final byte[] largestLowerBoundary;
    private final byte[] smallestUpperBoundary;
    private final boolean mergedWithRoot;

    private NonLeafItemDeletionResult(
        byte[] largestLowerBoundary, byte[] smallestUpperBoundary, boolean mergedWithRoot) {
      this.largestLowerBoundary = largestLowerBoundary;
      this.smallestUpperBoundary = smallestUpperBoundary;
      this.mergedWithRoot = mergedWithRoot;
    }
  }

  private static final class PagePathItemUnit {
    private final long pageIndex;
    private final int itemIndex;

    private PagePathItemUnit(final long pageIndex, final int itemIndex) {
      this.pageIndex = pageIndex;
      this.itemIndex = itemIndex;
    }
  }

  private static final class BucketSearchResult {
    private final int itemIndex;
    private final long pageIndex;

    private BucketSearchResult(final int itemIndex, final long pageIndex) {
      this.itemIndex = itemIndex;
      this.pageIndex = pageIndex;
    }
  }

  private static final class UpdateBucketSearchResult {
    private final List<Integer> insertionIndexes;
    private final ArrayList<byte[]> keyPrefixes;
    private final ArrayList<byte[]> largestLowerBounds;
    private final ArrayList<byte[]> smallestUpperBounds;

    private final ArrayList<Long> path;
    private final int itemIndex;

    private UpdateBucketSearchResult(
        final List<Integer> insertionIndexes,
        ArrayList<byte[]> keyPrefixes,
        ArrayList<byte[]> largestLowerBounds,
        ArrayList<byte[]> smallestUpperBounds,
        final ArrayList<Long> path,
        final int itemIndex) {
      this.insertionIndexes = insertionIndexes;
      this.keyPrefixes = keyPrefixes;
      this.largestLowerBounds = largestLowerBounds;
      this.smallestUpperBounds = smallestUpperBounds;
      this.path = path;
      this.itemIndex = itemIndex;
    }

    private long getLastPathItem() {
      return path.get(path.size() - 1);
    }
  }

  private static final class RemoveSearchResult {
    private final long leafPageIndex;
    private final int leafEntryPageIndex;
    private final List<RemovalPathItem> path;

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final ArrayList<byte[]> keyPrefixes;

    private RemoveSearchResult(
        long leafPageIndex,
        int leafEntryPageIndex,
        List<RemovalPathItem> path,
        ArrayList<byte[]> keyPrefixes) {
      this.leafPageIndex = leafPageIndex;
      this.leafEntryPageIndex = leafEntryPageIndex;
      this.path = path;
      this.keyPrefixes = keyPrefixes;
    }
  }

  private static final class RemovalPathItem {
    private final long pageIndex;
    private final int indexInsidePage;
    private final boolean leftChild;

    private RemovalPathItem(long pageIndex, int indexInsidePage, boolean leftChild) {
      this.pageIndex = pageIndex;
      this.indexInsidePage = indexInsidePage;
      this.leftChild = leftChild;
    }
  }
}
