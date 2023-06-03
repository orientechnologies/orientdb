package com.orientechnologies.orient.core.storage.index.nkbtree.binarybtree;

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

                        final byte[] serializedValue =
                                new byte[OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE];
                        OShortSerializer.INSTANCE.serializeNative(
                                (short) value.getClusterId(), serializedValue, 0);
                        OLongSerializer.INSTANCE.serializeNative(
                                value.getClusterPosition(), serializedValue, OShortSerializer.SHORT_SIZE);

                        int insertionIndex;
                        final int sizeDiff;
                        if (bucketSearchResult.itemIndex >= 0) {
                            assert oldRawValue != null;

                            if (oldRawValue.length == serializedValue.length) {
                                keyBucket.updateValue(bucketSearchResult.itemIndex, serializedValue, key.length);
                                releasePageFromWrite(atomicOperation, keyBucketCacheEntry);
                                return true;
                            } else {
                                keyBucket.removeLeafEntry(bucketSearchResult.itemIndex, key.length);
                                insertionIndex = bucketSearchResult.itemIndex;
                                sizeDiff = 0;
                            }
                        } else {
                            insertionIndex = -bucketSearchResult.itemIndex - 1;
                            sizeDiff = 1;
                        }

                        while (!keyBucket.addLeafEntry(insertionIndex, key, serializedValue)) {
                            bucketSearchResult =
                                    splitBucket(
                                            keyBucket,
                                            keyBucketCacheEntry,
                                            bucketSearchResult.path,
                                            bucketSearchResult.insertionIndexes,
                                            insertionIndex,
                                            key,
                                            atomicOperation);

                            insertionIndex = bucketSearchResult.itemIndex;

                            final long pageIndex = bucketSearchResult.getLastPathItem();

                            if (pageIndex != keyBucketCacheEntry.getPageIndex()) {
                                releasePageFromWrite(atomicOperation, keyBucketCacheEntry);

                                keyBucketCacheEntry =
                                        loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
                            }

                            //noinspection ObjectAllocationInLoop
                            keyBucket = new Bucket(keyBucketCacheEntry);
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

    private UpdateBucketSearchResult splitBucket(
            final Bucket bucketToSplit,
            final OCacheEntry entryToSplit,
            final List<Long> path,
            final List<Integer> itemPointers,
            final int keyIndex,
            final byte[] key,
            final OAtomicOperation atomicOperation)
            throws IOException {
        final boolean splitLeaf = bucketToSplit.isLeaf();
        final int bucketSize = bucketToSplit.size();

        final int indexToSplit;

        byte[] separationKey = null;
        if (splitLeaf) {
            final int median = bucketSize >>> 1;
            int minLen = Integer.MAX_VALUE;
            int minIndex = Integer.MIN_VALUE;

            for (int i = -1; i < 2; i++) {
                int index = median + i;
                final byte[] keyOne = keyIndex == index ? key : bucketToSplit.getKey(index - 1);
                final byte[] keyTwo = bucketToSplit.getKey(index);

                final int commonLen = Math.min(keyOne.length, keyTwo.length);

                boolean keyFound = false;
                for (int k = 0; k < commonLen; k++) {
                    if (keyOne[k] < keyTwo[k]) {
                        if (minLen > k + 1) {
                            minLen = k + 1;
                            minIndex = index;
                        }

                        keyFound = true;
                        break;
                    }
                }

                if (!keyFound && minLen > commonLen + 1) {
                    assert keyOne.length > keyTwo.length;

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
        final List<byte[]> rightEntries = new ArrayList<>(indexToSplit);

        // we remove the separation key from the child in case of non-leaf node that is why skip first
        // entry
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
            final List<Long> path,
            final List<Integer> itemPointers,
            final int keyIndex,
            final long pageIndex,
            final Bucket bucketToSplit,
            final boolean splitLeaf,
            final int indexToSplit,
            final byte[] separationKey,
            final List<byte[]> rightEntries,
            final OAtomicOperation atomicOperation)
            throws IOException {

        final OCacheEntry rightBucketEntry = allocateNewPage(atomicOperation);
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
                int insertionIndex = itemPointers.get(itemPointers.size() - 2);
                while (!parentBucket.addNonLeafEntry(
                        insertionIndex, (int) pageIndex, rightBucketEntry.getPageIndex(), separationKey)) {
                    final UpdateBucketSearchResult bucketSearchResult =
                            splitBucket(
                                    parentBucket,
                                    parentCacheEntry,
                                    path.subList(0, path.size() - 1),
                                    itemPointers.subList(0, itemPointers.size() - 1),
                                    insertionIndex,
                                    separationKey,
                                    atomicOperation);

                    parentIndex = bucketSearchResult.getLastPathItem();
                    insertionIndex = bucketSearchResult.itemIndex;

                    if (parentIndex != parentCacheEntry.getPageIndex()) {
                        releasePageFromWrite(atomicOperation, parentCacheEntry);

                        parentCacheEntry = loadPageForWrite(atomicOperation, fileId, parentIndex, false, true);
                    }

                    //noinspection ObjectAllocationInLoop
                    parentBucket = new Bucket(parentCacheEntry);
                }

            } finally {
                releasePageFromWrite(atomicOperation, parentCacheEntry);
            }

        } finally {
            releasePageFromWrite(atomicOperation, rightBucketEntry);
        }

        final ArrayList<Long> resultPath = new ArrayList<>(path.subList(0, path.size() - 1));
        final ArrayList<Integer> resultItemPointers =
                new ArrayList<>(itemPointers.subList(0, itemPointers.size() - 1));

        if (splitLeaf) {
            if (keyIndex <= indexToSplit) {
                resultPath.add(pageIndex);
                resultItemPointers.add(keyIndex);

                return new UpdateBucketSearchResult(resultItemPointers, resultPath, keyIndex);
            }

            // key to insert is bigger than separation key and is going to be inserted into the right
            // bucket
            final int parentIndex = resultItemPointers.size() - 1;
            resultItemPointers.set(parentIndex, resultItemPointers.get(parentIndex) + 1);
            resultPath.add((long) rightBucketEntry.getPageIndex());

            resultItemPointers.add(keyIndex - indexToSplit);
            return new UpdateBucketSearchResult(resultItemPointers, resultPath, keyIndex - indexToSplit);
        }

        // key to insert is smaller than separation key, entry is going to be inserted into the left
        // bucket
        if (keyIndex <= indexToSplit) {
            resultPath.add(pageIndex);
            resultItemPointers.add(keyIndex);

            return new UpdateBucketSearchResult(resultItemPointers, resultPath, keyIndex);
        }

        // key to insert is bigger than separation key and is going to be inserted into the right bucket
        final int parentIndex = resultItemPointers.size() - 1;
        resultItemPointers.set(parentIndex, resultItemPointers.get(parentIndex) + 1);
        resultPath.add((long) rightBucketEntry.getPageIndex());

        resultItemPointers.add(keyIndex - indexToSplit - 1);
        return new UpdateBucketSearchResult(
                resultItemPointers, resultPath, keyIndex - indexToSplit - 1);
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

    private UpdateBucketSearchResult splitRootBucket(
            final int keyIndex,
            final OCacheEntry bucketEntry,
            Bucket bucketToSplit,
            final boolean splitLeaf,
            final int indexToSplit,
            final byte[] separationKey,
            final List<byte[]> rightEntries,
            final OAtomicOperation atomicOperation)
            throws IOException {
        final List<byte[]> leftEntries = new ArrayList<>(indexToSplit);

        for (int i = 0; i < indexToSplit; i++) {
            leftEntries.add(bucketToSplit.getRawEntry(i));
        }

        final OCacheEntry leftBucketEntry;
        final OCacheEntry rightBucketEntry;

        leftBucketEntry = allocateNewPage(atomicOperation);
        rightBucketEntry = allocateNewPage(atomicOperation);

        try {
            final Bucket newLeftBucket = new Bucket(leftBucketEntry);
            newLeftBucket.init(splitLeaf);
            newLeftBucket.addAll(leftEntries);

            if (splitLeaf) {
                newLeftBucket.setRightSibling(rightBucketEntry.getPageIndex());
            }

        } finally {
            releasePageFromWrite(atomicOperation, leftBucketEntry);
        }

        try {
            final Bucket newRightBucket = new Bucket(rightBucketEntry);
            newRightBucket.init(splitLeaf);
            newRightBucket.addAll(rightEntries);

            if (splitLeaf) {
                newRightBucket.setLeftSibling(leftBucketEntry.getPageIndex());
            }
        } finally {
            releasePageFromWrite(atomicOperation, rightBucketEntry);
        }

        bucketToSplit = new Bucket(bucketEntry);
        bucketToSplit.shrink(0);
        if (splitLeaf) {
            bucketToSplit.switchBucketType();
        }
        bucketToSplit.addNonLeafEntry(
                0, leftBucketEntry.getPageIndex(), rightBucketEntry.getPageIndex(), separationKey);

        final ArrayList<Long> resultPath = new ArrayList<>(8);
        resultPath.add(ROOT_INDEX);

        final ArrayList<Integer> itemPointers = new ArrayList<>(8);

        if (splitLeaf) {
            if (keyIndex <= indexToSplit) {
                itemPointers.add(-1);
                itemPointers.add(keyIndex);

                resultPath.add((long) leftBucketEntry.getPageIndex());
                return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex);
            }

            resultPath.add((long) rightBucketEntry.getPageIndex());
            itemPointers.add(0);

            itemPointers.add(keyIndex - indexToSplit);
            return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex - indexToSplit);
        }

        if (keyIndex <= indexToSplit) {
            itemPointers.add(-1);
            itemPointers.add(keyIndex);

            resultPath.add((long) leftBucketEntry.getPageIndex());
            return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex);
        }

        resultPath.add((long) rightBucketEntry.getPageIndex());
        itemPointers.add(0);

        itemPointers.add(keyIndex - indexToSplit - 1);
        return new UpdateBucketSearchResult(itemPointers, resultPath, keyIndex - indexToSplit - 1);
    }

    private UpdateBucketSearchResult findBucketForUpdate(
            final byte[] key, final OAtomicOperation atomicOperation) throws IOException {
        long pageIndex = ROOT_INDEX;

        final ArrayList<Long> path = new ArrayList<>(8);
        final ArrayList<Integer> itemIndexes = new ArrayList<>(8);

        while (true) {
            if (path.size() > maxPathLength) {
                throw new BinaryBTreeException(
                        "We reached max level of depth of binary BTree but still found nothing,"
                                + " seems like tree is in corrupted state. You should rebuild index related to given query.",
                        this);
            }

            path.add(pageIndex);
            final OCacheEntry bucketEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
            try {
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
            } finally {
                releasePageFromRead(atomicOperation, bucketEntry);
            }
        }
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

    private boolean deleteFromNonLeafNode(
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

    private boolean rotateNoneLeafRightAndRemoveItem(
            final RemovalPathItem parentItem,
            final Bucket parentBucket,
            final Bucket bucket,
            final Bucket leftSibling,
            final int orhanPointer) {
        if (bucket.size() != 1 || leftSibling.size() <= 1) {
            throw new BinaryBTreeException("BTree is broken", this);
        }

        final byte[] bucketKey = parentBucket.getKey(parentItem.indexInsidePage);
        final int leftSiblingSize = leftSibling.size();
        final byte[] separatorKey = leftSibling.getKey(leftSiblingSize - 1);

        if (!parentBucket.updateKey(parentItem.indexInsidePage, separatorKey)) {
            return false;
        }
        final int bucketLeft = leftSibling.getRight(leftSiblingSize - 1);

        leftSibling.removeNonLeafEntry(leftSiblingSize - 1, false);
        bucket.removeNonLeafEntry(0, true);
        final boolean result = bucket.addNonLeafEntry(0, bucketLeft, orhanPointer, bucketKey);
        assert result;

        return true;
    }

    private boolean rotateNoneLeafLeftAndRemoveItem(
            final RemovalPathItem parentItem,
            final Bucket parentBucket,
            final Bucket bucket,
            final Bucket rightSibling,
            final int orphanPointer) {
        if (bucket.size() != 1 || rightSibling.size() <= 1) {
            throw new BinaryBTreeException("BTree is broken", this);
        }

        final byte[] bucketKey = parentBucket.getKey(parentItem.indexInsidePage);

        final byte[] separatorKey = rightSibling.getKey(0);

        if (!parentBucket.updateKey(parentItem.indexInsidePage, separatorKey)) {
            return false;
        }

        final int bucketRight = rightSibling.getLeft(0);

        bucket.removeNonLeafEntry(0, true);
        final boolean result = bucket.addNonLeafEntry(0, orphanPointer, bucketRight, bucketKey);
        assert result;

        rightSibling.removeNonLeafEntry(0, true);
        return true;
    }

    private boolean mergeNoneLeafWithRightSiblingAndDeleteItem(
            final OAtomicOperation atomicOperation,
            final RemovalPathItem parentItem,
            final Bucket parentBucket,
            final Bucket bucket,
            final Bucket rightSibling,
            final int orphanPointer,
            final List<RemovalPathItem> path)
            throws IOException {

        if (rightSibling.size() != 1 || bucket.size() != 1) {
            throw new BinaryBTreeException("BTree is broken", this);
        }

        final byte[] leftKey = parentBucket.getKey(parentItem.indexInsidePage);
        final int rightChild = rightSibling.getLeft(0);

        final boolean result = rightSibling.addNonLeafEntry(0, orphanPointer, rightChild, leftKey);
        assert result;

        if (deleteFromNonLeafNode(
                atomicOperation, parentBucket, rightSibling, path.subList(0, path.size() - 1))) {
            addToFreeList(atomicOperation, bucket.getCacheEntry().getPageIndex());

            return true;
        }

        rightSibling.removeNonLeafEntry(0, leftKey.length, true);
        return false;
    }

    private boolean mergeNoneLeafWithLeftSiblingAndDeleteItem(
            final OAtomicOperation atomicOperation,
            final RemovalPathItem parentItem,
            final Bucket parentBucket,
            final Bucket bucket,
            final Bucket leftSibling,
            final int orphanPointer,
            final List<RemovalPathItem> path)
            throws IOException {

        if (leftSibling.size() != 1 || bucket.size() != 1) {
            throw new BinaryBTreeException("BTree is broken", this);
        }

        final byte[] rightKey = parentBucket.getKey(parentItem.indexInsidePage);
        final int leftChild = leftSibling.getRight(0);
        final boolean result = leftSibling.addNonLeafEntry(1, leftChild, orphanPointer, rightKey);
        assert result;

        if (deleteFromNonLeafNode(
                atomicOperation, parentBucket, leftSibling, path.subList(0, path.size() - 1))) {
            addToFreeList(atomicOperation, bucket.getCacheEntry().getPageIndex());

            return true;
        }

        leftSibling.removeNonLeafEntry(1, rightKey.length, false);
        return false;
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

    private Optional<RemoveSearchResult> findBucketForRemove(
            final byte[] key, final OAtomicOperation atomicOperation) throws IOException {

        final ArrayList<RemovalPathItem> path = new ArrayList<>(8);

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

                final int index = bucket.find(key);

                if (bucket.isLeaf()) {
                    if (index < 0) {
                        return Optional.empty();
                    }

                    return Optional.of(new RemoveSearchResult(pageIndex, index, path));
                }

                if (index >= 0) {
                    path.add(new RemovalPathItem(pageIndex, index, false));

                    pageIndex = bucket.getRight(index);
                } else {
                    final int insertionIndex = -index - 1;
                    if (insertionIndex >= bucket.size()) {
                        path.add(new RemovalPathItem(pageIndex, insertionIndex - 1, false));

                        pageIndex = bucket.getRight(insertionIndex - 1);
                    } else {
                        path.add(new RemovalPathItem(pageIndex, insertionIndex, true));

                        pageIndex = bucket.getLeft(insertionIndex);
                    }
                }
            } finally {
                releasePageFromRead(atomicOperation, bucketEntry);
            }
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
                    // so, we only started iteration
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
                    // so, we only started iteration
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
        private final ArrayList<Long> path;
        private final int itemIndex;

        private UpdateBucketSearchResult(
                final List<Integer> insertionIndexes, final ArrayList<Long> path, final int itemIndex) {
            this.insertionIndexes = insertionIndexes;
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

        private RemoveSearchResult(
                long leafPageIndex, int leafEntryPageIndex, List<RemovalPathItem> path) {
            this.leafPageIndex = leafPageIndex;
            this.leafEntryPageIndex = leafEntryPageIndex;
            this.path = path;
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