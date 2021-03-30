package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.sizemap;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

public class Bucket extends ODurablePage {

  private static final int ENTRY_SIZE = Integer.SIZE / 8;
  private static final int PAGE_SIZE =
      OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  private static final int SIZE_OFFSET = NEXT_FREE_POSITION;
  private static final int ENTRIES_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  public static final int MAX_BUCKET_SIZE = (PAGE_SIZE - ENTRIES_OFFSET) / ENTRY_SIZE;

  public Bucket(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() {
    setIntValue(SIZE_OFFSET, 0);
  }

  public int addEntry(final int entryIndex) {
    final int index;
    final int entryPosition;

    final int currentSize = getIntValue(SIZE_OFFSET);
    if (entryIndex < 0) {

      if (currentSize >= MAX_BUCKET_SIZE) {
        return -1;
      }

      setIntValue(SIZE_OFFSET, currentSize + 1);
      index = currentSize;
      entryPosition = ENTRIES_OFFSET + index * ENTRY_SIZE;
    } else {
      if (entryIndex >= currentSize) {
        throw new OStorageException(
            "Bucket is filled up to " + currentSize + " but requested index is " + entryIndex);
      }

      index = entryIndex;
      entryPosition = ENTRIES_OFFSET + index * ENTRY_SIZE;

      if (getIntValue(entryPosition) >= 0) {
        throw new OStorageException(
            "RidBag for the bucket with index "
                + getPageIndex()
                + " and local index "
                + entryIndex
                + " is not deleted and can not be reused");
      }
    }

    setIntValue(entryPosition, 0);
    return index;
  }

  public void incrementSize(int ridBagId) {
    final int bucketSize = getIntValue(SIZE_OFFSET);
    if (ridBagId >= bucketSize) {
      throw new OStorageException(
          "Bucket is filled up to " + bucketSize + " but requested index is " + bucketSize);
    }

    final int entryPosition = ENTRIES_OFFSET + ridBagId * ENTRY_SIZE;
    final int currentSize = getIntValue(entryPosition);
    if (currentSize < 0) {
      throw new OStorageException("RidBag is deleted and can not be used");
    }

    setIntValue(entryPosition, currentSize + 1);
  }

  public void decrementSize(int ridBagId) {
    final int bucketSize = getIntValue(SIZE_OFFSET);
    if (ridBagId >= bucketSize) {
      throw new OStorageException(
          "Bucket is filled up to " + bucketSize + " but requested index is " + bucketSize);
    }

    final int entryPosition = ENTRIES_OFFSET + ridBagId * ENTRY_SIZE;
    final int currentSize = getIntValue(entryPosition);
    if (currentSize < 0) {
      throw new OStorageException("RidBag is deleted and can not be used");
    }

    if (currentSize == 0) {
      throw new OStorageException("RidBag is in invalid state because it does not have any items");
    }

    setIntValue(entryPosition, currentSize - 1);
  }

  public int getSize(int ridBagId) {
    final int bucketSize = getIntValue(SIZE_OFFSET);
    if (ridBagId >= bucketSize) {
      throw new OStorageException(
          "Bucket is filled up to " + bucketSize + " but requested index is " + bucketSize);
    }

    final int entryPosition = ENTRIES_OFFSET + ridBagId * ENTRY_SIZE;
    final int currentSize = getIntValue(entryPosition);
    if (currentSize < 0) {
      throw new OStorageException("RidBag is deleted and can not be used");
    }

    return currentSize;
  }

  public void delete(int ridBagId, int freeListHeader) {
    final int bucketSize = getIntValue(SIZE_OFFSET);
    if (ridBagId >= bucketSize) {
      throw new OStorageException(
          "Bucket is filled up to " + bucketSize + " but requested index is " + bucketSize);
    }

    final int entryPosition = ENTRIES_OFFSET + ridBagId * ENTRY_SIZE;
    final int currentSize = getIntValue(entryPosition);
    if (currentSize < 0) {
      throw new OStorageException("RidBag is already deleted and can not be used");
    }

    setIntValue(entryPosition, -2 - freeListHeader);
  }

  public int getNextFreeListItem(int ridBagId) {
    final int bucketSize = getIntValue(SIZE_OFFSET);
    if (ridBagId >= bucketSize) {
      throw new OStorageException(
          "Bucket is filled up to " + bucketSize + " but requested index is " + bucketSize);
    }

    final int entryPosition = ENTRIES_OFFSET + ridBagId * ENTRY_SIZE;
    final int listHeader = getIntValue(entryPosition);

    if (listHeader >= 0) {
      throw new OStorageException(
          "Bucket with index "
              + getPageIndex()
              + " does not contain pointer on next item of free list, instead it contains ridbag with id "
              + ridBagId);
    }

    return -listHeader - 2;
  }
}
