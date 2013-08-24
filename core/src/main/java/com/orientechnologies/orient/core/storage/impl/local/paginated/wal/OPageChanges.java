package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;

public class OPageChanges {
  private static final int    INITIAL_SIZE   = 16;

  private final ODirectMemory directMemory   = ODirectMemoryFactory.INSTANCE.directMemory();
  private ChangesBucket[]     changesBuckets = new ChangesBucket[INITIAL_SIZE];

  private int                 size           = 0;

  public void addChanges(int pageOffset, byte[] newValues, byte[] oldValues) {
    assert newValues.length == oldValues.length;

    if (size == 0) {
      changesBuckets[0] = new ChangesBucket(pageOffset, newValues, oldValues);
      size = 1;
    } else {
      ChangesBucket bucketToUse;
      int bucketIndex;

      final int insertionIndex = binarySearch(pageOffset);
      if (insertionIndex >= 0) {
        bucketIndex = insertionIndex;
        bucketToUse = changesBuckets[bucketIndex];
        bucketToUse.updateValues(pageOffset, newValues, oldValues);
      } else {
        bucketIndex = -insertionIndex - 1;

        if (bucketIndex < size) {
          final ChangesBucket bucket = changesBuckets[bucketIndex];
          if (bucket.startPosition < pageOffset) {
            bucketToUse = bucket;
            bucketToUse.updateValues(pageOffset, newValues, oldValues);
          } else {
            bucketToUse = new ChangesBucket(pageOffset, newValues, oldValues);
          }
        } else {
          bucketToUse = new ChangesBucket(pageOffset, newValues, oldValues);
        }
      }

      int shiftBackFrom = -1;
      int shiftBackTo = -1;

      int startIndex;
      if (bucketIndex < size && bucketToUse == changesBuckets[bucketIndex]) {
        startIndex = bucketIndex + 1;
      } else {
        startIndex = bucketIndex;
      }

      for (int i = startIndex; i < size; i++) {
        ChangesBucket bucketToMerge = changesBuckets[i];
        if (bucketToUse.endPosition >= bucketToMerge.startPosition) {
          bucketToUse.merge(bucketToMerge);
          if (i == startIndex) {
            shiftBackFrom = startIndex;
            shiftBackTo = startIndex;
          } else
            shiftBackTo = i;
        } else
          break;
      }

      if (shiftBackFrom >= bucketIndex && shiftBackTo < bucketIndex) {
        shiftBackFrom++;
        changesBuckets[bucketIndex] = bucketToUse;

        collapse(shiftBackFrom, shiftBackTo);
      } else {
        if (shiftBackFrom >= 0)
          collapse(shiftBackFrom, shiftBackTo);
      }

      if (bucketIndex >= size || bucketToUse != changesBuckets[bucketIndex])
        insert(bucketIndex, bucketToUse);
    }
  }

  public void applyChanges(long pointer) {
    for (int i = 0; i < size; i++) {
      ChangesBucket bucket = changesBuckets[i];
      directMemory.set(pointer + bucket.startPosition, bucket.newValues, 0, bucket.newValues.length);
    }
  }

  public void revertChanges(long pointer) {
    for (int i = 0; i < size; i++) {
      ChangesBucket bucket = changesBuckets[i];
      directMemory.set(pointer + bucket.startPosition, bucket.oldValues, 0, bucket.oldValues.length);
    }
  }

  private void insert(int bucketIndex, ChangesBucket bucket) {
    assert bucketIndex <= size;

    if (size < changesBuckets.length) {
      System.arraycopy(changesBuckets, bucketIndex, changesBuckets, bucketIndex + 1, size - bucketIndex);
      changesBuckets[bucketIndex] = bucket;
    } else {
      ChangesBucket[] oldChangesBuckets = changesBuckets;
      changesBuckets = new ChangesBucket[changesBuckets.length << 1];

      if (bucketIndex == size - 1) {
        System.arraycopy(oldChangesBuckets, 0, changesBuckets, 0, oldChangesBuckets.length);
        changesBuckets[bucketIndex] = bucket;
      } else {
        if (bucketIndex > 0)
          System.arraycopy(oldChangesBuckets, 0, changesBuckets, 0, bucketIndex);

        if (bucketIndex < size)
          System.arraycopy(oldChangesBuckets, bucketIndex, changesBuckets, bucketIndex + 1, size - bucketIndex);

        changesBuckets[bucketIndex] = bucket;
      }
    }

    size++;
  }

  private int binarySearch(int startPosition) {
    int low = 0;
    int high = size - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      ChangesBucket midBucket = changesBuckets[mid];
      if (midBucket.endPosition < startPosition)
        low = mid + 1;
      else if (midBucket.endPosition > startPosition)
        high = mid - 1;
      else
        return mid;
    }
    return -(low + 1);
  }

  private void collapse(int shiftBackFrom, int shiftBackTo) {
    assert shiftBackTo >= shiftBackFrom;
    if (shiftBackTo < size - 1) {
      System.arraycopy(changesBuckets, shiftBackTo + 1, changesBuckets, shiftBackFrom, size - shiftBackTo);
      for (int i = size - shiftBackTo; i < size; i++)
        changesBuckets[i] = null;
    } else {
      for (int i = shiftBackFrom; i <= shiftBackTo; i++)
        changesBuckets[i] = null;
    }

    size -= (shiftBackTo - shiftBackFrom + 1);
  }

  private static final class ChangesBucket {
    private final int startPosition;
    private int       endPosition;

    private byte[]    newValues;
    private byte[]    oldValues;

    private ChangesBucket(int startPosition, byte[] newValues, byte[] oldValues) {
      assert newValues.length == oldValues.length;

      this.startPosition = startPosition;
      this.endPosition = startPosition + newValues.length;
      this.newValues = newValues;
      this.oldValues = oldValues;
    }

    public void updateValues(int startPosition, byte[] newValues, byte[] oldValues) {
      assert startPosition <= this.endPosition;
      assert startPosition >= this.startPosition;
      assert newValues.length == oldValues.length;

      int endPosition = startPosition + newValues.length;

      if (endPosition > this.endPosition) {
        int lenDiff = endPosition - this.endPosition;

        byte[] oldNewValues = this.newValues;
        byte[] oldOldValues = this.oldValues;

        this.newValues = new byte[this.newValues.length + lenDiff];
        System.arraycopy(oldNewValues, 0, this.newValues, 0, oldNewValues.length);

        this.oldValues = new byte[this.oldValues.length + lenDiff];
        System.arraycopy(oldOldValues, 0, this.oldValues, 0, oldOldValues.length);

        System.arraycopy(oldValues, oldValues.length - lenDiff, this.oldValues, this.oldValues.length - lenDiff, lenDiff);

        this.endPosition = endPosition;
      }

      final int dataOffset = startPosition - this.startPosition;
      System.arraycopy(newValues, 0, this.newValues, dataOffset, newValues.length);
    }

    public void merge(ChangesBucket bucketToMerge) {
      assert bucketToMerge.startPosition <= endPosition;
      assert bucketToMerge.startPosition >= startPosition;

      if (endPosition < bucketToMerge.endPosition) {
        int newValuesDiff = bucketToMerge.endPosition - this.endPosition;

        byte[] oldNewValues = this.newValues;
        byte[] oldOldValues = this.oldValues;

        this.newValues = new byte[this.newValues.length + newValuesDiff];
        System.arraycopy(oldNewValues, 0, this.newValues, 0, oldNewValues.length);

        this.oldValues = new byte[this.oldValues.length + newValuesDiff];
        System.arraycopy(oldOldValues, 0, this.oldValues, 0, oldOldValues.length);

        System.arraycopy(bucketToMerge.newValues, bucketToMerge.newValues.length - newValuesDiff, this.newValues,
            this.newValues.length - newValuesDiff, newValuesDiff);

        this.endPosition = bucketToMerge.endPosition;
      }

      int oldValuesFrom = bucketToMerge.startPosition - this.startPosition;

      assert oldValuesFrom + bucketToMerge.oldValues.length <= this.oldValues.length;
      System.arraycopy(bucketToMerge.oldValues, 0, this.oldValues, oldValuesFrom, bucketToMerge.oldValues.length);

    }
  }
}
