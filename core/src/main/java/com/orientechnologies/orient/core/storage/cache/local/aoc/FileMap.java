package com.orientechnologies.orient.core.storage.cache.local.aoc;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

public final class FileMap {
  private static final int MAX_PAGE_SIZE = (1 << 7) - 1;
  private static final int MAX_PAGE_VERSION = (1 << (3 * 8)) - 1;

  public static final byte DATA_PAGE = 0;
  public static final byte DELTA_PAGE = 1;

  private final AtomicReferenceArray<AtomicLongArray> container = new AtomicReferenceArray<>(32);
  private final AtomicInteger size = new AtomicInteger();

  public int allocateNewPage() {
    final int index = size.getAndIncrement();
    final long mappingEntry = mappingEntry(-1, 0, DATA_PAGE, 0);
    doSet(index, mappingEntry);

    return index;
  }

  public void setMapping(
      final int index,
      final int physicalPageIndex,
      final int pageSize,
      final byte pageType,
      final int pageVersion) {
    checkFileSize(index);

    validateData(physicalPageIndex, pageSize, pageType, pageVersion);

    final long mappingEntry = mappingEntry(physicalPageIndex, pageSize, pageType, pageVersion);
    doSet(index, mappingEntry);
  }

  private void validateData(int physicalPageIndex, int pageSize, byte pageType, int pageVersion) {
    if (physicalPageIndex < 0) {
      throw new IllegalArgumentException(
          "Invalid value of physical page index " + physicalPageIndex);
    }
    if (pageSize <= 0) {
      throw new IllegalArgumentException("Invalid value of page size " + pageSize);
    }
    if (pageSize > MAX_PAGE_SIZE) {
      throw new IllegalArgumentException("Value of page size can not exceed " + MAX_PAGE_SIZE);
    }
    if (pageType != DATA_PAGE && pageType != DELTA_PAGE) {
      throw new IllegalArgumentException("Invalid value of page type " + pageType);
    }
    if (pageVersion > MAX_PAGE_VERSION) {
      throw new IllegalArgumentException("Value of page version can not exceed " + pageVersion);
    }
    if (pageVersion < 0) {
      throw new IllegalArgumentException("Invalid value of page version " + pageVersion);
    }
  }

  public int getSize() {
    return size.get();
  }

  public int[] mappingData(final int index) {
    final int currentSize = size.get();
    if (index >= currentSize) {
      return null;
    }

    final long mappingEntry = doGet(index);
    return mappingData(mappingEntry);
  }

  private void checkFileSize(int index) {
    final int currentSize = size.get();
    if (index >= size.get()) {
      throw new IllegalArgumentException(
          "Attempt to access index outside of file size. File size: "
              + currentSize
              + ", index "
              + index);
    }
  }

  private void doSet(final int index, final long value) {
    final int containerIndex = 31 - Integer.numberOfLeadingZeros(index + 1);
    AtomicLongArray array = container.get(containerIndex);

    while (array == null) {
      final AtomicLongArray newArray = new AtomicLongArray(1 << containerIndex);
      if (container.compareAndSet(containerIndex, null, newArray)) {
        array = newArray;
      }
    }

    final int arrayIndex = index - ((1 << containerIndex) - 1);
    array.set(arrayIndex, value);
  }

  private long doGet(final int index) {
    final int containerIndex = 31 - Integer.numberOfLeadingZeros(index + 1);
    final AtomicLongArray array = container.get(containerIndex);
    if (array == null) {
      throw new IllegalArgumentException("Attempt to access outside of file, index " + index);
    }
    final int arrayIndex = index - ((1 << containerIndex) - 1);
    return array.get(arrayIndex);
  }

  private long mappingEntry(
      final int pageIndex, final int pageSize, final byte pageType, final int pageVersion) {
    return (0xFF_FF_FF_FFL & pageIndex)
        | (((long) pageSize) << (4 * 8))
        | (((long) pageType) << (4 * 8 + 7))
        | (((long) pageVersion) << 5 * 8);
  }

  private int[] mappingData(final long mappingEntry) {
    final int pageIndex = (int) mappingEntry;
    final int pageSize = (int) (0x7F & (mappingEntry >>> (4 * 8)));
    final int pageType = (int) (1 & (mappingEntry >>> (4 * 8 + 7)));
    final int pageVersion = (int) (mappingEntry >>> 5 * 8);

    return new int[] {pageIndex, pageSize, pageType, pageVersion};
  }
}
