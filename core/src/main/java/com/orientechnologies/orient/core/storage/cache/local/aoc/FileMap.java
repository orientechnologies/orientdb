package com.orientechnologies.orient.core.storage.cache.local.aoc;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class FileMap {
  private final AtomicReferenceArray<AtomicLongArray> container = new AtomicReferenceArray<>(32);
  private final AtomicInteger                         size      = new AtomicInteger();

  public int allocateNewPage() {
    final int index = size.getAndIncrement();
    final long mappingEntry = mappingEntry(-1, 0, 0);
    doSet(index, mappingEntry);

    return index;
  }

  public void setMapping(final int index, final int physicalPage, final int startPosition, final int pageSize) {
    checkFileSize(index);

    final long mappingEntry = mappingEntry(physicalPage, startPosition, pageSize);
    doSet(index, mappingEntry);
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
          "Attempt to access index outside of file size. File size: " + currentSize + ", index " + index);
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

  private long mappingEntry(final int pageIndex, final int startPosition, final int pageSize) {
    return (0xFF_FF_FF_FFL & pageIndex) | (((long) startPosition) << (4 * 8)) | (((long) pageSize) << (6 * 8));
  }

  private int physicalIndex(final long mappingEntry) {
    return (int) mappingEntry;
  }

  private int[] mappingData(final long mappingEntry) {
    final int pageIndex = (int) mappingEntry;
    final int startPosition = 0xFF_FF & (int) (mappingEntry >>> (4 * 8));
    final int pageSize = 0xFF_FF & (int) (mappingEntry >>> (6 * 8));

    return new int[] { pageIndex, startPosition, pageSize };
  }

}
