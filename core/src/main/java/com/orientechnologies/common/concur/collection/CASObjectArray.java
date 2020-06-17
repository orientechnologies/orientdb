package com.orientechnologies.common.concur.collection;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

public final class CASObjectArray<T> {
  private final AtomicInteger size = new AtomicInteger();
  private final AtomicReferenceArray<AtomicReferenceArray<T>> containers =
      new AtomicReferenceArray<>(32);

  public int add(T value) {
    Objects.requireNonNull(value);

    while (true) {
      final int newIndex = size.get();
      final int containerIndex = 31 - Integer.numberOfLeadingZeros(newIndex + 1);
      final int containerSize = 1 << containerIndex;
      final int indexInsideContainer = newIndex + 1 - containerSize;

      AtomicReferenceArray<T> container = containers.get(containerIndex);
      if (container == null) {
        container = new AtomicReferenceArray<>(containerSize);
        if (!containers.compareAndSet(containerIndex, null, container)) {
          container = containers.get(containerIndex);
        }
      }

      if (container.compareAndSet(indexInsideContainer, null, value)) {
        size.incrementAndGet();
        return newIndex;
      }
    }
  }

  public void set(int index, T value, T placeholder) {
    Objects.requireNonNull(value);
    Objects.requireNonNull(placeholder);

    final int size = this.size.get();

    if (size <= index) {
      //noinspection StatementWithEmptyBody
      while (add(placeholder) < index) {
        // repeat till we will not create place for the element
      }
    }

    final int containerIndex = 31 - Integer.numberOfLeadingZeros(index + 1);
    final int containerSize = 1 << containerIndex;
    final int indexInsideContainer = index + 1 - containerSize;

    AtomicReferenceArray<T> container;
    while (true) {
      container = containers.get(containerIndex);
      if (container == null) {
        Thread.yield();
      } else {
        break;
      }
    }

    container.set(indexInsideContainer, value);
  }

  public boolean compareAndSet(int index, T oldValue, T value) {
    Objects.requireNonNull(value);
    Objects.requireNonNull(oldValue);

    final int size = this.size.get();

    if (size <= index) {
      throw new ArrayIndexOutOfBoundsException("Requested " + index + ", size is " + size);
    }

    final int containerIndex = 31 - Integer.numberOfLeadingZeros(index + 1);
    final int containerSize = 1 << containerIndex;
    final int indexInsideContainer = index + 1 - containerSize;

    AtomicReferenceArray<T> container;
    while (true) {
      container = containers.get(containerIndex);
      if (container == null) {
        Thread.yield();
      } else {
        break;
      }
    }

    return container.compareAndSet(indexInsideContainer, oldValue, value);
  }

  public T get(int index) {
    final int size = this.size.get();

    if (size <= index) {
      throw new ArrayIndexOutOfBoundsException("Requested " + index + ", size is " + size);
    }

    final int containerIndex = 31 - Integer.numberOfLeadingZeros(index + 1);
    final int containerSize = 1 << containerIndex;
    final int indexInsideContainer = index + 1 - containerSize;

    AtomicReferenceArray<T> container;
    while (true) {
      container = containers.get(containerIndex);
      if (container == null) {
        Thread.yield();
      } else {
        break;
      }
    }

    T value;
    while (true) {
      value = container.get(indexInsideContainer);
      if (value == null) {
        Thread.yield();
      } else {
        break;
      }
    }

    return value;
  }

  public int size() {
    return size.get();
  }
}
