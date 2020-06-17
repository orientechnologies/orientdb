package com.orientechnologies.orient.core.storage.cache.chm;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class LRUList implements Iterable<OCacheEntry> {
  private int size;

  private OCacheEntry head;
  private OCacheEntry tail;

  void remove(final OCacheEntry entry) {
    final OCacheEntry next = entry.getNext();
    final OCacheEntry prev = entry.getPrev();

    if (!(next != null || prev != null || entry == head)) {
      return;
    }

    assert prev == null || prev.getNext() == entry;
    assert next == null || next.getPrev() == entry;

    if (next != null) {
      next.setPrev(prev);
    }

    if (prev != null) {
      prev.setNext(next);
    }

    if (head == entry) {
      assert entry.getPrev() == null;
      head = next;
    }

    if (tail == entry) {
      assert entry.getNext() == null;
      tail = prev;
    }

    entry.setNext(null);
    entry.setPrev(null);
    entry.setContainer(null);

    size--;
  }

  boolean contains(final OCacheEntry entry) {
    return entry.getContainer() == this;
  }

  void moveToTheTail(final OCacheEntry entry) {
    if (tail == entry) {
      assert entry.getNext() == null;
      return;
    }

    final OCacheEntry next = entry.getNext();
    final OCacheEntry prev = entry.getPrev();

    final boolean newEntry = entry.getContainer() == null;
    assert entry.getContainer() == null || entry.getContainer() == this;

    assert prev == null || prev.getNext() == entry;
    assert next == null || next.getPrev() == entry;

    if (prev != null) {
      prev.setNext(next);
    }

    if (next != null) {
      next.setPrev(prev);
    }

    if (head == entry) {
      assert entry.getPrev() == null;
      head = next;
    }

    entry.setPrev(tail);
    entry.setNext(null);

    if (tail != null) {
      assert tail.getNext() == null;
      tail.setNext(entry);
      tail = entry;
    } else {
      tail = head = entry;
    }

    if (newEntry) {
      entry.setContainer(this);
      size++;
    } else {
      assert entry.getContainer() == this;
    }
  }

  int size() {
    return size;
  }

  OCacheEntry poll() {
    if (head == null) {
      return null;
    }

    final OCacheEntry entry = head;

    final OCacheEntry next = head.getNext();
    assert next == null || next.getPrev() == head;

    head = next;
    if (next != null) {
      next.setPrev(null);
    }

    assert head == null || head.getPrev() == null;

    if (head == null) {
      tail = null;
    }

    entry.setNext(null);
    assert entry.getPrev() == null;

    size--;

    entry.setContainer(null);
    return entry;
  }

  OCacheEntry peek() {
    return head;
  }

  public Iterator<OCacheEntry> iterator() {
    return new Iterator<OCacheEntry>() {
      private OCacheEntry next = tail;

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public OCacheEntry next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final OCacheEntry result = next;
        next = next.getPrev();

        return result;
      }
    };
  }
}
