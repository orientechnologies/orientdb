package com.orientechnologies.common.collection.closabledictionary;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * LRU list is used inside of {@link OClosableLinkedContainer}.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
class OClosableLRUList<K, V extends OClosableItem> implements Iterable<OClosableEntry<K, V>> {
  private int size;

  private OClosableEntry<K, V> head;
  private OClosableEntry<K, V> tail;

  void remove(OClosableEntry<K, V> entry) {
    final OClosableEntry<K, V> next = entry.getNext();
    final OClosableEntry<K, V> prev = entry.getPrev();

    if (!(next != null || prev != null || entry == head)) return;

    if (prev != null) {
      assert prev.getNext() == entry;
    }

    if (next != null) {
      assert next.getPrev() == entry;
    }

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

    size--;
  }

  boolean contains(OClosableEntry<K, V> entry) {
    return entry.getNext() != null || entry.getPrev() != null || entry == head;
  }

  void moveToTheTail(OClosableEntry<K, V> entry) {
    if (tail == entry) {
      assert entry.getNext() == null;
      return;
    }

    final OClosableEntry<K, V> next = entry.getNext();
    final OClosableEntry<K, V> prev = entry.getPrev();

    boolean newEntry = !(next != null || prev != null || entry == head);

    if (prev != null) {
      assert prev.getNext() == entry;
    }

    if (next != null) {
      assert next.getPrev() == entry;
    }

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

    if (newEntry) size++;
  }

  int size() {
    return size;
  }

  OClosableEntry<K, V> poll() {
    if (head == null) return null;

    final OClosableEntry<K, V> entry = head;

    OClosableEntry<K, V> next = head.getNext();
    assert next == null || next.getPrev() == head;

    head = next;
    if (next != null) {
      next.setPrev(null);
    }

    assert head == null || head.getPrev() == null;

    if (head == null) tail = null;

    entry.setNext(null);
    assert entry.getPrev() == null;

    size--;

    return entry;
  }

  /** @return Iterator to iterate from head to the tail. */
  public Iterator<OClosableEntry<K, V>> iterator() {
    return new Iterator<OClosableEntry<K, V>>() {
      private OClosableEntry<K, V> next = head;
      private OClosableEntry<K, V> current = null;

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public OClosableEntry<K, V> next() {
        if (next == null) {
          throw new NoSuchElementException();
        }

        current = next;
        next = next.getNext();
        return current;
      }

      @Override
      public void remove() {
        if (current == null) {
          throw new IllegalStateException("Method next was not called");
        }

        OClosableLRUList.this.remove(current);
        current = null;
      }
    };
  }

  boolean assertForwardStructure() {
    if (head == null) return tail == null;

    OClosableEntry<K, V> current = head;

    while (current.getNext() != null) {
      OClosableEntry<K, V> prev = current.getPrev();
      OClosableEntry<K, V> next = current.getNext();

      if (prev != null) {
        assert prev.getNext() == current;
      }

      if (next != null) {
        assert next.getPrev() == current;
      }

      current = current.getNext();
    }

    return current == tail;
  }

  boolean assertBackwardStructure() {
    if (tail == null) return head == null;

    OClosableEntry<K, V> current = tail;

    while (current.getPrev() != null) {
      OClosableEntry<K, V> prev = current.getPrev();
      OClosableEntry<K, V> next = current.getNext();

      if (prev != null) {
        assert prev.getNext() == current;
      }

      if (next != null) {
        assert next.getPrev() == current;
      }

      current = current.getPrev();
    }

    return current == head;
  }
}
