package com.orientechnologies.common.collection.closabledictionary;

import java.util.Iterator;
import java.util.NoSuchElementException;

class OClosableLRUList<K, E> implements Iterable<OClosableEntry<K, E>> {
  private int size;

  private OClosableEntry<K, E> head;
  private OClosableEntry<K, E> tail;

  void remove(OClosableEntry<K, E> entry) {
    final OClosableEntry<K, E> next = entry.getNext();
    final OClosableEntry<K, E> prev = entry.getPrev();

    if (!(next != null || prev != null || entry == head))
      return;

    if (prev != null) {
      assert prev.getNext() == entry;
      prev.setNext(next);
    }

    if (next != null) {
      assert next.getPrev() == entry;
      next.setPrev(next);
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

  boolean contains(OClosableEntry<K, E> entry) {
    return entry.getNext() != null || entry.getPrev() != null || entry == head;
  }

  void moveToTheTail(OClosableEntry<K, E> entry) {
    if (tail == entry) {
      assert entry.getNext() == null;
      return;
    }

    final OClosableEntry<K, E> next = entry.getNext();
    final OClosableEntry<K, E> prev = entry.getPrev();

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

    if (newEntry)
      size++;

    assert assertForwardStructure();
    assert assertBackwardStructure();
  }

  int size() {
    return size;
  }

  OClosableEntry<K, E> poll() {
    if (head == null)
      return null;

    final OClosableEntry<K, E> entry = head;

    OClosableEntry<K, E> next = head.getNext();
    assert next.getPrev() == head;

    head = next;
    next.setPrev(null);

    assert head.getPrev() == null;

    if (head == null)
      tail = null;

    entry.setNext(null);
    assert entry.getPrev() == null;

    size--;

    assert assertForwardStructure();
    assert assertBackwardStructure();

    return entry;
  }

  public Iterator<OClosableEntry<K, E>> iterator() {
    return new Iterator<OClosableEntry<K, E>>() {
      private OClosableEntry<K, E> next = tail;

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public OClosableEntry<K, E> next() {
        if (next == null) {
          throw new NoSuchElementException();
        }

        OClosableEntry<K, E> result = next;
        next = next.getPrev();
        return result;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private boolean assertForwardStructure() {
    if (head == null)
      return tail == null;

    OClosableEntry<K, E> current = head;

    while (current.getNext() != null) {
      OClosableEntry<K, E> prev = current.getPrev();
      OClosableEntry<K, E> next = current.getNext();

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

  private boolean assertBackwardStructure() {
    if (tail == null)
      return head == null;

    OClosableEntry<K, E> current = tail;

    while (current.getPrev() != null) {
      OClosableEntry<K, E> prev = current.getPrev();
      OClosableEntry<K, E> next = current.getNext();

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
