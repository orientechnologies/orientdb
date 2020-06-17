package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.deque;

import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.deque.Node.BUFFER_SIZE;

import java.util.concurrent.atomic.AtomicReference;

public final class MPSCFAAArrayDequeue<T> extends AtomicReference<Node<T>> {
  private volatile Node<T> head;
  private static final Object taken = new Object();

  public MPSCFAAArrayDequeue() {
    final Node<T> dummyNode = new Node<>();
    dummyNode.enqidx.set(0);

    set(dummyNode);
    head = dummyNode;
  }

  public void offer(T record) {
    while (true) {
      final Node<T> tail = get();
      final int idx = tail.enqidx.getAndIncrement();

      if (idx > BUFFER_SIZE - 1) { // This node is full
        if (tail != get()) continue;
        final Node<T> next = tail.getNext();
        if (next == null) {
          final Node<T> newNode = new Node<>(record, tail);

          if (tail.casNext(null, newNode)) {
            compareAndSet(tail, newNode);
            return;
          }
        } else {
          compareAndSet(tail, next);
        }
        continue;
      }

      if (tail.items.compareAndSet(idx, null, record)) {
        return;
      }
    }
  }

  public T poll() {
    while (true) {
      Node<T> head = this.head;

      final int deqidx = head.deqidx;
      final int enqidx = head.enqidx.get();

      if ((deqidx >= enqidx || deqidx >= BUFFER_SIZE) && head.getNext() == null) {
        return null;
      }

      if (deqidx >= BUFFER_SIZE) {
        this.head = head.getNext();

        head.clearPrev(); // allow gc to clear previous items
        continue;
      }

      final int idx = head.deqidx++;

      @SuppressWarnings("unchecked")
      final T item = head.items.getAndSet(idx, (T) taken);
      if (item == null) {
        continue;
      }

      return item;
    }
  }

  public T peek() {
    Node<T> head = this.head;

    while (true) {
      final int deqidx = head.deqidx;
      final int enqidx = head.enqidx.get();

      if (deqidx >= enqidx || deqidx >= BUFFER_SIZE) {
        if (head.getNext() == null) {
          return null;
        }

        head = head.getNext();
        continue;
      }

      final int idx = deqidx;
      final T item = head.items.get(idx);

      if (item == null || item == taken) {
        continue;
      }

      return item;
    }
  }

  public Cursor<T> peekFirst() {
    Node<T> head = this.head;

    while (true) {
      final int deqidx = head.deqidx;
      final int enqidx = head.enqidx.get();

      if (deqidx >= enqidx || deqidx >= BUFFER_SIZE) {
        if (head.getNext() == null) {
          return null;
        }

        head = head.getNext();
        continue;
      }

      final int idx = deqidx;
      final T item = head.items.get(idx);
      if (item == null || item == taken) {
        continue;
      }

      return new Cursor<>(head, idx, item);
    }
  }

  public static <T> Cursor<T> next(Cursor<T> cursor) {
    if (cursor == null) {
      return null;
    }

    Node<T> node = cursor.node;
    int idx = cursor.itemIndex + 1;

    while (node != null) {
      int enqidx = node.enqidx.get();

      if (idx >= enqidx || idx >= BUFFER_SIZE) {
        if (enqidx < BUFFER_SIZE) {
          return null; // reached the end of the queue
        } else {
          node = node.getNext();
          idx = 0;
          continue;
        }
      }

      final T item = node.items.get(idx);
      if (item == null) {
        continue; // counters may be updated but item itslef is not updated yet
      }
      if (item == taken) {
        return null;
      }

      return new Cursor<>(node, idx, item);
    }

    return null;
  }

  public Cursor<T> peekLast() {
    while (true) {
      Node<T> tail = get();

      final int enqidx = tail.enqidx.get();
      final int deqidx = tail.deqidx;
      if (deqidx >= enqidx || deqidx >= BUFFER_SIZE) {
        return null; // we remove only from the head, so if tail is empty it means that queue is
        // empty
      }

      int idx = enqidx;
      if (idx >= BUFFER_SIZE) {
        idx = BUFFER_SIZE;
      }

      if (idx <= 0) {
        return null; // No more items in the node
      }

      final T item = tail.items.get(idx - 1);
      if (item == null || item == taken) {
        continue; // concurrent modification
      }

      return new Cursor<>(tail, idx - 1, item);
    }
  }

  public static <T> Cursor<T> prev(Cursor<T> cursor) {
    if (cursor == null) {
      return null;
    }

    Node<T> node = cursor.node;
    int idx = cursor.itemIndex - 1;

    while (node != null) {
      int deqidx = node.deqidx;

      if (deqidx > idx
          || deqidx >= BUFFER_SIZE) { // idx == enqidx -1, that is why we use >, but not >=
        if (deqidx > 0) {
          return null; // reached the end of the queue
        } else {
          node = node.getPrev();
          idx = BUFFER_SIZE - 1;
          continue;
        }
      }

      final T item = node.items.get(idx); // reached end of the queue
      if (item == null) {
        continue; // counters may be updated but values are still not updated
      }
      if (item == taken) {
        return null;
      }

      return new Cursor<>(node, idx, item);
    }

    return null;
  }
}
