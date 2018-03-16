package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.deque;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;

import java.util.concurrent.atomic.AtomicReference;

import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.deque.Node.BUFFER_SIZE;

public class MPSCFAAArrayDequeue extends AtomicReference<Node> {
  private volatile Node head;

  public MPSCFAAArrayDequeue() {
    final Node dummyNode = new Node();
    dummyNode.enqidx.set(0);

    set(dummyNode);
    head = dummyNode;
  }

  public void offer(OWALRecord record) {
    while (true) {
      final Node tail = get();
      final int idx = tail.enqidx.getAndIncrement();

      if (idx > BUFFER_SIZE - 1) { // This node is full
        if (tail != get())
          continue;
        final Node next = tail.getNext();
        if (next == null) {
          final Node newNode = new Node(record, tail);

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

  public OWALRecord poll() {
    while (true) {
      Node head = this.head;

      if (head.deqidx >= head.enqidx.get() && head.getNext() == null) {
        return null;
      }

      final int idx = head.deqidx++;

      if (idx > BUFFER_SIZE - 1) { // This node has been drained, check if there is another one
        if (head.getNext() == null)
          return null;  // No more nodes in the queue

        this.head = head.getNext();

        head.clearPrev();
        head.clearNext();

        continue;
      }

      final OWALRecord item = head.items.get(idx);
      head.items.set(idx, null);

      assert item != null;

      return item;
    }
  }

  public OWALRecord peek() {
    while (true) {
      Node head = this.head;

      if (head.deqidx >= head.enqidx.get() && head.getNext() == null) {
        return null;
      }

      int idx = head.deqidx;

      if (idx > BUFFER_SIZE - 1) { // This node has been drained, check if there is another one
        if (head.getNext() == null)
          return null;  // No more nodes in the queue

        continue;
      }

      final OWALRecord item = head.items.get(idx);
      if (item == null) {
        continue;
      }

      return item;
    }
  }

  public Cursor peekFirst() {
    while (true) {
      Node head = this.head;

      if (head.deqidx >= head.enqidx.get() && head.getNext() == null) {
        return null;
      }

      int idx = head.deqidx;

      if (idx > BUFFER_SIZE - 1) { // This node has been drained, check if there is another one
        if (head.getNext() == null)
          return null;  // No more nodes in the queue

        continue;
      }

      final OWALRecord record = head.items.get(idx);
      if (record == null) {
        continue;
      }

      return new Cursor(head, idx, record);
    }
  }

  public static Cursor next(Cursor cursor) {
    if (cursor == null) {
      return null;
    }

    Node node = cursor.node;
    int idx = cursor.pageIndex + 1;

    while (node != null) {
      int inqidx = node.enqidx.get();

      if (idx >= inqidx) {
        if (inqidx < BUFFER_SIZE) {
          return null; //reached the end of the queue
        } else {
          node = node.getNext();
          continue;
        }
      }

      final OWALRecord record = node.items.get(idx);//reached end of the queue
      if (record == null) {
        return null;
      }

      return new Cursor(node, idx, record);
    }

    return null;
  }

  public Cursor peekLast() {
    while (true) {
      Node tail = get();

      if (tail.deqidx >= tail.enqidx.get() && tail.getPrev() == null) {
        return null;
      }

      int idx = tail.enqidx.get();

      if (idx <= 0) { // This node is empty check if there is predecessor of it.
        if (tail.getPrev() == null)
          return null;  // No more nodes in the queue

        continue;
      }

      final OWALRecord item = tail.items.get(idx - 1);
      if (item == null) {
        continue;
      }

      return new Cursor(tail, idx, item);
    }
  }

  public static Cursor prev(Cursor cursor) {
    if (cursor == null) {
      return null;
    }

    Node node = cursor.node;
    int idx = cursor.pageIndex - 1;

    while (node != null) {
      int deqidx = node.deqidx;

      if (deqidx >= idx) {
        if (deqidx > 0) {
          return null; //reached the end of the queue
        } else {
          node = node.getPrev();
          continue;
        }
      }

      final OWALRecord record = node.items.get(idx);//reached end of the queue
      if (record == null) {
        return null;
      }

      return new Cursor(node, idx, record);
    }

    return null;
  }
}