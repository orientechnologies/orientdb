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

      final int deqidx = head.deqidx;
      final int enqidx = head.enqidx.get();

      if (deqidx >= enqidx || deqidx >= BUFFER_SIZE) {
        if (head.getNext() == null) {
          return null;
        }

        this.head = head.getNext();
        continue;
      }

      final int idx = head.deqidx++;

      final OWALRecord item = head.items.get(idx);
      head.items.set(idx, null);

      assert item != null;

      if (idx == BUFFER_SIZE - 1) {
        if (head.getNext() != null) {
          this.head = head.getNext();
        }
      }

      return item;
    }
  }

  public OWALRecord peek() {
    Node head = this.head;

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
      final OWALRecord item = head.items.get(idx);
      if (item == null) {
        continue;
      }

      return item;
    }
  }

  public Cursor peekFirst() {
    Node head = this.head;

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
      int enqidx = node.enqidx.get();

      if (idx >= enqidx || idx >= BUFFER_SIZE) {
        if (enqidx < BUFFER_SIZE) {
          return null; //reached the end of the queue
        } else {
          node = node.getNext();
          idx = 0;
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

      final int enqidx = tail.enqidx.get();
      final int deqidx = tail.deqidx;
      if (deqidx >= enqidx || deqidx >= BUFFER_SIZE) {
        return null; //we remove only from the head, so if tail is empty it means that queue is empty
      }

      final int idx = enqidx;

      if (idx <= 0) {
        return null;  // No more nodes in the queue
      }

      final OWALRecord item = tail.items.get(idx - 1);
      if (item == null) {
        continue;//should be not null, concurrent modification
      }

      return new Cursor(tail, idx - 1, item);
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

      if (deqidx > idx || deqidx >= BUFFER_SIZE) {//idx == enqidx -1, that is why we use >, but not >=
        if (deqidx > 0) {
          return null; //reached the end of the queue
        } else {
          node = node.getPrev();
          idx = BUFFER_SIZE - 1;
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