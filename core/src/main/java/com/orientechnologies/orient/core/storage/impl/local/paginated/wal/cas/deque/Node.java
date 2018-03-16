package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.deque;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class Node extends AtomicReference<Node> {
  static final int BUFFER_SIZE = 128;

  private volatile Node prev;

  public volatile int                              deqidx = 0;
  public final    AtomicReferenceArray<OWALRecord> items  = new AtomicReferenceArray<>(BUFFER_SIZE);
  public final    AtomicInteger                    enqidx = new AtomicInteger(1);

  Node() {
  }

  Node(OWALRecord record, Node prev) {
    items.lazySet(0, record);
    this.prev = prev;
  }

  public void clearPrev() {
    prev = null;
  }

  public void clearNext() {
    set(null);
  }

  public boolean casNext(Node oldNode, Node newNode) {
    return compareAndSet(oldNode, newNode);
  }

  public Node getNext() {
    return get();
  }

  public Node getPrev() {
    return prev;
  }
}