package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.deque;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

public final class Node<T> extends AtomicReference<Node<T>> {
  static final int BUFFER_SIZE = 1024;

  private volatile Node<T> prev;

  public volatile int deqidx = 0;
  public final AtomicReferenceArray<T> items = new AtomicReferenceArray<>(BUFFER_SIZE);
  public final AtomicInteger enqidx = new AtomicInteger(1);

  Node() {}

  Node(T record, Node<T> prev) {
    items.lazySet(0, record);
    this.prev = prev;
  }

  public void clearPrev() {
    prev = null;
  }

  public boolean casNext(Node<T> oldNode, Node<T> newNode) {
    return compareAndSet(oldNode, newNode);
  }

  public Node<T> getNext() {
    return get();
  }

  public Node<T> getPrev() {
    return prev;
  }

  @Override
  public String toString() {
    return "Node{" + "deqidx=" + deqidx + ", items=" + items + ", enqidx=" + enqidx + '}';
  }
}
