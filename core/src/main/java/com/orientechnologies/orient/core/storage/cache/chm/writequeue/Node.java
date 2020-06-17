package com.orientechnologies.orient.core.storage.cache.chm.writequeue;

import java.util.concurrent.atomic.AtomicReference;

public final class Node<E> {
  private final AtomicReference<Node<E>> next = new AtomicReference<>();
  private final E item;

  public Node(final E item) {
    this.item = item;
  }

  public Node<E> getNext() {
    return next.get();
  }

  public E getItem() {
    return item;
  }

  void lazySetNext(final Node<E> next) {
    this.next.lazySet(next);
  }
}
