package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.deque;

public class Cursor<T> {
  final Node<T> node;
  final int  pageIndex;
  final T    item;

  public Cursor(Node<T> node, int pageIndex, T item) {
    this.node = node;
    this.pageIndex = pageIndex;
    this.item = item;
  }

  public T getItem() {
    return item;
  }
}
