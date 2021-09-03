package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.deque;

public final class Cursor<T> {
  protected final Node<T> node;
  protected final int itemIndex;
  private final T item;

  public Cursor(Node<T> node, int itemIndex, T item) {
    this.node = node;
    this.itemIndex = itemIndex;
    this.item = item;
  }

  public T getItem() {
    return item;
  }

  @Override
  public String toString() {
    return "Cursor{" + "node=" + node + ", itemIndex=" + itemIndex + ", item=" + item + '}';
  }
}
