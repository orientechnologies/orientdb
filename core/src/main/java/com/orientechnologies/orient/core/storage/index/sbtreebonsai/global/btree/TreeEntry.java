package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree;

import java.util.Objects;

final class TreeEntry implements Comparable<TreeEntry> {

  protected final int leftChild;
  protected final int rightChild;
  private final EdgeKey key;
  private final int value;

  public TreeEntry(final int leftChild, final int rightChild, final EdgeKey key, final int value) {
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.key = key;
    this.value = value;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TreeEntry that = (TreeEntry) o;
    return leftChild == that.leftChild
        && rightChild == that.rightChild
        && Objects.equals(getKey(), that.getKey())
        && Objects.equals(getValue(), that.getValue());
  }

  @Override
  public int hashCode() {
    return Objects.hash(leftChild, rightChild, getKey(), getValue());
  }

  @Override
  public String toString() {
    return "CellBTreeEntry{"
        + "leftChild="
        + leftChild
        + ", rightChild="
        + rightChild
        + ", key="
        + getKey()
        + ", value="
        + getValue()
        + '}';
  }

  @Override
  public int compareTo(final TreeEntry other) {
    return getKey().compareTo(other.getKey());
  }

  public EdgeKey getKey() {
    return key;
  }

  public int getValue() {
    return value;
  }
}
