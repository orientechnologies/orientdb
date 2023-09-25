package com.orientechnologies.orient.core.storage.cache.local;

final class PageKey implements Comparable<PageKey> {
  final int fileId;
  final long pageIndex;

  PageKey(final int fileId, final long pageIndex) {
    this.fileId = fileId;
    this.pageIndex = pageIndex;
  }

  @Override
  public int compareTo(final PageKey other) {
    if (fileId > other.fileId) {
      return 1;
    }
    if (fileId < other.fileId) {
      return -1;
    }

    return Long.compare(pageIndex, other.pageIndex);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final PageKey pageKey = (PageKey) o;

    if (fileId != pageKey.fileId) {
      return false;
    }
    return pageIndex == pageKey.pageIndex;
  }

  @Override
  public int hashCode() {
    int result = fileId;
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "PageKey{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + '}';
  }

  public PageKey previous() {
    return pageIndex == -1 ? this : new PageKey(fileId, pageIndex - 1);
  }
}
