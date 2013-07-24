package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;

/**
 * @author Andrey Lomakin
 * @since 7/24/13
 */
public class O2QReadCache {
  private int                           maxSize;
  private int                           K_IN;
  private int                           K_OUT;

  private final int                     pageSize;

  private final LRUList                 am;
  private final LRUList                 a1out;
  private final LRUList                 a1in;

  private final ODirectMemory           directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

  private final Map<Long, OFileClassic> files;
  private final Map<Long, Set<Long>>    filePages;

  public O2QReadCache(Map<Long, OFileClassic> files, int pageSize) {
    this.files = files;
    this.filePages = new HashMap<Long, Set<Long>>();

    this.pageSize = pageSize;

    am = new LRUList();
    a1out = new LRUList();
    a1in = new LRUList();
  }

  public OCacheEntry get(long fileId, long pageIndex) {
    return null;
  }

  public OCacheEntry load(long fileId, long pageIndex) {
    return null;
  }

  public void clear(long fileId) {
  }

  public void clear() {
  }

  private static final class FileLockKey implements Comparable<FileLockKey> {
    private final long fileId;
    private final long pageIndex;

    private FileLockKey(long fileId, long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      FileLockKey that = (FileLockKey) o;

      if (fileId != that.fileId)
        return false;
      if (pageIndex != that.pageIndex)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (fileId ^ (fileId >>> 32));
      result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
      return result;
    }

    @Override
    public int compareTo(FileLockKey otherKey) {
      if (fileId > otherKey.fileId)
        return 1;
      if (fileId < otherKey.fileId)
        return -1;

      if (pageIndex > otherKey.pageIndex)
        return 1;
      if (pageIndex < otherKey.pageIndex)
        return -1;

      return 0;
    }
  }

}
