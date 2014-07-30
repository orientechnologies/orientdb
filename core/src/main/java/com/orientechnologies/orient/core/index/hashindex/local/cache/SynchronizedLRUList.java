package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.util.Iterator;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class SynchronizedLRUList implements LRUList {
  private final LRUList underlying = new HashLRUList();

  @Override
  public synchronized OCacheEntry get(long fileId, long pageIndex) {
    return underlying.get(fileId, pageIndex);
  }

  @Override
  public synchronized OCacheEntry remove(long fileId, long pageIndex) {
    return underlying.remove(fileId, pageIndex);
  }

  @Override
  public synchronized void putToMRU(OCacheEntry cacheEntry) {
    underlying.putToMRU(cacheEntry);
  }

  @Override
  public synchronized void clear() {
    underlying.clear();
  }

  @Override
  public synchronized boolean contains(long fileId, long filePosition) {
    return underlying.contains(fileId, filePosition);
  }

  @Override
  public synchronized int size() {
    return underlying.size();
  }

  @Override
  public synchronized OCacheEntry removeLRU() {
    return underlying.removeLRU();
  }

  @Override
  public synchronized OCacheEntry getLRU() {
    return underlying.getLRU();
  }

  @Override
  public synchronized Iterator<OCacheEntry> iterator() {
    return underlying.iterator();
  }
}
