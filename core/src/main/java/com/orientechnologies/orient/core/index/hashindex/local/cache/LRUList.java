package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.util.Iterator;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public interface LRUList extends Iterable<OCacheEntry> {
  OCacheEntry get(long fileId, long pageIndex);

  OCacheEntry remove(long fileId, long pageIndex);

  void putToMRU(OCacheEntry cacheEntry);

  void clear();

  boolean contains(long fileId, long filePosition);

  int size();

  OCacheEntry removeLRU();

	OCacheEntry getLRU();

  @Override
  Iterator<OCacheEntry> iterator();
}
