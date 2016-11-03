/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.cache.pages;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * The implementation of a page cache based on the LRU algorithm. Built on a linked hash map, on a page load request always evicts
 * the least recently accessed page.
 *
 * @author Sergey Sitnikov
 */
public class OLruPageCache implements OPageCache {

  private final OReadCache readCache;
  private final int        maximumSize;
  private final int        evictionBatchSize;

  private final LinkedHashMap<Key, Record> pages;

  /**
   * Constructs a new LRU page cache instance.
   *
   * @param readCache         the underlying read cache.
   * @param maximumSize       the maximum cache size in pages.
   * @param evictionBatchSize the eviction batch size, the cache may overgrow by this much.
   */
  public OLruPageCache(OReadCache readCache, int maximumSize, int evictionBatchSize) {
    this.readCache = readCache;
    this.maximumSize = maximumSize;
    this.evictionBatchSize = evictionBatchSize;

    this.pages = new LinkedHashMap<Key, Record>(maximumSize * 2, 0.75F, true);
  }

  @Override
  public OCacheEntry loadPage(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount)
      throws IOException {
    final Key key = new Key(fileId, pageIndex);
    final Record record = pages.get(key);

    if (record != null) {
      ++record.usages;
      return record.cacheEntry;
    }

    final OCacheEntry cacheEntry = readCache.load(fileId, pageIndex, checkPinnedPages, writeCache, pageCount);
    if (cacheEntry == null)
      return null;

    if (pages.size() - maximumSize < evictionBatchSize)
      pages.put(key, new Record(cacheEntry));
    else {
      for (final Iterator<Record> i = pages.values().iterator(); i.hasNext(); ) {
        final Record r = i.next();
        if (r.usages == 0) {
          readCache.release(r.cacheEntry, writeCache);
          i.remove();

          if (pages.size() < maximumSize) {
            pages.put(key, new Record(cacheEntry));
            break;
          }
        }
      }
    }

    return cacheEntry;
  }

  @Override
  public long releasePage(OCacheEntry cacheEntry, OWriteCache writeCache) {
    final Key key = new Key(cacheEntry.getFileId(), cacheEntry.getPageIndex());
    final Record record = pages.get(key);
    if (record == null) {
      readCache.release(cacheEntry, writeCache);
      return NOT_CACHED;
    } else
      return --record.usages;
  }

  @Override
  public void releaseFilePages(long fileId, OWriteCache writeCache) {
    for (final Iterator<Record> i = pages.values().iterator(); i.hasNext(); ) {
      final Record r = i.next();
      if (fileId == r.cacheEntry.getFileId()) {
        assert r.usages == 0;
        readCache.release(r.cacheEntry, writeCache);
        i.remove();
        break;
      }
    }
  }

  @Override
  public OCacheEntry purgePage(long fileId, long pageIndex) {
    final Key key = new Key(fileId, pageIndex);
    final Record record = pages.remove(key);
    assert record == null || record.usages == 0;
    return record == null ? null : record.cacheEntry;
  }

  @Override
  public void reset(OWriteCache writeCache) {
    for (Record r : pages.values()) {
      assert r.usages == 0;
      readCache.release(r.cacheEntry, writeCache);
    }
    pages.clear();
  }

  private static class Key {
    public final long fileId;
    public final long pageIndex;

    public Key(long fileId, long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public int hashCode() {
      return (int) ((fileId ^ (fileId >>> 32)) ^ pageIndex);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass") // the class is private, we are in control
    @Override
    public boolean equals(Object obj) {
      if (obj == null)
        return false;

      final Key other = (Key) obj;
      return pageIndex == other.pageIndex && fileId == other.fileId;
    }
  }

  private static class Record {
    public final OCacheEntry cacheEntry;

    public long usages = 1;

    public Record(OCacheEntry cacheEntry) {
      this.cacheEntry = cacheEntry;
    }
  }

}
