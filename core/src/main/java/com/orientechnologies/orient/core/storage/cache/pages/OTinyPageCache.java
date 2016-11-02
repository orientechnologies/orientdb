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

/**
 * The implementation of a page cache suitable for page caches of a very tiny size, like 1-8 pages. Built on arrays, on a page load
 * request always evicts the page with the lowest observed hits.
 *
 * @author Sergey Sitnikov
 */
public class OTinyPageCache implements OPageCache {

  private final OReadCache readCache;

  private final OCacheEntry[] pages;
  private final long[]        counters;

  /**
   * Constructs a new tiny page cache instance.
   *
   * @param readCache   the underlying read cache.
   * @param maximumSize the maximum cache size in pages.
   */
  public OTinyPageCache(OReadCache readCache, int maximumSize) {
    this.readCache = readCache;

    this.pages = new OCacheEntry[maximumSize];
    this.counters = new long[maximumSize * 2];
  }

  @Override
  public OCacheEntry loadPage(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount)
      throws IOException {
    long victimHits = Long.MAX_VALUE;
    int victimIndex = -1;

    for (int i = 0; i < pages.length; ++i) {
      final OCacheEntry entry = pages[i];

      if (entry == null) { // free slot found
        victimIndex = i;
        victimHits = 0; // mark as free
        continue;
      }

      final int countersBase = i * 2;
      assert counters[countersBase + 1] >= 1;

      if (pageIndex == entry.getPageIndex() && fileId == entry.getFileId()) { // found in cache
        ++counters[countersBase];
        ++counters[countersBase + 1];
        return entry;
      }

      if (victimHits != 0 /* still no free slot found */ && counters[countersBase] <= 0 /* no usages */) {
        final long hits = counters[countersBase + 1];
        if (hits < victimHits) {
          victimIndex = i;
          victimHits = hits;
        }
      }
    }

    final OCacheEntry entry = readCache.load(fileId, pageIndex, checkPinnedPages, writeCache, pageCount);
    if (entry == null)
      return null;

    if (victimIndex != -1) { // slot found
      final int countersBase = victimIndex * 2;

      if (victimHits != 0) { // slot is not free
        final OCacheEntry victim = pages[victimIndex];
        for (long i = counters[countersBase]; i <= 0; ++i)
          readCache.release(victim, writeCache);
      }

      pages[victimIndex] = entry;
      counters[countersBase] = 1;
      counters[countersBase + 1] = 1;
    }

    return entry;
  }

  @Override
  public void releasePage(OCacheEntry cacheEntry, OWriteCache writeCache) {
    for (int i = 0; i < pages.length; ++i)
      if (pages[i] == cacheEntry) { // found in cache
        --counters[i * 2];
        return;
      }

    readCache.release(cacheEntry, writeCache); // not cached
  }

  @Override
  public void releaseFilePages(long fileId, OWriteCache writeCache) {
    for (int i = 0; i < pages.length; ++i) {
      final OCacheEntry entry = pages[i];

      if (entry != null && fileId == entry.getFileId()) {
        assert counters[i * 2] <= 0;
        pages[i] = null;
        for (long j = counters[i * 2]; j <= 0; ++j)
          readCache.release(entry, writeCache);
      }
    }
  }

  @Override
  public OCacheEntry purgePage(long fileId, long pageIndex, OWriteCache writeCache) {
    for (int i = 0; i < pages.length; ++i) {
      final OCacheEntry entry = pages[i];

      if (entry != null && pageIndex == entry.getPageIndex() && fileId == entry.getFileId()) {
        assert counters[i * 2] <= 0;
        pages[i] = null;
        for (long j = counters[i * 2]; j < 0; ++j)
          readCache.release(entry, writeCache);
        return entry;
      }
    }

    return null;
  }

  @Override
  public void reset(OWriteCache writeCache) {
    for (int i = 0; i < pages.length; ++i) {
      final OCacheEntry entry = pages[i];

      if (entry != null) {
        assert counters[i * 2] <= 0;
        pages[i] = null;
        for (long j = counters[i * 2]; j <= 0; ++j)
          readCache.release(entry, writeCache);
      }
    }
  }

}
