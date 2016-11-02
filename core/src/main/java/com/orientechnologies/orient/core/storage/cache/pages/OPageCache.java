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
 * Defines the contract for page caches which act as a buffer between some page source and the consumer of the pages. Typically
 * instances of this interface are backed by the underlying {@link OReadCache} and {@link OWriteCache} instances.
 *
 * @author Sergey Sitnikov
 */
public interface OPageCache {

  /**
   * Loads the page.
   *
   * @param fileId           the page's file id to load from.
   * @param pageIndex        the page's index in the file.
   * @param checkPinnedPages {@code true} to inspect the pinned pages in the underlying {@link OReadCache}, {@code false} to skip
   *                         them.
   * @param writeCache       the underlying write cache.
   * @param pageCount        the number of pages to load.
   *
   * @return the loaded page or {@code null} if page is not found neither in this page cache nor underlying {@link OReadCache}.
   *
   * @throws IOException if there was an I/O error.
   * @see OReadCache#load(long, long, boolean, OWriteCache, int)
   */
  OCacheEntry loadPage(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount)
      throws IOException;

  /**
   * Releases the page.
   *
   * @param cacheEntry the page to release.
   * @param writeCache the underlying write cache instance.
   *
   * @see OReadCache#release(OCacheEntry, OWriteCache)
   */
  void releasePage(OCacheEntry cacheEntry, OWriteCache writeCache);

  /**
   * Releases the pages associated with the given file.
   *
   * @param fileId     the file id of the file.
   * @param writeCache the underlying write cache instance.
   */
  void releaseFilePages(long fileId, OWriteCache writeCache);

  /**
   * Purges the page from this page cache, but not from the underlying {@link OReadCache read cache}. On return, there are no
   * references to the purged page in a context of this page cache, but the page is still present in the underlying read cache.
   *
   * @param fileId     the page's file id.
   * @param pageIndex  the page's index in the file.
   * @param writeCache the underlying write cache instance.
   *
   * @return the purged page or {@code null} if the page is not cached by this page cache.
   */
  OCacheEntry purgePage(long fileId, long pageIndex, OWriteCache writeCache);

  /**
   * Resets this page cache by releasing all cached pages.
   *
   * @param writeCache the underlying write cache instance.
   */
  void reset(OWriteCache writeCache);

}
