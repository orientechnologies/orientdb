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
 * The no-cache passthrough implementation of a page cache, caches nothing.
 *
 * @author Sergey Sitnikov
 */
public class OPassthroughPageCache implements OPageCache {

  private final OReadCache readCache;

  /**
   * Constructs a new passthrough page cache instance.
   *
   * @param readCache the underlying read cache.
   */
  public OPassthroughPageCache(OReadCache readCache) {
    this.readCache = readCache;
  }

  @Override
  public OCacheEntry loadPage(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount)
      throws IOException {
    return readCache.load(fileId, pageIndex, checkPinnedPages, writeCache, pageCount);
  }

  @Override
  public void releasePage(OCacheEntry cacheEntry, OWriteCache writeCache) {
    readCache.release(cacheEntry, writeCache);
  }

  @Override
  public void releaseFilePages(long fileId, OWriteCache writeCache) {
    // do nothing
  }

  @Override
  public OCacheEntry purgePage(long fileId, long pageIndex, OWriteCache writeCache) {
    return null; // do nothing
  }

  @Override
  public void reset(OWriteCache writeCache) {
    // do nothing
  }

}
