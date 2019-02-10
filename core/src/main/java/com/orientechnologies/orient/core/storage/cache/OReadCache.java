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
package com.orientechnologies.orient.core.storage.cache;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

import java.io.IOException;

/**
 * This class is heart of OrientDB storage model it presents disk backed data cache which works with direct memory.
 * <p>
 * Model of this cache is based on page model. All direct memory area is mapped to disk files and each file is split on pages. Page
 * is smallest unit of work. The amount of RAM which can be used for data manipulation is limited so only a subset of data will be
 * really loaded into RAM on demand, if there is not enough RAM to store all data, part of them will by flushed to the disk. If disk
 * cache is closed all changes will be flushed to the disk.
 * <p>
 * Typical steps if you work with disk cache are following:
 * <ol>
 * <li>Open file using {@link #openFile(String, OWriteCache)} method</li>
 * <li>Remember id of opened file</li>
 * <li>Load page which you want to use to write data using method {@link #load(long, long, boolean, OWriteCache, int)}</li>
 * <li>Get pointer to the memory page {@link OCacheEntry#getCachePointer()}</li>
 * <li>Lock allocated page for writes {@link OCachePointer#acquireExclusiveLock()}</li>
 * <li>Get pointer to the direct memory which is allocated to hold page data {@link OCachePointer#getDataPointer()}</li>
 * <li>Change page content as you wish.</li>
 * <li>Release page write lock {@link OCachePointer#releaseExclusiveLock()}</li>
 * <li>Mark page as dirty so it will be flushed eventually to the disk {@link OCacheEntry#markDirty()}</li>
 * <li>Put page back to the cache {@link #release(OCacheEntry, OWriteCache)}</li>
 * </ol>
 * <p>
 * If you wish to read data, not change them, you use the same steps but:
 * <ol>
 * <li>Acquire read lock instead of write lock using {@link OCachePointer#acquireSharedLock()}</li> method.
 * <li>Do not mark page as dirty</li>
 * </ol>
 * <p>
 * If you want to add new data but not to change existing one and you do not have enough space to add new data use method
 * {@link #allocateNewPage(long, OWriteCache)} instead of {@link #load(long, long, boolean, OWriteCache, int)}.
 * <p>
 * {@link #load(long, long, boolean, OWriteCache, int)} method has checkPinnedPages parameter. Pinned pages are pages which are kept
 * always loaded in RAM ,this class of pages is needed for some data structures usually this attribute should be set to
 * <code>false</code> and it is set to <code>true</code> when storage goes through data restore procedure after system crash.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 14.03.13
 */
public interface OReadCache {
  long addFile(String fileName, OWriteCache writeCache) throws IOException;

  long addFile(String fileName, long fileId, OWriteCache writeCache) throws IOException;

  OCacheEntry loadForWrite(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount,
      boolean verifyChecksums, OLogSequenceNumber startLSN) throws IOException;

  OCacheEntry loadForRead(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount,
      boolean verifyChecksums) throws IOException;

  void releaseFromRead(OCacheEntry cacheEntry, OWriteCache writeCache);

  void releaseFromWrite(OCacheEntry cacheEntry, OWriteCache writeCache);

  void pinPage(OCacheEntry cacheEntry, OWriteCache writeCache);

  OCacheEntry allocateNewPage(long fileId, OWriteCache writeCache, OLogSequenceNumber startLSN)
      throws IOException;

  long getUsedMemory();

  void clear();

  void truncateFile(long fileId, OWriteCache writeCache) throws IOException;

  void closeFile(long fileId, boolean flush, OWriteCache writeCache);

  void deleteFile(long fileId, OWriteCache writeCache) throws IOException;

  void deleteStorage(OWriteCache writeCache) throws IOException;

  /**
   * Closes all files inside of write cache and flushes all associated data.
   *
   * @param writeCache Write cache to close.
   */
  void closeStorage(OWriteCache writeCache) throws IOException;

  /**
   * Load state of cache from file system if possible.
   *
   * @param writeCache Write cache is used to load pages back into cache if possible.
   */
  void loadCacheState(OWriteCache writeCache);

  /**
   * Stores state of cache inside file if possible.
   *
   * @param writeCache Write cache which manages files cache state of which is going to be stored.
   */
  void storeCacheState(OWriteCache writeCache);

  void changeMaximumAmountOfMemory(long calculateReadCacheMaxMemory);
}
