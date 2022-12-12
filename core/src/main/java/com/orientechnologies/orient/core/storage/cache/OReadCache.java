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
 * This class is heart of OrientDB storage model it presents disk backed data cache which works with
 * direct memory.
 *
 * <p>Model of this cache is based on page model. All direct memory area is mapped to disk files and
 * each file is split on pages. Page is smallest unit of work. The amount of RAM which can be used
 * for data manipulation is limited so only a subset of data will be really loaded into RAM on
 * demand, if there is not enough RAM to store all data, part of them will by flushed to the disk.
 * If disk cache is closed all changes will be flushed to the disk.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 14.03.13
 */
public interface OReadCache {
  /**
   * Minimum size of memory which may be allocated by cache (in pages). This parameter is used only
   * if related flag is set in constrictor of cache.
   */
  int MIN_CACHE_SIZE = 256;

  long addFile(String fileName, OWriteCache writeCache) throws IOException;

  long addFile(String fileName, long fileId, OWriteCache writeCache) throws IOException;

  OCacheEntry loadForWrite(
      long fileId,
      long pageIndex,
      OWriteCache writeCache,
      boolean verifyChecksums,
      OLogSequenceNumber startLSN)
      throws IOException;

  OCacheEntry loadForRead(
      long fileId, long pageIndex, OWriteCache writeCache, boolean verifyChecksums)
      throws IOException;

  OCacheEntry silentLoadForRead(
      final long extFileId,
      final int pageIndex,
      final OWriteCache writeCache,
      final boolean verifyChecksums);

  void releaseFromRead(OCacheEntry cacheEntry);

  void releaseFromWrite(OCacheEntry cacheEntry, OWriteCache writeCache, boolean changed);

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

  void changeMaximumAmountOfMemory(long calculateReadCacheMaxMemory);
}
