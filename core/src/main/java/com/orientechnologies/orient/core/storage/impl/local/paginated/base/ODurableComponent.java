/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.base;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.io.IOException;

/**
 * Base class for all durable data structures, that is data structures state of which can be consistently restored after system
 * crash but results of last operations in small interval before crash may be lost.
 * <p>
 * This class contains methods which are used to support such concepts as:
 * <ol>
 * <li>"atomic operation" - set of operations which should be either applied together or not. It includes not only changes on
 * current data structure but on all durable data structures which are used by current one during implementation of specific
 * operation.</li>
 * <li>write ahead log - log of all changes which were done with page content after loading it from cache.</li>
 * </ol>
 * <p>
 * <p>
 * To support of "atomic operation" concept following should be done:
 * <ol>
 * <li>Call {@link #startAtomicOperation(boolean)} method.</li>
 * <li>Call {@link #endAtomicOperation(boolean, Exception)} method when atomic operation completes, passed in parameter should be
 * <code>false</code> if atomic operation completes with success and <code>true</code> if there were some exceptions and it is
 * needed to rollback given operation.</li>
 * </ol>
 *
 * @author Andrey Lomakin
 * @since 8/27/13
 */
public abstract class ODurableComponent extends OSharedResourceAdaptive {
  protected final OAtomicOperationsManager     atomicOperationsManager;
  protected final OAbstractPaginatedStorage    storage;
  protected final OReadCache                   readCache;
  protected final OWriteCache                  writeCache;
  private final   OPerformanceStatisticManager performanceStatisticManager;

  private volatile String name;
  private volatile String fullName;

  protected final String extension;

  private volatile String lockName;

  public ODurableComponent(OAbstractPaginatedStorage storage, String name, String extension, String lockName) {
    super(true);

    assert name != null;
    this.extension = extension;
    this.storage = storage;
    this.fullName = name + extension;
    this.name = name;
    this.atomicOperationsManager = storage.getAtomicOperationsManager();
    this.readCache = storage.getReadCache();
    this.writeCache = storage.getWriteCache();
    this.performanceStatisticManager = storage.getPerformanceStatisticManager();
    this.lockName = lockName;

    performanceStatisticManager.registerComponent(fullName);
  }

  public String getLockName() {
    return lockName;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
    this.fullName = name + extension;
  }

  public String getFullName() {
    return fullName;
  }

  public String getExtension() {
    return extension;
  }

  @Override
  protected void acquireExclusiveLock() {
    super.acquireExclusiveLock();
  }

  protected void endAtomicOperation(boolean rollback, Exception e) throws IOException {
    atomicOperationsManager.endAtomicOperation(rollback, e);
  }

  /**
   * @see OAtomicOperationsManager#startAtomicOperation(com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent, boolean)
   */
  protected OAtomicOperation startAtomicOperation(boolean trackNonTxOperations) throws IOException {
    return atomicOperationsManager.startAtomicOperation(this, trackNonTxOperations);
  }

  protected OWALChanges getChanges(OAtomicOperation atomicOperation, OCacheEntry entry) {
    if (atomicOperation == null)
      return null;

    return atomicOperation.getChanges(entry.getFileId(), entry.getPageIndex());
  }

  protected long getFilledUpTo(OAtomicOperation atomicOperation, long fileId) throws IOException {
    if (atomicOperation == null)
      return writeCache.getFilledUpTo(fileId);

    return atomicOperation.filledUpTo(fileId);
  }

  protected OCacheEntry loadPage(final OAtomicOperation atomicOperation, final long fileId, final long pageIndex,
      final boolean checkPinnedPages) throws IOException {
    return loadPage(atomicOperation, fileId, pageIndex, checkPinnedPages, 1);
  }

  protected OCacheEntry loadPage(OAtomicOperation atomicOperation, long fileId, long pageIndex, boolean checkPinnedPages,
      final int pageCount) throws IOException {
    if (atomicOperation == null)
      return readCache.load(fileId, pageIndex, checkPinnedPages, writeCache, pageCount);

    return atomicOperation.loadPage(fileId, pageIndex, checkPinnedPages, pageCount);
  }

  protected void pinPage(OAtomicOperation atomicOperation, OCacheEntry cacheEntry) throws IOException {
    if (atomicOperation == null)
      readCache.pinPage(cacheEntry);
    else
      atomicOperation.pinPage(cacheEntry);
  }

  protected OCacheEntry addPage(OAtomicOperation atomicOperation, long fileId) throws IOException {
    if (atomicOperation == null)
      return readCache.allocateNewPage(fileId, writeCache);

    return atomicOperation.addPage(fileId);
  }

  protected void releasePage(OAtomicOperation atomicOperation, OCacheEntry cacheEntry) {
    if (atomicOperation == null)
      readCache.release(cacheEntry, writeCache);
    else
      atomicOperation.releasePage(cacheEntry);
  }

  protected long addFile(OAtomicOperation atomicOperation, String fileName) throws IOException {
    if (atomicOperation == null)
      return readCache.addFile(fileName, writeCache);

    return atomicOperation.addFile(fileName);
  }

  protected long openFile(OAtomicOperation atomicOperation, String fileName) throws IOException {
    if (atomicOperation == null)
      return readCache.openFile(fileName, writeCache);

    return atomicOperation.openFile(fileName);
  }

  protected void openFile(OAtomicOperation atomicOperation, long fileId) throws IOException {
    if (atomicOperation == null)
      readCache.openFile(fileId, writeCache);
    else
      atomicOperation.openFile(fileId);
  }

  protected void deleteFile(OAtomicOperation atomicOperation, long fileId) throws IOException {
    if (atomicOperation == null)
      readCache.deleteFile(fileId, writeCache);
    else
      atomicOperation.deleteFile(fileId);
  }

  protected boolean isFileExists(OAtomicOperation atomicOperation, String fileName) {
    if (atomicOperation == null)
      return writeCache.exists(fileName);

    return atomicOperation.isFileExists(fileName);
  }

  protected boolean isFileExists(OAtomicOperation atomicOperation, long fileId) {
    if (atomicOperation == null)
      return writeCache.exists(fileId);

    return atomicOperation.isFileExists(fileId);
  }

  protected void truncateFile(OAtomicOperation atomicOperation, long filedId) throws IOException {
    if (atomicOperation == null)
      readCache.truncateFile(filedId, writeCache);
    else
      atomicOperation.truncateFile(filedId);
  }

  protected void startOperation() {
    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = performanceStatisticManager
        .getSessionPerformanceStatistic();
    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic.startComponentOperation(getFullName());
    }
  }

  protected void completeOperation() {
    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = performanceStatisticManager
        .getSessionPerformanceStatistic();
    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic.completeComponentOperation();
    }
  }

}
