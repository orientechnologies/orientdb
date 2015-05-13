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

import java.io.IOException;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import com.orientechnologies.orient.core.storage.impl.memory.ODirectMemoryStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

/**
 * Base class for all durable data structures, that is data structures state of which can be consistently restored after system
 * crash but results of last operations in small interval before crash may be lost.
 * 
 * This class contains methods which are used to support such concepts as:
 * <ol>
 * <li>"atomic operation" - set of operations which should be either applied together or not. It includes not only changes on
 * current data structure but on all durable data structures which are used by current one during implementation of specific
 * operation.</li>
 * <li>write ahead log - log of all changes which were done with page content after loading it from cache.</li>
 * </ol>
 * 
 * 
 * To support of "atomic operation" concept following should be done:
 * <ol>
 * <li>Call {@link #startAtomicOperation()} method.</li>
 * <li>If changes should be isolated till operation completes call also {@link #lockTillAtomicOperationCompletes()} which will apply
 * exclusive lock on existing data structure till atomic operation completes. This lock is not replacement of data structure locking
 * system it is used to serialize access to set of components which participate in single atomic operation. It is kind of rudiment
 * lock manager which is used to isolate access to units which participate in single transaction and which is going to be created to
 * provide efficient multi core scalability feature. It is recommended to always call it just after start of atomic operation but
 * always remember it is not replacement of thread safety mechanics for current data structure it is a mean to provide isolation
 * between atomic operations.</li>
 * <li>Call {@link #endAtomicOperation(boolean)} method when atomic operation completes, passed in parameter should be
 * <code>false</code> if atomic operation completes with success and <code>true</code> if there were some exceptions and it is
 * needed to rollback given operation.</li>
 * </ol>
 * 
 * 
 * @author Andrey Lomakin
 * @since 8/27/13
 */
public abstract class ODurableComponent extends OSharedResourceAdaptive {
  protected final OAtomicOperationsManager  atomicOperationsManager;
  protected final OAbstractPaginatedStorage storage;

  public ODurableComponent(OAbstractPaginatedStorage storage) {
    super(true);

    this.storage = storage;
    this.atomicOperationsManager = storage.getAtomicOperationsManager();
  }

  @Override
  protected void acquireExclusiveLock() {
    super.acquireExclusiveLock();
  }

  protected void endAtomicOperation(boolean rollback) throws IOException {
    atomicOperationsManager.endAtomicOperation(rollback);
  }

  protected OAtomicOperation startAtomicOperation() throws IOException {
    return atomicOperationsManager.startAtomicOperation(this);
  }

  protected static OWALChangesTree getChangesTree(OAtomicOperation atomicOperation, OCacheEntry entry) {
    if (atomicOperation == null)
      return null;

    return atomicOperation.getChangesTree(entry.getFileId(), entry.getPageIndex());
  }

  protected static long getFilledUpTo(OAtomicOperation atomicOperation, ODiskCache diskCache, long fileId) throws IOException {
    if (atomicOperation == null)
      return diskCache.getFilledUpTo(fileId);

    return atomicOperation.filledUpTo(fileId, diskCache);
  }

  protected static OCacheEntry loadPage(OAtomicOperation atomicOperation, long fileId, long pageIndex, boolean checkPinnedPages,
      ODiskCache diskCache) throws IOException {
    if (atomicOperation == null)
      return diskCache.load(fileId, pageIndex, checkPinnedPages);

    return atomicOperation.loadPage(fileId, pageIndex, diskCache, checkPinnedPages);
  }

  protected static void pinPage(OAtomicOperation atomicOperation, OCacheEntry cacheEntry, ODiskCache diskCache) throws IOException {
    if (atomicOperation == null)
      diskCache.pinPage(cacheEntry);
    else
      atomicOperation.pinPage(cacheEntry);
  }

  protected static OCacheEntry addPage(OAtomicOperation atomicOperation, long fileId, ODiskCache diskCache) throws IOException {
    if (atomicOperation == null)
      return diskCache.allocateNewPage(fileId);

    return atomicOperation.addPage(fileId, diskCache);
  }

  protected static void releasePage(OAtomicOperation atomicOperation, OCacheEntry cacheEntry, ODiskCache diskCache) {
    if (atomicOperation == null)
      diskCache.release(cacheEntry);
    else
      atomicOperation.releasePage(cacheEntry, diskCache);
  }

  protected static long addFile(OAtomicOperation atomicOperation, String fileName, ODiskCache diskCache) throws IOException {
    if (atomicOperation == null)
      return diskCache.addFile(fileName);

    return atomicOperation.addFile(fileName, diskCache);
  }

  protected static long openFile(OAtomicOperation atomicOperation, String fileName, ODiskCache diskCache) throws IOException {
    if (atomicOperation == null)
      return diskCache.openFile(fileName);

    return atomicOperation.openFile(fileName, diskCache);
  }

  protected static void openFile(OAtomicOperation atomicOperation, long fileId, ODiskCache diskCache) throws IOException {
    if (atomicOperation == null)
      diskCache.openFile(fileId);
    else
      atomicOperation.openFile(fileId, diskCache);
  }

  protected static void deleteFile(OAtomicOperation atomicOperation, long fileId, ODiskCache diskCache) throws IOException {
    if (atomicOperation == null)
      diskCache.deleteFile(fileId);
    else
      atomicOperation.deleteFile(fileId, diskCache);
  }

  protected static boolean isFileExists(OAtomicOperation atomicOperation, String fileName, ODiskCache diskCache) {
    if (atomicOperation == null)
      return diskCache.exists(fileName);

    return atomicOperation.isFileExists(fileName, diskCache);
  }

  protected static boolean isFileExists(OAtomicOperation atomicOperation, long fileId, ODiskCache diskCache) {
    if (atomicOperation == null)
      return diskCache.exists(fileId);

    return atomicOperation.isFileExists(fileId, diskCache);
  }

  protected static String fileNameById(OAtomicOperation atomicOperation, long fileId, ODiskCache diskCache) {
    if (atomicOperation == null)
      return diskCache.fileNameById(fileId);

    return atomicOperation.fileNameById(fileId, diskCache);
  }

  protected static void truncateFile(OAtomicOperation atomicOperation, long filedId, ODiskCache diskCache) throws IOException {
    if (atomicOperation == null)
      diskCache.truncateFile(filedId);
    else
      atomicOperation.truncateFile(filedId);
  }
}
