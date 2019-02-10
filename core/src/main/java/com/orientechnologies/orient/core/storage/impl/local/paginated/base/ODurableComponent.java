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

package com.orientechnologies.orient.core.storage.impl.local.paginated.base;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;

import java.io.IOException;

/**
 * Base class for all durable data structures, that is data structures state of which can be consistently restored after system
 * crash but results of last operations in small interval before crash may be lost.
 * This class contains methods which are used to support such concepts as:
 * <ol>
 * <li>"atomic operation" - set of operations which should be either applied together or not. It includes not only changes on
 * current data structure but on all durable data structures which are used by current one during implementation of specific
 * operation.</li>
 * <li>write ahead log - log of all changes which were done with page content after loading it from cache.</li>
 * </ol>
 * To support of "atomic operation" concept following should be done:
 * <ol>
 * <li>Call {@link #startAtomicOperation(boolean)} method.</li>
 * <li>Call {@link #endAtomicOperation(boolean)} method when atomic operation completes, passed in parameter should be
 * <code>false</code> if atomic operation completes with success and <code>true</code> if there were some exceptions and it is
 * needed to rollback given operation.</li>
 * </ol>
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/27/13
 */
public abstract class ODurableComponent extends OSharedResourceAdaptive {
  protected final OAtomicOperationsManager  atomicOperationsManager;
  protected final OAbstractPaginatedStorage storage;
  protected final OReadCache                readCache;
  protected final OWriteCache               writeCache;

  private volatile String name;
  private volatile String fullName;

  private final String extension;

  private final String lockName;

  public ODurableComponent(final OAbstractPaginatedStorage storage, final String name, final String extension,
      final String lockName) {
    super(true);

    assert name != null;
    this.extension = extension;
    this.storage = storage;
    this.fullName = name + extension;
    this.name = name;
    this.atomicOperationsManager = storage.getAtomicOperationsManager();
    this.readCache = storage.getReadCache();
    this.writeCache = storage.getWriteCache();
    this.lockName = lockName;
  }

  public String getLockName() {
    return lockName;
  }

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = name;
    this.fullName = name + extension;
  }

  public String getFullName() {
    return fullName;
  }

  public String getExtension() {
    return extension;
  }

  protected void endAtomicOperation(final boolean rollback) throws IOException {
    atomicOperationsManager.endAtomicOperation(rollback);
  }

  /**
   * @see OAtomicOperationsManager#startAtomicOperation(com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent,
   * boolean)
   */
  protected OAtomicOperation startAtomicOperation(final boolean trackNonTxOperations) throws IOException {
    return atomicOperationsManager.startAtomicOperation(this, trackNonTxOperations);
  }

  protected long getFilledUpTo(final OAtomicOperation atomicOperation, final long fileId) {
    if (atomicOperation == null) {
      return writeCache.getFilledUpTo(fileId);
    }

    return atomicOperation.filledUpTo(fileId);
  }

  protected OCacheEntry loadPageForWrite(final OAtomicOperation atomicOperation, final long fileId, final long pageIndex,
      final boolean checkPinnedPages, final boolean verifyCheckSum) throws IOException {
    if (atomicOperation == null) {
      return readCache.loadForWrite(fileId, pageIndex, checkPinnedPages, writeCache, 1, true, null);
    }

    return atomicOperation.loadPageForWrite(fileId, pageIndex, checkPinnedPages, 1, verifyCheckSum);
  }

  protected OCacheEntry loadPageForRead(final OAtomicOperation atomicOperation, final long fileId, final long pageIndex,
      final boolean checkPinnedPages) throws IOException {
    return loadPageForRead(atomicOperation, fileId, pageIndex, checkPinnedPages, 1);
  }

  protected OCacheEntry loadPageForRead(final OAtomicOperation atomicOperation, final long fileId, final long pageIndex,
      final boolean checkPinnedPages, final int pageCount) throws IOException {
    if (atomicOperation == null) {
      return readCache.loadForRead(fileId, pageIndex, checkPinnedPages, writeCache, pageCount, true);
    }

    return atomicOperation.loadPageForRead(fileId, pageIndex, checkPinnedPages, pageCount);
  }

  protected void pinPage(final OAtomicOperation atomicOperation, final OCacheEntry cacheEntry) {
    if (atomicOperation == null) {
      readCache.pinPage(cacheEntry, writeCache);
    } else {
      atomicOperation.pinPage(cacheEntry);
    }
  }

  protected OCacheEntry addPage(final OAtomicOperation atomicOperation, final long fileId) throws IOException {
    if (atomicOperation == null) {
      return readCache.allocateNewPage(fileId, writeCache, null);
    }

    return atomicOperation.addPage(fileId);
  }

  protected void releasePageFromWrite(final OAtomicOperation atomicOperation, final OCacheEntry cacheEntry) {
    if (atomicOperation == null) {
      readCache.releaseFromWrite(cacheEntry, writeCache);
    } else {
      atomicOperation.releasePageFromWrite(cacheEntry);
    }
  }

  protected void releasePageFromRead(final OAtomicOperation atomicOperation, final OCacheEntry cacheEntry) {
    if (atomicOperation == null) {
      readCache.releaseFromRead(cacheEntry, writeCache);
    } else {
      atomicOperation.releasePageFromRead(cacheEntry);
    }
  }

  protected long addFile(final OAtomicOperation atomicOperation, final String fileName) throws IOException {
    if (atomicOperation == null) {
      return readCache.addFile(fileName, writeCache);
    }

    return atomicOperation.addFile(fileName);
  }

  protected long openFile(final OAtomicOperation atomicOperation, final String fileName) throws IOException {
    if (atomicOperation == null) {
      return writeCache.loadFile(fileName);
    }

    return atomicOperation.loadFile(fileName);
  }

  protected void deleteFile(final OAtomicOperation atomicOperation, final long fileId) throws IOException {
    if (atomicOperation == null) {
      readCache.deleteFile(fileId, writeCache);
    } else {
      atomicOperation.deleteFile(fileId);
    }
  }

  protected boolean isFileExists(final OAtomicOperation atomicOperation, final String fileName) {
    if (atomicOperation == null) {
      return writeCache.exists(fileName);
    }

    return atomicOperation.isFileExists(fileName);
  }

  protected void truncateFile(final OAtomicOperation atomicOperation, final long filedId) throws IOException {
    if (atomicOperation == null) {
      readCache.truncateFile(filedId, writeCache);
    } else {
      atomicOperation.truncateFile(filedId);
    }
  }
}
