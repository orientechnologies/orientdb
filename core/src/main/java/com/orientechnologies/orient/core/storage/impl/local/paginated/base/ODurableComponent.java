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
import com.orientechnologies.common.function.TxConsumer;
import com.orientechnologies.common.function.TxFunction;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import java.io.IOException;

/**
 * Base class for all durable data structures, that is data structures state of which can be
 * consistently restored after system crash but results of last operations in small interval before
 * crash may be lost.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/27/13
 */
public abstract class ODurableComponent extends OSharedResourceAdaptive {
  protected final OAtomicOperationsManager atomicOperationsManager;
  protected final OAbstractPaginatedStorage storage;
  protected final OReadCache readCache;
  protected final OWriteCache writeCache;

  private volatile String name;
  private volatile String fullName;

  private final String extension;

  private final String lockName;

  public ODurableComponent(
      final OAbstractPaginatedStorage storage,
      final String name,
      final String extension,
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

  protected <T> T calculateInsideComponentOperation(
      final OAtomicOperation atomicOperation, final TxFunction<T> function) {
    return atomicOperationsManager.calculateInsideComponentOperation(
        atomicOperation, this, function);
  }

  protected void executeInsideComponentOperation(
      final OAtomicOperation operation, final TxConsumer consumer) {
    atomicOperationsManager.executeInsideComponentOperation(operation, this, consumer);
  }

  protected boolean tryExecuteInsideComponentOperation(
      final OAtomicOperation operation, final TxConsumer consumer) {
    return atomicOperationsManager.tryExecuteInsideComponentOperation(operation, this, consumer);
  }

  protected long getFilledUpTo(final OAtomicOperation atomicOperation, final long fileId) {
    if (atomicOperation == null) {
      return writeCache.getFilledUpTo(fileId);
    }
    return atomicOperation.filledUpTo(fileId);
  }

  protected static OCacheEntry loadPageForWrite(
      final OAtomicOperation atomicOperation,
      final long fileId,
      final long pageIndex,
      final boolean verifyCheckSum)
      throws IOException {
    return atomicOperation.loadPageForWrite(fileId, pageIndex, 1, verifyCheckSum);
  }

  protected OCacheEntry loadOrAddPageForWrite(
      final OAtomicOperation atomicOperation,
      final long fileId,
      final long pageIndex,
      final boolean verifyCheckSum)
      throws IOException {
    OCacheEntry entry = atomicOperation.loadPageForWrite(fileId, pageIndex, 1, verifyCheckSum);
    if (entry == null) {
      entry = addPage(atomicOperation, fileId);
    }
    return entry;
  }

  protected OCacheEntry loadPageForRead(
      final OAtomicOperation atomicOperation, final long fileId, final long pageIndex)
      throws IOException {
    if (atomicOperation == null) {
      return readCache.loadForRead(fileId, pageIndex, writeCache, true);
    }
    return atomicOperation.loadPageForRead(fileId, pageIndex);
  }

  protected OCacheEntry addPage(final OAtomicOperation atomicOperation, final long fileId)
      throws IOException {
    assert atomicOperation != null;
    return atomicOperation.addPage(fileId);
  }

  protected void releasePageFromWrite(
      final OAtomicOperation atomicOperation, final OCacheEntry cacheEntry) throws IOException {
    assert atomicOperation != null;
    atomicOperation.releasePageFromWrite(cacheEntry);
  }

  protected void releasePageFromRead(
      final OAtomicOperation atomicOperation, final OCacheEntry cacheEntry) {
    if (atomicOperation == null) {
      readCache.releaseFromRead(cacheEntry);
    } else {
      atomicOperation.releasePageFromRead(cacheEntry);
    }
  }

  protected long addFile(final OAtomicOperation atomicOperation, final String fileName)
      throws IOException {
    assert atomicOperation != null;
    return atomicOperation.addFile(fileName);
  }

  protected long openFile(final OAtomicOperation atomicOperation, final String fileName)
      throws IOException {
    if (atomicOperation == null) {
      return writeCache.loadFile(fileName);
    }
    return atomicOperation.loadFile(fileName);
  }

  protected void deleteFile(final OAtomicOperation atomicOperation, final long fileId)
      throws IOException {
    assert atomicOperation != null;
    atomicOperation.deleteFile(fileId);
  }

  protected boolean isFileExists(final OAtomicOperation atomicOperation, final String fileName) {
    if (atomicOperation == null) {
      return writeCache.exists(fileName);
    }
    return atomicOperation.isFileExists(fileName);
  }

  protected void truncateFile(final OAtomicOperation atomicOperation, final long filedId)
      throws IOException {
    assert atomicOperation != null;
    atomicOperation.truncateFile(filedId);
  }
}
