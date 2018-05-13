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
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OUpdatePageRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.OComponentOperation;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ODurableComponent extends OSharedResourceAdaptive {
  protected final OAtomicOperationsManager  atomicOperationsManager;
  protected final OAbstractPaginatedStorage storage;
  protected final OReadCache                readCache;
  protected final OWriteCache               writeCache;
  protected final OWriteAheadLog            writeAheadLog;

  private volatile String name;
  private volatile String fullName;

  private final String extension;

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
    this.writeAheadLog = storage.getWALInstance();

    this.lockName = lockName;
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

  protected void endAtomicOperation(boolean rollback, Exception e) throws IOException {
    atomicOperationsManager.endAtomicOperation(rollback, e);
  }

  /**
   * @see OAtomicOperationsManager#startAtomicOperation(com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent, boolean)
   */
  protected OAtomicOperation startAtomicOperation(boolean trackNonTxOperations) throws IOException {
    return atomicOperationsManager.startAtomicOperation(this, trackNonTxOperations);
  }

  protected long getFilledUpTo(long fileId) {
    return writeCache.getFilledUpTo(fileId);
  }

  protected OCacheEntry loadPageForWrite(final long fileId, final long pageIndex, final boolean checkPinnedPages)
      throws IOException {
    return readCache.loadForWrite(fileId, pageIndex, checkPinnedPages, writeCache, 1, true);
  }

  protected OCacheEntry loadPageForRead(final long fileId, final long pageIndex, final boolean checkPinnedPages)
      throws IOException {
    return loadPageForRead(fileId, pageIndex, checkPinnedPages, 1);
  }

  protected OCacheEntry loadPageForRead(long fileId, long pageIndex, boolean checkPinnedPages, final int pageCount)
      throws IOException {
    return readCache.loadForRead(fileId, pageIndex, checkPinnedPages, writeCache, pageCount, true);
  }

  protected void pinPage(OCacheEntry cacheEntry) {
    readCache.pinPage(cacheEntry);
  }

  protected OCacheEntry addPage(long fileId) throws IOException {
    final OCacheEntry cacheEntry = readCache.allocateNewPage(fileId, writeCache, true);
    final ByteBuffer buffer = cacheEntry.getCachePointer().getBufferDuplicate();
    //TODO: change LSN according new format and add modification counter
    ODurablePage.setLogSequenceNumber(buffer, writeAheadLog.end());

    return cacheEntry;
  }

  protected void releasePageFromWrite(OCacheEntry cacheEntry, OAtomicOperation atomicOperation) {
    if (cacheEntry.isDirty()) {
      final OLogSequenceNumber end = writeAheadLog.end();
      final ByteBuffer buffer = cacheEntry.getCachePointer().getBufferDuplicate();
      final OLogSequenceNumber pageLsn = ODurablePage.getLogSequenceNumberFromPage(buffer);

      if (pageLsn.getSegment() < end.getSegment()) {
        final byte[] page = new byte[buffer.limit()];

        buffer.position(0);
        buffer.get(page);

        final OLogSequenceNumber recordLSN;
        try {
          final OUpdatePageRecord updatePageRecord = new OUpdatePageRecord(cacheEntry.getPageIndex(), cacheEntry.getFileId(),
              atomicOperation.getOperationUnitId(), page);
          recordLSN = writeAheadLog.log(updatePageRecord);
        } catch (IOException e) {
          throw OException.wrapException(new OStorageException(
              "Error during loging of changes of page " + cacheEntry.getFileId() + ":" + cacheEntry.getPageIndex() + " in storage "
                  + storage.getName()), e);
        }

        //TODO: change LSN according new format
        ODurablePage.setLogSequenceNumber(buffer, recordLSN);
      } else {
        assert pageLsn.getSegment() == end.getSegment();
        //TODO:update lsn by using modification counter
      }
    }

    readCache.releaseFromWrite(cacheEntry, writeCache);
  }

  public void logComponentOperation(OAtomicOperation atomicOperation, OComponentOperation componentOperation) {
    assert componentOperation != null;
    try {
      writeAheadLog.log(componentOperation);
    } catch (IOException e) {
      throw OException
          .wrapException(new OStorageException("Error during logging of component operation in storage " + storage.getName()), e);
    }
  }

  protected void releasePageFromRead(OCacheEntry cacheEntry) {
    readCache.releaseFromRead(cacheEntry, writeCache);
  }

  protected long addFile(String fileName) throws IOException {
    return readCache.addFile(fileName, writeCache);
  }

  protected long openFile(String fileName) throws IOException {
    return writeCache.loadFile(fileName);
  }

  protected void deleteFile(long fileId) throws IOException {
    readCache.deleteFile(fileId, writeCache);
  }

  protected boolean isFileExists(String fileName) {
    return writeCache.exists(fileName);
  }

  protected boolean isFileExists(long fileId) {
    return writeCache.exists(fileId);
  }

  protected void truncateFile(long filedId) throws IOException {
    readCache.truncateFile(filedId, writeCache);
  }
}
