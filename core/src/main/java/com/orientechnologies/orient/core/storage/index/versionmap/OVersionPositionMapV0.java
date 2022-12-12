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

package com.orientechnologies.orient.core.storage.index.versionmap;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.version.OVersionPage;
import java.io.IOException;

/**
 * The version position map in version 0 stores a version of type int for all change operations on
 * the `OAbstractPaginatedStorage` storage. It creates one file with extension `vpm` (i.e. w/o meta
 * data) and expected number of elements OBaseIndexEngine.DEFAULT_VERSION_ARRAY_SIZE.
 */
public final class OVersionPositionMapV0 extends OVersionPositionMap {
  private long fileId;
  private int numberOfPages;

  public OVersionPositionMapV0(
      final OAbstractPaginatedStorage storage,
      final String name,
      final String lockName,
      final String extension) {
    super(storage, name, extension, lockName);
  }

  @Override
  public void create(final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            this.createVPM(atomicOperation);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @Override
  public void delete(final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            this.deleteVPM(atomicOperation);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @Override
  public void open() throws IOException {
    acquireExclusiveLock();
    try {
      final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
      this.openVPM(atomicOperation);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void updateVersion(final int hash) {
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final int startPositionWithOffset = OVersionPositionMapBucket.entryPosition(hash);
            final int pageIndex = calculatePageIndex(startPositionWithOffset);
            try (final OCacheEntry cacheEntry =
                loadPageForWrite(atomicOperation, fileId, pageIndex, true)) {
              final OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
              bucket.incrementVersion(hash);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @Override
  public int getVersion(final int hash) {
    final int startPositionWithOffset = OVersionPositionMapBucket.entryPosition(hash);
    final int pageIndex = calculatePageIndex(startPositionWithOffset);
    acquireSharedLock();
    try {
      final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

      try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
        final OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
        return bucket.getVersion(hash);
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during reading the size of rid bag"), e);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public int getKeyHash(final Object key) {
    int keyHash = 0; // as for null values in hash map
    if (key != null) {
      keyHash = Math.abs(key.hashCode()) % DEFAULT_VERSION_ARRAY_SIZE;
    }
    return keyHash;
  }

  private void openVPM(final OAtomicOperation atomicOperation) throws IOException {
    // In case an old storage does not have a VPM yet, it will be created.
    // If the creation of a VPM is interrupted due to any error / exception, the file is either
    // created corrupt, and thus subsequent access will (not hiding the issue), or the file will not
    // be created properly and a new one will be created during the next access on openVPM.
    if (!isFileExists(atomicOperation, getFullName())) {
      OLogManager.instance()
          .debug(
              this,
              "VPM missing with fileId:%s: fileName = %s. A new VPM will be created.",
              fileId,
              getFullName());
      if (atomicOperation != null) {
        createVPM(atomicOperation);
      } else {
        atomicOperationsManager.executeInsideAtomicOperation(null, this::createVPM);
      }
    }
    fileId = openFile(atomicOperation, getFullName());
    OLogManager.instance().debug(this, "VPM open fileId:%s: fileName = %s", fileId, getFullName());
  }

  private void createVPM(final OAtomicOperation atomicOperation) throws IOException {
    fileId = addFile(atomicOperation, getFullName());
    final int sizeOfIntInBytes = Integer.SIZE / 8;
    numberOfPages =
        (int)
            Math.ceil(
                (DEFAULT_VERSION_ARRAY_SIZE * sizeOfIntInBytes * 1.0) / OVersionPage.PAGE_SIZE);
    final long foundNumberOfPages = getFilledUpTo(atomicOperation, fileId);
    OLogManager.instance()
        .debug(
            this,
            "VPM created with fileId:%s: fileName = %s, expected #pages = %d, actual #pages = %d",
            fileId,
            getFullName(),
            numberOfPages,
            foundNumberOfPages);
    if (foundNumberOfPages != numberOfPages) {
      for (int i = 0; i < numberOfPages; i++) {
        addInitializedPage(atomicOperation);
      }
    } else {
      try (final OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, 0, false)) {
        final MapEntryPoint mapEntryPoint = new MapEntryPoint(cacheEntry);
        mapEntryPoint.setFileSize(0);
      }
    }
  }

  private void addInitializedPage(final OAtomicOperation atomicOperation) throws IOException {
    try (final OCacheEntry cacheEntry = addPage(atomicOperation, fileId)) {
      final MapEntryPoint mapEntryPoint = new MapEntryPoint(cacheEntry);
      mapEntryPoint.setFileSize(0);
    }
  }

  private void deleteVPM(final OAtomicOperation atomicOperation) throws IOException {
    deleteFile(atomicOperation, fileId);
  }

  private int calculatePageIndex(final int startPositionWithOffset) {
    return (int) Math.ceil(startPositionWithOffset / OVersionPage.PAGE_SIZE);
  }

  int getNumberOfPages() {
    return numberOfPages;
  }
}
