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
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import java.io.IOException;

/**
 * The version position map in version 0 stores a version of type int for all change operations on
 * the `OAbstractPaginatedStorage` storage. It creates one file with extension `vpm` (i.e. w/o meta
 * data) and expected number of elements OBaseIndexEngine.DEFAULT_VERSION_ARRAY_SIZE.
 */
public final class OVersionPositionMapV0 extends OVersionPositionMap {
  private long fileId;

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
      final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
      this.openVPM(atomicOperation);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void updateVersion(final int hash) {
    final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final int startPositionWithOffset = OVersionPositionMapBucket.entryPosition(hash);
            final int pageIndex = calculatePageIndex(startPositionWithOffset);
            OLogManager.instance()
                .info(
                    this,
                    "VPM update on fileId:%s: hash = %d, entry position = %d, page index = %d",
                    fileId,
                    hash,
                    startPositionWithOffset,
                    pageIndex);
            final OCacheEntry cacheEntry =
                loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
            try {
              final OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
              bucket.incrementVersion(hash);
            } finally {
              releasePageFromWrite(atomicOperation, cacheEntry);
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
    OLogManager.instance()
        .info(
            this,
            "VPM getVersion on fileId:%s: hash = %d, entry position = %d, page index = %d",
            fileId,
            hash,
            startPositionWithOffset,
            pageIndex);

    acquireSharedLock();
    try {
      final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
      final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      try {
        final OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
        return bucket.getVersion(hash);
      } finally {
        releasePageFromRead(atomicOperation, cacheEntry);
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during reading the size of rid bag"), e);
    } finally {
      releaseSharedLock();
    }
  }

  private int calculatePageIndex(final int startPositionWithOffset) {
    return 0; // (int) Math.ceil(startPositionWithOffset / OVersionPage.PAGE_SIZE) + 1;
  }

  private void openVPM(final OAtomicOperation atomicOperation) throws IOException {
    fileId = openFile(atomicOperation, getFullName());
    OLogManager.instance().info(this, "VPM open fileId:%s: fileName = %s", fileId, getFullName());
  }

  private void createVPM(final OAtomicOperation atomicOperation) throws IOException {
    fileId = addFile(atomicOperation, getFullName());
    OLogManager.instance().info(this, "VPM open fileId:%s: fileName = %s", fileId, getFullName());
    if (getFilledUpTo(atomicOperation, fileId) == 0) {
      // first one page is added for the meta data
      // addInitializedPage(atomicOperation);

      // then let us add several empty data pages
      final int sizeOfIntInBytes = Integer.SIZE / 8;
      /*final int numberOfPages =
          (int)
                  Math.ceil(
                      (OBaseIndexEngine.DEFAULT_VERSION_ARRAY_SIZE * sizeOfIntInBytes)
                          / OVersionPage.PAGE_SIZE)
              + 1;
      for (int i = 0; i < numberOfPages; i++) {*/
      addInitializedPage(atomicOperation);
      // }
    } else {
      final OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, 0, false, false);
      try {
        final MapEntryPoint mapEntryPoint = new MapEntryPoint(cacheEntry);
        mapEntryPoint.setFileSize(0);
      } finally {
        releasePageFromWrite(atomicOperation, cacheEntry);
      }
    }
  }

  private void addInitializedPage(final OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry cacheEntry = addPage(atomicOperation, fileId);
    try {
      final MapEntryPoint mapEntryPoint = new MapEntryPoint(cacheEntry);
      mapEntryPoint.setFileSize(0);
    } finally {
      releasePageFromWrite(atomicOperation, cacheEntry);
    }
  }

  private void deleteVPM(final OAtomicOperation atomicOperation) throws IOException {
    deleteFile(atomicOperation, fileId);
  }
}
