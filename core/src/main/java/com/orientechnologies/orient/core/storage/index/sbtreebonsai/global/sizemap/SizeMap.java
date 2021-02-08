package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.sizemap;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import java.io.IOException;

public final class SizeMap extends ODurableComponent {

  private long fileId;

  public SizeMap(OAbstractPaginatedStorage storage, String name, String extension) {
    super(storage, name, extension, name + extension);
  }

  public void create(final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        (operation) -> {
          acquireExclusiveLock();
          try {
            fileId = addFile(atomicOperation, getFullName());

            final OCacheEntry cacheEntry = addPage(atomicOperation, fileId);
            try {
              assert cacheEntry.getPageIndex() == 0;

              final EntryPoint entryPoint = new EntryPoint(cacheEntry);
              entryPoint.setFileSize(0);
              entryPoint.setFreeListHeader(-1);
            } finally {
              releasePageFromWrite(atomicOperation, cacheEntry);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public void load() {
    acquireExclusiveLock();
    try {
      final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();

      fileId = openFile(atomicOperation, getFullName());
    } catch (final IOException e) {
      throw OException.wrapException(
          new OStorageException("Exception during loading of rid bag " + getName()), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void delete(final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            deleteFile(atomicOperation, fileId);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public int addTree(final OAtomicOperation atomicOperation) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            int freeListHeader;
            int fileSize;
            {
              final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, 0, false);
              try {
                final EntryPoint entryPoint = new EntryPoint(cacheEntry);
                fileSize = entryPoint.getFileSize();

                freeListHeader = entryPoint.getFreeListHeader();
              } finally {
                releasePageFromRead(atomicOperation, cacheEntry);
              }
            }

            if (fileSize == 0) {
              // add new page and entry
              final int localRidBagId;
              final OCacheEntry newEntry = addPage(atomicOperation, fileId);
              try {
                final Bucket bucket = new Bucket(newEntry);
                bucket.init();

                localRidBagId = bucket.addEntry(-1);
                assert localRidBagId >= 0;

              } finally {
                releasePageFromWrite(atomicOperation, newEntry);
              }

              final OCacheEntry stateEntry =
                  loadPageForWrite(atomicOperation, fileId, 0, false, true);
              try {
                final EntryPoint entryPoint = new EntryPoint(stateEntry);
                entryPoint.setFileSize(1);
              } finally {
                releasePageFromWrite(atomicOperation, stateEntry);
              }

              return localRidBagId;
            } else {
              if (freeListHeader == -1) {
                return addNewTree(atomicOperation, fileSize);
              }

              return reuseTreeEntry(atomicOperation, freeListHeader);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  private int addNewTree(final OAtomicOperation atomicOperation, final int fileSize)
      throws IOException {
    int pageIndex = fileSize;
    OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
    while (true) {
      try {
        final Bucket bucket = new Bucket(cacheEntry);
        final int localRidBagId = bucket.addEntry(-1);

        if (localRidBagId >= 0) {
          return localRidBagId + (pageIndex - 1) * Bucket.MAX_BUCKET_SIZE;
        }
      } finally {
        releasePageFromWrite(atomicOperation, cacheEntry);
      }

      cacheEntry = addPage(atomicOperation, fileId);

      final OCacheEntry stateEntry = loadPageForWrite(atomicOperation, fileId, 0, false, true);
      try {
        final EntryPoint entryPoint = new EntryPoint(stateEntry);
        entryPoint.setFileSize(fileSize + 1);
        pageIndex = fileSize + 1;
      } finally {
        releasePageFromWrite(atomicOperation, stateEntry);
      }
    }
  }

  private int reuseTreeEntry(final OAtomicOperation atomicOperation, int freeListHeader)
      throws IOException {
    final int freeListPageIndex = freeListHeader / Bucket.MAX_BUCKET_SIZE + 1;
    final int freeListLocalIndex =
        freeListHeader - (freeListPageIndex - 1) * Bucket.MAX_BUCKET_SIZE;

    final OCacheEntry cacheEntry =
        loadPageForWrite(atomicOperation, fileId, freeListPageIndex, false, true);

    final int localRidBagId;
    try {
      final Bucket bucket = new Bucket(cacheEntry);
      freeListHeader = bucket.getNextFreeListItem(freeListLocalIndex);
      localRidBagId = bucket.addEntry(freeListLocalIndex);
    } finally {
      releasePageFromWrite(atomicOperation, cacheEntry);
    }

    final OCacheEntry stateEntry = loadPageForWrite(atomicOperation, fileId, 0, false, true);
    try {
      final EntryPoint entryPoint = new EntryPoint(stateEntry);
      entryPoint.setFreeListHeader(freeListHeader);
    } finally {
      releasePageFromWrite(atomicOperation, stateEntry);
    }

    return localRidBagId + (freeListPageIndex - 1) * Bucket.MAX_BUCKET_SIZE;
  }

  public void incrementSize(final OAtomicOperation atomicOperation, final int ridBagId) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final int pageIndex = ridBagId / Bucket.MAX_BUCKET_SIZE + 1;
            final int localRidBagId = ridBagId - (pageIndex - 1) * Bucket.MAX_BUCKET_SIZE;

            final OCacheEntry cacheEntry =
                loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
            try {
              final Bucket bucket = new Bucket(cacheEntry);
              bucket.incrementSize(localRidBagId);
            } finally {
              releasePageFromWrite(atomicOperation, cacheEntry);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public void decrementSize(final OAtomicOperation atomicOperation, final int ridBagId) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final int pageIndex = ridBagId / Bucket.MAX_BUCKET_SIZE + 1;
            final int localRidBagId = ridBagId - (pageIndex - 1) * Bucket.MAX_BUCKET_SIZE;

            final OCacheEntry cacheEntry =
                loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
            try {
              final Bucket bucket = new Bucket(cacheEntry);
              bucket.decrementSize(localRidBagId);
            } finally {
              releasePageFromWrite(atomicOperation, cacheEntry);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  public int getSize(final int ridBagId) {
    acquireSharedLock();
    try {
      final int pageIndex = ridBagId / Bucket.MAX_BUCKET_SIZE + 1;
      final int localRidBagId = ridBagId - (pageIndex - 1) * Bucket.MAX_BUCKET_SIZE;

      final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
      final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      try {
        final Bucket bucket = new Bucket(cacheEntry);
        return bucket.getSize(localRidBagId);
      } finally {
        releasePageFromRead(atomicOperation, cacheEntry);
      }
    } catch (IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during reading the size of rid bag"), e);
    } finally {
      releaseSharedLock();
    }
  }

  public void remove(final OAtomicOperation atomicOperation, final int ridBagId) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final int freeListHeader;
            final OCacheEntry stateEntry =
                loadPageForWrite(atomicOperation, fileId, 0, false, true);
            try {
              final EntryPoint entryPoint = new EntryPoint(stateEntry);
              freeListHeader = entryPoint.getFreeListHeader();
              entryPoint.setFreeListHeader(ridBagId);
            } finally {
              releasePageFromWrite(atomicOperation, stateEntry);
            }

            final int pageIndex = ridBagId / Bucket.MAX_BUCKET_SIZE + 1;
            final int localRidBagId = ridBagId - (pageIndex - 1) * Bucket.MAX_BUCKET_SIZE;

            final OCacheEntry cacheEntry =
                loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
            try {
              final Bucket bucket = new Bucket(cacheEntry);
              bucket.delete(localRidBagId, freeListHeader);
            } finally {
              releasePageFromWrite(atomicOperation, cacheEntry);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }
}
