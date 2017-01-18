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

package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.exception.OHashTableDirectoryException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

import java.io.IOException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/14/14
 */
public class OHashTableDirectory extends ODurableComponent {
  static final int ITEM_SIZE = OLongSerializer.LONG_SIZE;

  private static final int LEVEL_SIZE = OLocalHashTable20.MAX_LEVEL_SIZE;

  static final int BINARY_LEVEL_SIZE = LEVEL_SIZE * ITEM_SIZE + 3 * OByteSerializer.BYTE_SIZE;

  private long fileId;

  private final long firstEntryIndex;


  OHashTableDirectory(String defaultExtension, String name, String lockName, OAbstractPaginatedStorage storage) {
    super(storage, name, defaultExtension, lockName);
    this.firstEntryIndex = 0;
  }

  public void create() throws IOException {
    startOperation();
    try {
      OAtomicOperation atomicOperation = startAtomicOperation(false);
      acquireExclusiveLock();
      try {

        fileId = addFile(atomicOperation, getFullName());
        init();
        endAtomicOperation(false, null);
      } catch (IOException e) {
        endAtomicOperation(true, e);
        throw e;
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(new OHashTableDirectoryException("Error during creation of hash table", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  private void init() throws IOException {
    OAtomicOperation atomicOperation = startAtomicOperation(false);
    try {
      OCacheEntry firstEntry = loadPageForWrite(atomicOperation, fileId, firstEntryIndex, true);

      if (firstEntry == null) {
        firstEntry = addPage(atomicOperation, fileId);
        assert firstEntry.getPageIndex() == 0;
      }

      pinPage(atomicOperation, firstEntry);

      try {
        ODirectoryFirstPage firstPage = new ODirectoryFirstPage(firstEntry, firstEntry);

        firstPage.setTreeSize(0);
        firstPage.setTombstone(-1);

      } finally {
        releasePageFromWrite(atomicOperation, firstEntry);
      }

      endAtomicOperation(false, null);
    } catch (IOException e) {
      endAtomicOperation(true, e);
      throw e;
    } catch (Exception e) {
      endAtomicOperation(true, e);
      throw OException.wrapException(new OHashTableDirectoryException("Error during hash table initialization", this), e);
    }
  }

  public void open() throws IOException {
    startOperation();
    try {
      acquireExclusiveLock();
      try {
        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        fileId = openFile(atomicOperation, getFullName());
        final int filledUpTo = (int) getFilledUpTo(atomicOperation, fileId);

        for (int i = 0; i < filledUpTo; i++) {
          final OCacheEntry entry = loadPageForRead(atomicOperation, fileId, i, true);
          assert entry != null;

          pinPage(atomicOperation, entry);
          releasePageFromRead(atomicOperation, entry);
        }
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public void close() throws IOException {
    startOperation();
    try {
      acquireExclusiveLock();
      try {
        readCache.closeFile(fileId, true, writeCache);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public void delete() throws IOException {
    startOperation();
    try {
      final OAtomicOperation atomicOperation = startAtomicOperation(false);
      acquireExclusiveLock();
      try {
        deleteFile(atomicOperation, fileId);
        endAtomicOperation(false, null);
      } catch (IOException e) {
        endAtomicOperation(true, e);
        throw e;
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(new OHashTableDirectoryException("Error during hash table deletion", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  void deleteWithoutOpen() throws IOException {
    startOperation();
    try {
      final OAtomicOperation atomicOperation = startAtomicOperation(false);
      acquireExclusiveLock();
      try {
        if (isFileExists(atomicOperation, getFullName())) {
          fileId = openFile(atomicOperation, getFullName());
          deleteFile(atomicOperation, fileId);
        }

        endAtomicOperation(false, null);
      } catch (IOException e) {
        endAtomicOperation(true, e);
        throw e;
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(new OHashTableDirectoryException("Error during deletion of hash table", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  int addNewNode(byte maxLeftChildDepth, byte maxRightChildDepth, byte nodeLocalDepth, long[] newNode) throws IOException {
    startOperation();
    try {
      int nodeIndex;

      OAtomicOperation atomicOperation = startAtomicOperation(true);
      acquireExclusiveLock();
      try {
        OCacheEntry firstEntry = loadPageForWrite(atomicOperation, fileId, firstEntryIndex, true);
        try {
          ODirectoryFirstPage firstPage = new ODirectoryFirstPage(firstEntry, firstEntry);

          final int tombstone = firstPage.getTombstone();

          if (tombstone >= 0)
            nodeIndex = tombstone;
          else {
            nodeIndex = firstPage.getTreeSize();
            firstPage.setTreeSize(nodeIndex + 1);
          }

          if (nodeIndex < ODirectoryFirstPage.NODES_PER_PAGE) {
            final int localNodeIndex = nodeIndex;

            firstPage.setMaxLeftChildDepth(localNodeIndex, maxLeftChildDepth);
            firstPage.setMaxRightChildDepth(localNodeIndex, maxRightChildDepth);
            firstPage.setNodeLocalDepth(localNodeIndex, nodeLocalDepth);

            if (tombstone >= 0)
              firstPage.setTombstone((int) firstPage.getPointer(nodeIndex, 0));

            for (int i = 0; i < newNode.length; i++)
              firstPage.setPointer(localNodeIndex, i, newNode[i]);

          } else {
            final int pageIndex = nodeIndex / ODirectoryPage.NODES_PER_PAGE;
            final int localLevel = nodeIndex % ODirectoryPage.NODES_PER_PAGE;

            OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true);
            while (cacheEntry == null || cacheEntry.getPageIndex() < pageIndex) {
              if (cacheEntry != null)
                releasePageFromWrite(atomicOperation, cacheEntry);

              cacheEntry = addPage(atomicOperation, fileId);
            }

            try {
              ODirectoryPage page = new ODirectoryPage(cacheEntry, cacheEntry);

              page.setMaxLeftChildDepth(localLevel, maxLeftChildDepth);
              page.setMaxRightChildDepth(localLevel, maxRightChildDepth);
              page.setNodeLocalDepth(localLevel, nodeLocalDepth);

              if (tombstone >= 0)
                firstPage.setTombstone((int) page.getPointer(localLevel, 0));

              for (int i = 0; i < newNode.length; i++)
                page.setPointer(localLevel, i, newNode[i]);

            } finally {
              releasePageFromWrite(atomicOperation, cacheEntry);
            }
          }

        } finally {
          releasePageFromWrite(atomicOperation, firstEntry);
        }

        endAtomicOperation(false, null);

      } catch (RuntimeException e) {
        endAtomicOperation(true, e);
        throw e;
      } finally {
        releaseExclusiveLock();
      }

      return nodeIndex;
    } finally {
      completeOperation();
    }
  }

  void deleteNode(int nodeIndex) throws IOException {
    startOperation();
    try {
      final OAtomicOperation atomicOperation = startAtomicOperation(true);
      acquireExclusiveLock();
      try {
        OCacheEntry firstEntry = loadPageForWrite(atomicOperation, fileId, firstEntryIndex, true);
        try {
          ODirectoryFirstPage firstPage = new ODirectoryFirstPage(firstEntry, firstEntry);
          if (nodeIndex < ODirectoryFirstPage.NODES_PER_PAGE) {
            firstPage.setPointer(nodeIndex, 0, firstPage.getTombstone());
            firstPage.setTombstone(nodeIndex);
          } else {
            final int pageIndex = nodeIndex / ODirectoryPage.NODES_PER_PAGE;
            final int localNodeIndex = nodeIndex % ODirectoryPage.NODES_PER_PAGE;

            final OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true);
            try {
              ODirectoryPage page = new ODirectoryPage(cacheEntry, cacheEntry);

              page.setPointer(localNodeIndex, 0, firstPage.getTombstone());
              firstPage.setTombstone(nodeIndex);

            } finally {
              releasePageFromWrite(atomicOperation, cacheEntry);
            }
          }
        } finally {
          releasePageFromWrite(atomicOperation, firstEntry);
        }

        endAtomicOperation(false, null);
      } catch (IOException e) {
        endAtomicOperation(true, e);
        throw e;
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(new OHashTableDirectoryException("Error during node deletion", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  byte getMaxLeftChildDepth(int nodeIndex) throws IOException {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          final ODirectoryPage page = loadPage(nodeIndex, false, atomicOperation);
          try {
            return page.getMaxLeftChildDepth(getLocalNodeIndex(nodeIndex));
          } finally {
            releasePage(page, false, atomicOperation);
          }
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  void setMaxLeftChildDepth(int nodeIndex, byte maxLeftChildDepth) throws IOException {
    startOperation();
    try {
      OAtomicOperation atomicOperation = startAtomicOperation(true);
      acquireExclusiveLock();
      try {

        final ODirectoryPage page = loadPage(nodeIndex, true, atomicOperation);
        try {
          page.setMaxLeftChildDepth(getLocalNodeIndex(nodeIndex), maxLeftChildDepth);
        } finally {
          releasePage(page, true, atomicOperation);
        }

        endAtomicOperation(false, null);
      } catch (IOException e) {
        endAtomicOperation(true, e);
        throw e;
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(new OHashTableDirectoryException("Error during setting of max left child depth", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  byte getMaxRightChildDepth(int nodeIndex) throws IOException {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
          final ODirectoryPage page = loadPage(nodeIndex, false, atomicOperation);
          try {
            return page.getMaxRightChildDepth(getLocalNodeIndex(nodeIndex));
          } finally {
            releasePage(page, false, atomicOperation);
          }
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  void setMaxRightChildDepth(int nodeIndex, byte maxRightChildDepth) throws IOException {
    startOperation();
    try {
      OAtomicOperation atomicOperation = startAtomicOperation(true);
      acquireExclusiveLock();
      try {

        final ODirectoryPage page = loadPage(nodeIndex, true, atomicOperation);
        try {
          page.setMaxRightChildDepth(getLocalNodeIndex(nodeIndex), maxRightChildDepth);
        } finally {
          releasePage(page, true, atomicOperation);
        }

        endAtomicOperation(false, null);
      } catch (IOException e) {
        endAtomicOperation(true, e);
        throw e;
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(new OHashTableDirectoryException("Error during setting of right max child depth", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  byte getNodeLocalDepth(int nodeIndex) throws IOException {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
          final ODirectoryPage page = loadPage(nodeIndex, false, atomicOperation);
          try {
            return page.getNodeLocalDepth(getLocalNodeIndex(nodeIndex));
          } finally {
            releasePage(page, false, atomicOperation);
          }
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  void setNodeLocalDepth(int nodeIndex, byte localNodeDepth) throws IOException {
    startOperation();
    try {
      OAtomicOperation atomicOperation = startAtomicOperation(true);
      acquireExclusiveLock();
      try {
        final ODirectoryPage page = loadPage(nodeIndex, true, atomicOperation);
        try {
          page.setNodeLocalDepth(getLocalNodeIndex(nodeIndex), localNodeDepth);
        } finally {
          releasePage(page, true, atomicOperation);
        }

        endAtomicOperation(false, null);
      } catch (IOException e) {
        endAtomicOperation(true, e);
        throw e;
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(new OHashTableDirectoryException("Error during setting of local node depth", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  long[] getNode(int nodeIndex) throws IOException {
    startOperation();
    try {
      final long[] node = new long[LEVEL_SIZE];

      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
          final ODirectoryPage page = loadPage(nodeIndex, false, atomicOperation);
          try {
            final int localNodeIndex = getLocalNodeIndex(nodeIndex);
            for (int i = 0; i < LEVEL_SIZE; i++)
              node[i] = page.getPointer(localNodeIndex, i);
          } finally {
            releasePage(page, false, atomicOperation);
          }
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }

      return node;
    } finally {
      completeOperation();
    }
  }

  void setNode(int nodeIndex, long[] node) throws IOException {
    startOperation();
    try {
      OAtomicOperation atomicOperation = startAtomicOperation(true);
      acquireExclusiveLock();
      try {

        final ODirectoryPage page = loadPage(nodeIndex, true, atomicOperation);
        try {
          final int localNodeIndex = getLocalNodeIndex(nodeIndex);
          for (int i = 0; i < LEVEL_SIZE; i++)
            page.setPointer(localNodeIndex, i, node[i]);
        } finally {
          releasePage(page, true, atomicOperation);
        }

        endAtomicOperation(false, null);
      } catch (IOException e) {
        endAtomicOperation(true, e);
        throw e;
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(new OHashTableDirectoryException("Error during setting of node", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  long getNodePointer(int nodeIndex, int index) throws IOException {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
          final ODirectoryPage page = loadPage(nodeIndex, false, atomicOperation);
          try {
            return page.getPointer(getLocalNodeIndex(nodeIndex), index);
          } finally {
            releasePage(page, false, atomicOperation);
          }
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  void setNodePointer(int nodeIndex, int index, long pointer) throws IOException {
    startOperation();
    try {
      OAtomicOperation atomicOperation = startAtomicOperation(true);
      acquireExclusiveLock();
      try {
        final ODirectoryPage page = loadPage(nodeIndex, true, atomicOperation);
        try {
          page.setPointer(getLocalNodeIndex(nodeIndex), index, pointer);
        } finally {
          releasePage(page, true, atomicOperation);
        }

        endAtomicOperation(false, null);
      } catch (IOException e) {
        endAtomicOperation(true, e);
        throw e;
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(new OHashTableDirectoryException("Error during setting of node pointer", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public void clear() throws IOException {
    startOperation();
    try {
      OAtomicOperation atomicOperation = startAtomicOperation(true);
      acquireExclusiveLock();
      try {
        truncateFile(atomicOperation, fileId);

        init();

        endAtomicOperation(false, null);
      } catch (IOException e) {
        endAtomicOperation(true, e);
        throw e;
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw OException
            .wrapException(new OHashTableDirectoryException("Error during removing of hash table directory content", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public void flush() throws IOException {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          writeCache.flush(fileId);
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  private ODirectoryPage loadPage(int nodeIndex, boolean exclusiveLock, OAtomicOperation atomicOperation) throws IOException {
    if (nodeIndex < ODirectoryFirstPage.NODES_PER_PAGE) {
      OCacheEntry cacheEntry;

      if (exclusiveLock)
        cacheEntry = loadPageForWrite(atomicOperation, fileId, firstEntryIndex, true);
      else
        cacheEntry = loadPageForRead(atomicOperation, fileId, firstEntryIndex, true);

      return new ODirectoryFirstPage(cacheEntry, cacheEntry);
    }

    final int pageIndex = nodeIndex / ODirectoryPage.NODES_PER_PAGE;

    final OCacheEntry cacheEntry;

    if (exclusiveLock)
      cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true);
    else
      cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, true);

    return new ODirectoryPage(cacheEntry, cacheEntry);
  }

  private void releasePage(ODirectoryPage page, boolean exclusiveLock, OAtomicOperation atomicOperation) {
    final OCacheEntry cacheEntry = page.getEntry();

    if (exclusiveLock)
      releasePageFromWrite(atomicOperation, cacheEntry);
    else
      releasePageFromRead(atomicOperation, cacheEntry);


  }

  private int getLocalNodeIndex(int nodeIndex) {
    if (nodeIndex < ODirectoryFirstPage.NODES_PER_PAGE)
      return nodeIndex;

    return (nodeIndex - ODirectoryFirstPage.NODES_PER_PAGE) % ODirectoryPage.NODES_PER_PAGE;
  }
}
