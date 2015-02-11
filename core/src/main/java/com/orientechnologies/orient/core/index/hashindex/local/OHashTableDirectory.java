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

package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCachePointer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 5/14/14
 */
public class OHashTableDirectory extends ODurableComponent {
  public static final int                 ITEM_SIZE         = OLongSerializer.LONG_SIZE;

  public static final int                 LEVEL_SIZE        = OLocalHashTable.MAX_LEVEL_SIZE;

  public static final int                 BINARY_LEVEL_SIZE = LEVEL_SIZE * ITEM_SIZE + 3 * OByteSerializer.BYTE_SIZE;

  private final String                    defaultExtension;
  private final String                    name;
  private final ODiskCache                diskCache;

  private long                            fileId;

  private OCacheEntry                     firstEntry;
  private List<OCacheEntry>               entries;

  private final boolean                   durableInNonTxMode;
  private final OAbstractPaginatedStorage storage;

  private final ODurablePage.TrackMode    txTrackMode       = ODurablePage.TrackMode.valueOf(OGlobalConfiguration.INDEX_TX_MODE
                                                                .getValueAsString().toUpperCase());

  public OHashTableDirectory(String defaultExtension, String name, boolean durableInNonTxMode, OAbstractPaginatedStorage storage) {
    this.defaultExtension = defaultExtension;
    this.name = name;
    this.diskCache = storage.getDiskCache();
    this.durableInNonTxMode = durableInNonTxMode;
    this.storage = storage;

    init(storage);
  }

  public void create() throws IOException {
    startAtomicOperation();
    acquireExclusiveLock();
    try {
      fileId = diskCache.openFile(name + defaultExtension);
      logFileCreation(name + defaultExtension, fileId);
      init();
      endAtomicOperation(false);
    } catch (Throwable e) {
      endAtomicOperation(true);
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void init() throws IOException {
    startAtomicOperation();
    try {
      firstEntry = diskCache.load(fileId, 0, true);

      diskCache.pinPage(firstEntry);

      firstEntry.acquireExclusiveLock();
      try {
        ODirectoryFirstPage firstPage = new ODirectoryFirstPage(firstEntry, getTrackMode(), firstEntry);

        firstPage.setTreeSize(0);
        firstPage.setTombstone(-1);

        firstEntry.markDirty();
        logPageChanges(firstPage, firstEntry.getFileId(), firstEntry.getPageIndex(), true);
      } finally {
        firstEntry.releaseExclusiveLock();
        diskCache.release(firstEntry);
      }

      entries = new ArrayList<OCacheEntry>();

      endAtomicOperation(false);
    } catch (IOException e) {
      endAtomicOperation(true);
      throw e;
    } catch (Throwable e) {
      endAtomicOperation(true);
      throw new OStorageException(null, e);
    }
  }

  public void open() throws IOException {
    acquireExclusiveLock();
    try {
      fileId = diskCache.openFile(name + defaultExtension);
      firstEntry = diskCache.load(fileId, 0, true);
      diskCache.pinPage(firstEntry);
      diskCache.release(firstEntry);

      final int filledUpTo = (int) diskCache.getFilledUpTo(fileId);

      entries = new ArrayList<OCacheEntry>(filledUpTo - 1);

      for (int i = 1; i < filledUpTo; i++) {
        final OCacheEntry entry = diskCache.load(fileId, i, true);
        diskCache.pinPage(entry);
        diskCache.release(entry);

        entries.add(entry);
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  public void close() throws IOException {
    acquireExclusiveLock();
    try {
      diskCache.closeFile(fileId);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void delete() throws IOException {
    acquireExclusiveLock();
    try {
      diskCache.deleteFile(fileId);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void deleteWithoutOpen() throws IOException {
    acquireExclusiveLock();
    try {
      if (diskCache.exists(name + defaultExtension)) {
        fileId = diskCache.openFile(name + defaultExtension);
        diskCache.deleteFile(fileId);
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  public int addNewNode(byte maxLeftChildDepth, byte maxRightChildDepth, byte nodeLocalDepth, long[] newNode) throws IOException {
    int nodeIndex;

    acquireExclusiveLock();
    startAtomicOperation();
    try {
      diskCache.loadPinnedPage(firstEntry);

      firstEntry.acquireExclusiveLock();
      try {
        ODirectoryFirstPage firstPage = new ODirectoryFirstPage(firstEntry, getTrackMode(), firstEntry);

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
          final int pageIndex = (nodeIndex - ODirectoryFirstPage.NODES_PER_PAGE) / ODirectoryPage.NODES_PER_PAGE;
          final int localLevel = (nodeIndex - ODirectoryFirstPage.NODES_PER_PAGE) % ODirectoryPage.NODES_PER_PAGE;

          boolean newPage = false;
          while (entries.size() <= pageIndex) {
            OCacheEntry cacheEntry = diskCache.load(fileId, entries.size() + 1, true);
            diskCache.pinPage(cacheEntry);
            diskCache.release(cacheEntry);

            entries.add(cacheEntry);
            newPage = true;
          }

          OCacheEntry cacheEntry = entries.get(pageIndex);

          diskCache.loadPinnedPage(cacheEntry);

          cacheEntry.acquireExclusiveLock();

          try {
            ODirectoryPage page = new ODirectoryPage(cacheEntry, ODurablePage.TrackMode.NONE, cacheEntry);

            page.setMaxLeftChildDepth(localLevel, maxLeftChildDepth);
            page.setMaxRightChildDepth(localLevel, maxRightChildDepth);
            page.setNodeLocalDepth(localLevel, nodeLocalDepth);

            if (tombstone >= 0)
              firstPage.setTombstone((int) page.getPointer(localLevel, 0));

            for (int i = 0; i < newNode.length; i++)
              page.setPointer(localLevel, i, newNode[i]);

            cacheEntry.markDirty();
            logPageChanges(page, cacheEntry.getFileId(), firstEntry.getPageIndex(), newPage);
          } finally {
            cacheEntry.releaseExclusiveLock();
            diskCache.release(cacheEntry);
          }
        }

        logPageChanges(firstPage, firstEntry.getFileId(), firstEntry.getPageIndex(), false);
      } finally {
        firstEntry.releaseExclusiveLock();
        diskCache.release(firstEntry);
      }

      endAtomicOperation(false);

    } catch (IOException e) {
      endAtomicOperation(true);
      throw e;
    } catch (Throwable e) {
      endAtomicOperation(true);
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }

    return nodeIndex;
  }

  public void deleteNode(int nodeIndex) throws IOException {
    acquireExclusiveLock();
    startAtomicOperation();
    try {
      diskCache.loadPinnedPage(firstEntry);

      firstEntry.acquireExclusiveLock();
      try {
        ODirectoryFirstPage firstPage = new ODirectoryFirstPage(firstEntry, ODurablePage.TrackMode.NONE, firstEntry);
        if (nodeIndex < ODirectoryFirstPage.NODES_PER_PAGE) {
          firstPage.setPointer(nodeIndex, 0, firstPage.getTombstone());
          firstPage.setTombstone(nodeIndex);
        } else {
          final int pageIndex = (nodeIndex - ODirectoryFirstPage.NODES_PER_PAGE) / ODirectoryPage.NODES_PER_PAGE;
          final int localNodeIndex = (nodeIndex - ODirectoryFirstPage.NODES_PER_PAGE) % ODirectoryPage.NODES_PER_PAGE;

          final OCacheEntry cacheEntry = entries.get(pageIndex);
          diskCache.loadPinnedPage(cacheEntry);

          cacheEntry.acquireExclusiveLock();
          try {
            ODirectoryPage page = new ODirectoryPage(cacheEntry, ODurablePage.TrackMode.NONE, cacheEntry);

            page.setPointer(localNodeIndex, 0, firstPage.getTombstone());
            firstPage.setTombstone(nodeIndex);

            cacheEntry.markDirty();
            logPageChanges(page, cacheEntry.getFileId(), cacheEntry.getPageIndex(), false);
          } finally {
            cacheEntry.releaseExclusiveLock();
            diskCache.release(cacheEntry);
          }
        }
        logPageChanges(firstPage, firstEntry.getFileId(), firstEntry.getPageIndex(), false);
      } finally {
        firstEntry.releaseExclusiveLock();
        diskCache.release(firstEntry);
      }

      endAtomicOperation(false);
    } catch (IOException e) {
      endAtomicOperation(true);
      throw e;
    } catch (Throwable e) {
      endAtomicOperation(true);
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public byte getMaxLeftChildDepth(int nodeIndex) throws IOException {
    acquireSharedLock();
    try {
      final ODirectoryPage page = loadPage(nodeIndex, false);
      try {
        return page.getMaxLeftChildDepth(getLocalNodeIndex(nodeIndex));
      } finally {
        releasePage(page, false);
      }
    } finally {
      releaseSharedLock();
    }
  }

  public void setMaxLeftChildDepth(int nodeIndex, byte maxLeftChildDepth) throws IOException {
    acquireExclusiveLock();
    startAtomicOperation();
    try {
      final ODirectoryPage page = loadPage(nodeIndex, true);
      try {
        page.setMaxLeftChildDepth(getLocalNodeIndex(nodeIndex), maxLeftChildDepth);

        OCacheEntry cacheEntry = page.getEntry();
        cacheEntry.markDirty();

        logPageChanges(page, cacheEntry.getFileId(), cacheEntry.getPageIndex(), false);
      } finally {
        releasePage(page, true);
      }

      endAtomicOperation(false);
    } catch (IOException e) {
      endAtomicOperation(true);
      throw e;
    } catch (Throwable e) {
      endAtomicOperation(true);
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public byte getMaxRightChildDepth(int nodeIndex) throws IOException {
    acquireSharedLock();
    try {
      final ODirectoryPage page = loadPage(nodeIndex, false);
      try {
        return page.getMaxRightChildDepth(getLocalNodeIndex(nodeIndex));
      } finally {
        releasePage(page, false);
      }
    } finally {
      releaseSharedLock();
    }
  }

  public void setMaxRightChildDepth(int nodeIndex, byte maxRightChildDepth) throws IOException {
    acquireExclusiveLock();
    startAtomicOperation();
    try {
      final ODirectoryPage page = loadPage(nodeIndex, true);
      try {
        page.setMaxRightChildDepth(getLocalNodeIndex(nodeIndex), (byte) maxRightChildDepth);

        OCacheEntry cacheEntry = page.getEntry();
        cacheEntry.markDirty();

        logPageChanges(page, cacheEntry.getFileId(), cacheEntry.getPageIndex(), false);
      } finally {
        releasePage(page, true);
      }

      endAtomicOperation(false);
    } catch (IOException e) {
      endAtomicOperation(true);
      throw e;
    } catch (Throwable e) {
      endAtomicOperation(true);
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public byte getNodeLocalDepth(int nodeIndex) throws IOException {
    acquireSharedLock();
    try {
      final ODirectoryPage page = loadPage(nodeIndex, false);
      try {
        return page.getNodeLocalDepth(getLocalNodeIndex(nodeIndex));
      } finally {
        releasePage(page, false);
      }
    } finally {
      releaseSharedLock();
    }
  }

  public void setNodeLocalDepth(int nodeIndex, byte localNodeDepth) throws IOException {
    acquireExclusiveLock();
    startAtomicOperation();
    try {
      final ODirectoryPage page = loadPage(nodeIndex, true);
      try {
        page.setNodeLocalDepth(getLocalNodeIndex(nodeIndex), localNodeDepth);

        OCacheEntry cacheEntry = page.getEntry();
        cacheEntry.markDirty();

        logPageChanges(page, cacheEntry.getFileId(), cacheEntry.getPageIndex(), false);
      } finally {
        releasePage(page, true);
      }

      endAtomicOperation(false);
    } catch (IOException e) {
      endAtomicOperation(true);
      throw e;
    } catch (Throwable e) {
      endAtomicOperation(true);
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public long[] getNode(int nodeIndex) throws IOException {
    final long[] node = new long[LEVEL_SIZE];

    acquireSharedLock();
    try {
      final ODirectoryPage page = loadPage(nodeIndex, false);
      try {
        final int localNodeIndex = getLocalNodeIndex(nodeIndex);
        for (int i = 0; i < LEVEL_SIZE; i++)
          node[i] = page.getPointer(localNodeIndex, i);
      } finally {
        releasePage(page, false);
      }
    } finally {
      releaseSharedLock();
    }

    return node;
  }

  public void setNode(int nodeIndex, long[] node) throws IOException {
    acquireExclusiveLock();
    startAtomicOperation();
    try {
      final ODirectoryPage page = loadPage(nodeIndex, true);
      try {
        final int localNodeIndex = getLocalNodeIndex(nodeIndex);
        for (int i = 0; i < LEVEL_SIZE; i++)
          page.setPointer(localNodeIndex, i, node[i]);

        OCacheEntry cacheEntry = page.getEntry();
        cacheEntry.markDirty();

        logPageChanges(page, cacheEntry.getFileId(), cacheEntry.getPageIndex(), false);
      } finally {
        releasePage(page, true);
      }

      endAtomicOperation(false);
    } catch (IOException e) {
      endAtomicOperation(true);
      throw e;
    } catch (Throwable e) {
      endAtomicOperation(true);
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public long getNodePointer(int nodeIndex, int index) throws IOException {
    acquireSharedLock();
    try {
      final ODirectoryPage page = loadPage(nodeIndex, false);
      try {
        return page.getPointer(getLocalNodeIndex(nodeIndex), index);
      } finally {
        releasePage(page, false);
      }
    } finally {
      releaseSharedLock();
    }
  }

  public void setNodePointer(int nodeIndex, int index, long pointer) throws IOException {
    acquireExclusiveLock();
    startAtomicOperation();
    try {
      final ODirectoryPage page = loadPage(nodeIndex, true);
      try {
        page.setPointer(getLocalNodeIndex(nodeIndex), index, pointer);

        OCacheEntry cacheEntry = page.getEntry();
        cacheEntry.markDirty();

        logPageChanges(page, cacheEntry.getFileId(), cacheEntry.getPageIndex(), false);
      } finally {
        releasePage(page, true);
      }

      endAtomicOperation(false);
    } catch (IOException e) {
      endAtomicOperation(true);
      throw e;
    } catch (Throwable e) {
      endAtomicOperation(true);
      throw new OStorageException(null, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void clear() throws IOException {
    acquireExclusiveLock();
    try {
      diskCache.truncateFile(fileId);

      init();
    } finally {
      releaseExclusiveLock();
    }
  }

  public void flush() throws IOException {
    acquireSharedLock();
    try {
      diskCache.flushFile(fileId);
    } finally {
      releaseSharedLock();
    }
  }

  private ODirectoryPage loadPage(int nodeIndex, boolean exclusiveLock) throws IOException {
    if (nodeIndex < ODirectoryFirstPage.NODES_PER_PAGE) {
      diskCache.loadPinnedPage(firstEntry);
      if (exclusiveLock)
        firstEntry.acquireExclusiveLock();

      return new ODirectoryFirstPage(firstEntry, getTrackMode(), firstEntry);
    }

    final int pageIndex = (nodeIndex - ODirectoryFirstPage.NODES_PER_PAGE) / ODirectoryPage.NODES_PER_PAGE;
    final OCacheEntry cacheEntry = entries.get(pageIndex);
    diskCache.loadPinnedPage(cacheEntry);

    if (exclusiveLock)
      cacheEntry.acquireExclusiveLock();

    return new ODirectoryPage(cacheEntry, getTrackMode(), cacheEntry);
  }

  private void releasePage(ODirectoryPage page, boolean exclusiveLock) {
    final OCacheEntry cacheEntry = page.getEntry();
    final OCachePointer cachePointer = cacheEntry.getCachePointer();

    if (exclusiveLock)
      cachePointer.releaseExclusiveLock();
    diskCache.release(cacheEntry);
  }

  private int getLocalNodeIndex(int nodeIndex) {
    if (nodeIndex < ODirectoryFirstPage.NODES_PER_PAGE)
      return nodeIndex;

    return (nodeIndex - ODirectoryFirstPage.NODES_PER_PAGE) % ODirectoryPage.NODES_PER_PAGE;
  }

  @Override
  protected ODurablePage.TrackMode getTrackMode() {
    final OStorageTransaction transaction = storage.getStorageTransaction();

    if (transaction == null && !durableInNonTxMode)
      return ODurablePage.TrackMode.NONE;

    final ODurablePage.TrackMode trackMode = super.getTrackMode();
    if (!trackMode.equals(ODurablePage.TrackMode.NONE))
      return txTrackMode;

    return trackMode;
  }

  @Override
  protected void endAtomicOperation(boolean rollback) throws IOException {
    if (storage.getStorageTransaction() == null && !durableInNonTxMode)
      return;

    super.endAtomicOperation(rollback);
  }

  @Override
  protected void startAtomicOperation() throws IOException {
    if (storage.getStorageTransaction() == null && !durableInNonTxMode)
      return;

    super.startAtomicOperation();
  }

  @Override
  protected void logFileCreation(String fileName, long fileId) throws IOException {
    if (storage.getStorageTransaction() == null && !durableInNonTxMode)
      return;

    super.logFileCreation(fileName, fileId);
  }
}
