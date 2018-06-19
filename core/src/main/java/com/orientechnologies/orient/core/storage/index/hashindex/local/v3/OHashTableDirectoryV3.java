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

package com.orientechnologies.orient.core.storage.index.hashindex.local.v3;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.io.IOException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/14/14
 */
public final class OHashTableDirectoryV3 extends ODurableComponent {
  static final int ITEM_SIZE = OLongSerializer.LONG_SIZE;

  private static final int LEVEL_SIZE = OLocalHashTableV3.MAX_LEVEL_SIZE;

  static final int BINARY_LEVEL_SIZE = LEVEL_SIZE * ITEM_SIZE + 3 * OByteSerializer.BYTE_SIZE;

  private long fileId;

  private final long firstEntryIndex;

  OHashTableDirectoryV3(final String defaultExtension, final String name, final String lockName,
      final OAbstractPaginatedStorage storage) {
    super(storage, name, defaultExtension, lockName);
    this.firstEntryIndex = 0;
  }

  public void create(final OAtomicOperation atomicOperation) throws IOException {
    fileId = addFile(getFullName());
    init(atomicOperation);
  }

  void createOnRestore(final OAtomicOperation atomicOperation) throws IOException {
    fileId = addFileOnRestore(getFullName());

    init(atomicOperation);
  }

  long getFileId() {
    return fileId;
  }

  private void init(final OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry firstEntry = loadPageForWrite(fileId, firstEntryIndex, true);

    if (firstEntry == null) {
      firstEntry = addPage(fileId);
      assert firstEntry.getPageIndex() == 0;
    }

    pinPage(firstEntry);

    ODirectoryFirstPageV3 firstPage = null;
    try {
      firstPage = new ODirectoryFirstPageV3(firstEntry);

      firstPage.setTreeSize(0);
      firstPage.setTombstone(-1);

    } finally {
      releasePageFromWrite(firstPage, atomicOperation);
    }
  }

  public void open() throws IOException {
    fileId = openFile(getFullName());
    final int filledUpTo = (int) getFilledUpTo(fileId);

    for (int i = 0; i < filledUpTo; i++) {
      final OCacheEntry entry = loadPageForRead(fileId, i, true);
      assert entry != null;

      pinPage(entry);
      releasePageFromRead(entry);
    }
  }

  public void close() {
    readCache.closeFile(fileId, true, writeCache);
  }

  public void delete() throws IOException {
    deleteFile(fileId);
  }

  void deleteWithoutOpen() throws IOException {
    if (isFileExists(getFullName())) {
      fileId = openFile(getFullName());
      deleteFile(fileId);
    }
  }

  int addNewNode(final byte maxLeftChildDepth, final byte maxRightChildDepth, final byte nodeLocalDepth, final long[] newNode,
      final OAtomicOperation atomicOperation) throws IOException {
    int nodeIndex;
    ODirectoryFirstPageV3 firstPage = null;
    final OCacheEntry firstEntry = loadPageForWrite(fileId, firstEntryIndex, true);
    try {
      firstPage = new ODirectoryFirstPageV3(firstEntry);

      final int tombstone = firstPage.getTombstone();

      if (tombstone >= 0)
        nodeIndex = tombstone;
      else {
        nodeIndex = firstPage.getTreeSize();
        firstPage.setTreeSize(nodeIndex + 1);
      }

      if (nodeIndex < ODirectoryFirstPageV3.NODES_PER_PAGE) {
        final int localNodeIndex = nodeIndex;

        firstPage.markNodeAsAllocated(localNodeIndex);

        firstPage.setMaxLeftChildDepth(localNodeIndex, maxLeftChildDepth);
        firstPage.setMaxRightChildDepth(localNodeIndex, maxRightChildDepth);
        firstPage.setNodeLocalDepth(localNodeIndex, nodeLocalDepth);

        if (tombstone >= 0) {
          firstPage.setTombstone((int) firstPage.getPointer(nodeIndex, 0));
        }

        for (int i = 0; i < newNode.length; i++) {
          firstPage.setPointer(localNodeIndex, i, newNode[i]);
        }
      } else {
        final int pageIndex = nodeIndex / ODirectoryPageV3.NODES_PER_PAGE;
        final int localLevel = nodeIndex % ODirectoryPageV3.NODES_PER_PAGE;

        OCacheEntry cacheEntry = loadPageForWrite(fileId, pageIndex, true);
        while (cacheEntry == null || cacheEntry.getPageIndex() < pageIndex) {
          if (cacheEntry != null) {
            releasePageFromWrite(new ODurablePage(cacheEntry), atomicOperation);
          }

          cacheEntry = addPage(fileId);
        }

        ODirectoryPageV3 page = null;
        try {
          page = new ODirectoryPageV3(cacheEntry);

          page.setMaxLeftChildDepth(localLevel, maxLeftChildDepth);
          page.setMaxRightChildDepth(localLevel, maxRightChildDepth);
          page.setNodeLocalDepth(localLevel, nodeLocalDepth);

          if (tombstone >= 0) {
            firstPage.setTombstone((int) page.getPointer(localLevel, 0));
          }

          for (int i = 0; i < newNode.length; i++) {
            page.setPointer(localLevel, i, newNode[i]);
          }

          page.markNodeAsAllocated(localLevel);
          pinPage(cacheEntry);
        } finally {
          releasePageFromWrite(page, atomicOperation);
        }
      }
    } finally {
      releasePageFromWrite(firstPage, atomicOperation);
    }

    return nodeIndex;
  }

  void deleteNode(final int nodeIndex, final OAtomicOperation atomicOperation) throws IOException {
    ODirectoryFirstPageV3 firstPage = null;
    final OCacheEntry firstEntry = loadPageForWrite(fileId, firstEntryIndex, true);
    try {
      firstPage = new ODirectoryFirstPageV3(firstEntry);
      if (nodeIndex < ODirectoryFirstPageV3.NODES_PER_PAGE) {
        firstPage.setPointer(nodeIndex, 0, firstPage.getTombstone());
        firstPage.setTombstone(nodeIndex);
        firstPage.markNodeAsDeleted(nodeIndex);
      } else {
        final int pageIndex = nodeIndex / ODirectoryPageV3.NODES_PER_PAGE;
        final int localNodeIndex = nodeIndex % ODirectoryPageV3.NODES_PER_PAGE;

        ODirectoryPageV3 page = null;
        final OCacheEntry cacheEntry = loadPageForWrite(fileId, pageIndex, true);
        try {
          page = new ODirectoryPageV3(cacheEntry);

          page.setPointer(localNodeIndex, 0, firstPage.getTombstone());
          firstPage.setTombstone(nodeIndex);

          page.markNodeAsDeleted(localNodeIndex);
        } finally {
          releasePageFromWrite(page, atomicOperation);
        }
      }
    } finally {
      releasePageFromWrite(firstPage, atomicOperation);
    }
  }

  byte getMaxLeftChildDepth(final int nodeIndex) throws IOException {
    final ODirectoryPageV3 page = loadPage(nodeIndex, false);
    try {
      return page.getMaxLeftChildDepth(getLocalNodeIndex(nodeIndex));
    } finally {
      releasePage(page, false, null);
    }
  }

  void setMaxLeftChildDepth(final int nodeIndex, final byte maxLeftChildDepth, final OAtomicOperation atomicOperation)
      throws IOException {
    final ODirectoryPageV3 page = loadPage(nodeIndex, true);
    try {
      page.setMaxLeftChildDepth(getLocalNodeIndex(nodeIndex), maxLeftChildDepth);
    } finally {
      releasePage(page, true, atomicOperation);
    }
  }

  byte getMaxRightChildDepth(final int nodeIndex) throws IOException {
    final ODirectoryPageV3 page = loadPage(nodeIndex, false);
    try {
      return page.getMaxRightChildDepth(getLocalNodeIndex(nodeIndex));
    } finally {
      releasePage(page, false, null);
    }
  }

  void setMaxRightChildDepth(final int nodeIndex, final byte maxRightChildDepth, final OAtomicOperation atomicOperation)
      throws IOException {
    final ODirectoryPageV3 page = loadPage(nodeIndex, true);
    try {
      page.setMaxRightChildDepth(getLocalNodeIndex(nodeIndex), maxRightChildDepth);
    } finally {
      releasePage(page, true, atomicOperation);
    }
  }

  byte getNodeLocalDepth(final int nodeIndex) throws IOException {
    final ODirectoryPageV3 page = loadPage(nodeIndex, false);
    try {
      return page.getNodeLocalDepth(getLocalNodeIndex(nodeIndex));
    } finally {
      releasePage(page, false, null);
    }
  }

  void setNodeLocalDepth(final int nodeIndex, final byte localNodeDepth, final OAtomicOperation atomicOperation)
      throws IOException {
    final ODirectoryPageV3 page = loadPage(nodeIndex, true);
    try {
      page.setNodeLocalDepth(getLocalNodeIndex(nodeIndex), localNodeDepth);
    } finally {
      releasePage(page, true, atomicOperation);
    }
  }

  long[] getNode(final int nodeIndex) throws IOException {
    final long[] node = new long[LEVEL_SIZE];

    final ODirectoryPageV3 page = loadPage(nodeIndex, false);
    try {
      final int localNodeIndex = getLocalNodeIndex(nodeIndex);
      for (int i = 0; i < LEVEL_SIZE; i++)
        node[i] = page.getPointer(localNodeIndex, i);
    } finally {
      releasePage(page, false, null);
    }

    return node;
  }

  void setNode(final int nodeIndex, final long[] node, final OAtomicOperation atomicOperation) throws IOException {
    final ODirectoryPageV3 page = loadPage(nodeIndex, true);
    try {
      final int localNodeIndex = getLocalNodeIndex(nodeIndex);
      for (int i = 0; i < LEVEL_SIZE; i++)
        page.setPointer(localNodeIndex, i, node[i]);
    } finally {
      releasePage(page, true, atomicOperation);
    }
  }

  long getNodePointer(final int nodeIndex, final int index) throws IOException {
    final ODirectoryPageV3 page = loadPage(nodeIndex, false);
    try {
      return page.getPointer(getLocalNodeIndex(nodeIndex), index);
    } finally {
      releasePage(page, false, null);
    }
  }

  void setNodePointer(final int nodeIndex, final int index, final long pointer, final OAtomicOperation atomicOperation)
      throws IOException {
    final ODirectoryPageV3 page = loadPage(nodeIndex, true);
    try {
      page.setPointer(getLocalNodeIndex(nodeIndex), index, pointer);
    } finally {
      releasePage(page, true, atomicOperation);
    }
  }

  public void clear(final OAtomicOperation atomicOperation) throws IOException {
    truncateFile(fileId);
    init(atomicOperation);
  }

  public void flush() {
    writeCache.flush(fileId);
  }

  private ODirectoryPageV3 loadPage(final int nodeIndex, final boolean exclusiveLock) throws IOException {
    if (nodeIndex < ODirectoryFirstPageV3.NODES_PER_PAGE) {
      final OCacheEntry cacheEntry;

      if (exclusiveLock)
        cacheEntry = loadPageForWrite(fileId, firstEntryIndex, true);
      else
        cacheEntry = loadPageForRead(fileId, firstEntryIndex, true);

      return new ODirectoryFirstPageV3(cacheEntry);
    }

    final int pageIndex = nodeIndex / ODirectoryPageV3.NODES_PER_PAGE;

    final OCacheEntry cacheEntry;

    if (exclusiveLock)
      cacheEntry = loadPageForWrite(fileId, pageIndex, true);
    else
      cacheEntry = loadPageForRead(fileId, pageIndex, true);

    return new ODirectoryPageV3(cacheEntry);
  }

  private void releasePage(final ODirectoryPageV3 page, final boolean exclusiveLock, final OAtomicOperation atomicOperation) {
    final OCacheEntry cacheEntry = page.getCacheEntry();

    if (exclusiveLock)
      releasePageFromWrite(page, atomicOperation);
    else
      releasePageFromRead(cacheEntry);

  }

  private static int getLocalNodeIndex(final int nodeIndex) {
    if (nodeIndex < ODirectoryFirstPageV3.NODES_PER_PAGE) {
      return nodeIndex;
    }

    return (nodeIndex - ODirectoryFirstPageV3.NODES_PER_PAGE) % ODirectoryPageV3.NODES_PER_PAGE;
  }
}
