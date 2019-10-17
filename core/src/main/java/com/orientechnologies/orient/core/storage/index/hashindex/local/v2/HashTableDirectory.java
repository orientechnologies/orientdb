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

package com.orientechnologies.orient.core.storage.index.hashindex.local.v2;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

import java.io.IOException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/14/14
 */
public class HashTableDirectory extends ODurableComponent {
  static final int ITEM_SIZE = OLongSerializer.LONG_SIZE;

  private static final int LEVEL_SIZE = LocalHashTableV2.MAX_LEVEL_SIZE;

  static final int BINARY_LEVEL_SIZE = LEVEL_SIZE * ITEM_SIZE + 3 * OByteSerializer.BYTE_SIZE;

  private long fileId;

  private final long firstEntryIndex;

  HashTableDirectory(final String defaultExtension, final String name, final String lockName,
      final OAbstractPaginatedStorage storage) {
    super(storage, name, defaultExtension, lockName);
    this.firstEntryIndex = 0;
  }

  public void create(final OAtomicOperation atomicOperation) throws IOException {
    fileId = addFile(atomicOperation, getFullName());
    init(atomicOperation);
  }

  private void init(final OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry firstEntry = loadPageForWrite(atomicOperation, fileId, firstEntryIndex, true, true);

    if (firstEntry == null) {
      firstEntry = addPage(atomicOperation, fileId);
      assert firstEntry.getPageIndex() == 0;
    }

    try {
      final DirectoryFirstPageV2 firstPage = new DirectoryFirstPageV2(firstEntry);

      firstPage.setTreeSize(0);
      firstPage.setTombstone(-1);

    } finally {
      releasePageFromWrite(atomicOperation, firstEntry);
    }
  }

  public void open(final OAtomicOperation atomicOperation) throws IOException {
    fileId = openFile(atomicOperation, getFullName());
    final int filledUpTo = (int) getFilledUpTo(atomicOperation, fileId);

    for (int i = 0; i < filledUpTo; i++) {
      final OCacheEntry entry = loadPageForRead(atomicOperation, fileId, i, true);
      assert entry != null;

      releasePageFromRead(atomicOperation, entry);
    }
  }

  public void close() throws IOException {
    readCache.closeFile(fileId, true, writeCache);
  }

  public void delete(final OAtomicOperation atomicOperation) throws IOException {
    deleteFile(atomicOperation, fileId);
  }

  int addNewNode(final byte maxLeftChildDepth, final byte maxRightChildDepth, final byte nodeLocalDepth, final long[] newNode,
      final OAtomicOperation atomicOperation) throws IOException {
    int nodeIndex;
    final OCacheEntry firstEntry = loadPageForWrite(atomicOperation, fileId, firstEntryIndex, true, true);
    try {
      final DirectoryFirstPageV2 firstPage = new DirectoryFirstPageV2(firstEntry);

      final int tombstone = firstPage.getTombstone();

      if (tombstone >= 0) {
        nodeIndex = tombstone;
      } else {
        nodeIndex = firstPage.getTreeSize();
        firstPage.setTreeSize(nodeIndex + 1);
      }

      if (nodeIndex < DirectoryFirstPageV2.NODES_PER_PAGE) {
        @SuppressWarnings("UnnecessaryLocalVariable")
        final int localNodeIndex = nodeIndex;

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
        final int pageIndex = nodeIndex / DirectoryPageV2.NODES_PER_PAGE;
        final int localLevel = nodeIndex % DirectoryPageV2.NODES_PER_PAGE;

        OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true, true);
        while (cacheEntry == null || cacheEntry.getPageIndex() < pageIndex) {
          if (cacheEntry != null) {
            releasePageFromWrite(atomicOperation, cacheEntry);
          }

          cacheEntry = addPage(atomicOperation, fileId);
        }

        try {
          final DirectoryPageV2 page = new DirectoryPageV2(cacheEntry);

          page.setMaxLeftChildDepth(localLevel, maxLeftChildDepth);
          page.setMaxRightChildDepth(localLevel, maxRightChildDepth);
          page.setNodeLocalDepth(localLevel, nodeLocalDepth);

          if (tombstone >= 0) {
            firstPage.setTombstone((int) page.getPointer(localLevel, 0));
          }

          for (int i = 0; i < newNode.length; i++) {
            page.setPointer(localLevel, i, newNode[i]);
          }

        } finally {
          releasePageFromWrite(atomicOperation, cacheEntry);
        }
      }

    } finally {
      releasePageFromWrite(atomicOperation, firstEntry);
    }

    return nodeIndex;
  }

  void deleteNode(final int nodeIndex, final OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry firstEntry = loadPageForWrite(atomicOperation, fileId, firstEntryIndex, true, true);
    try {
      final DirectoryFirstPageV2 firstPage = new DirectoryFirstPageV2(firstEntry);
      if (nodeIndex < DirectoryFirstPageV2.NODES_PER_PAGE) {
        firstPage.setPointer(nodeIndex, 0, firstPage.getTombstone());
        firstPage.setTombstone(nodeIndex);
      } else {
        final int pageIndex = nodeIndex / DirectoryPageV2.NODES_PER_PAGE;
        final int localNodeIndex = nodeIndex % DirectoryPageV2.NODES_PER_PAGE;

        final OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true, true);
        try {
          final DirectoryPageV2 page = new DirectoryPageV2(cacheEntry);

          page.setPointer(localNodeIndex, 0, firstPage.getTombstone());
          firstPage.setTombstone(nodeIndex);

        } finally {
          releasePageFromWrite(atomicOperation, cacheEntry);
        }
      }
    } finally {
      releasePageFromWrite(atomicOperation, firstEntry);
    }
  }

  byte getMaxLeftChildDepth(final int nodeIndex, final OAtomicOperation atomicOperation) throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, false, atomicOperation);
    try {
      return page.getMaxLeftChildDepth(getLocalNodeIndex(nodeIndex));
    } finally {
      releasePage(page, false, atomicOperation);
    }
  }

  void setMaxLeftChildDepth(final int nodeIndex, final byte maxLeftChildDepth, final OAtomicOperation atomicOperation)
      throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, true, atomicOperation);
    try {
      page.setMaxLeftChildDepth(getLocalNodeIndex(nodeIndex), maxLeftChildDepth);
    } finally {
      releasePage(page, true, atomicOperation);
    }
  }

  byte getMaxRightChildDepth(final int nodeIndex, final OAtomicOperation atomicOperation) throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, false, atomicOperation);
    try {
      return page.getMaxRightChildDepth(getLocalNodeIndex(nodeIndex));
    } finally {
      releasePage(page, false, atomicOperation);
    }
  }

  void setMaxRightChildDepth(final int nodeIndex, final byte maxRightChildDepth, final OAtomicOperation atomicOperation)
      throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, true, atomicOperation);
    try {
      page.setMaxRightChildDepth(getLocalNodeIndex(nodeIndex), maxRightChildDepth);
    } finally {
      releasePage(page, true, atomicOperation);
    }
  }

  byte getNodeLocalDepth(final int nodeIndex, final OAtomicOperation atomicOperation) throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, false, atomicOperation);
    try {
      return page.getNodeLocalDepth(getLocalNodeIndex(nodeIndex));
    } finally {
      releasePage(page, false, atomicOperation);
    }
  }

  void setNodeLocalDepth(final int nodeIndex, final byte localNodeDepth, final OAtomicOperation atomicOperation)
      throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, true, atomicOperation);
    try {
      page.setNodeLocalDepth(getLocalNodeIndex(nodeIndex), localNodeDepth);
    } finally {
      releasePage(page, true, atomicOperation);
    }
  }

  long[] getNode(final int nodeIndex, final OAtomicOperation atomicOperation) throws IOException {
    final long[] node = new long[LEVEL_SIZE];
    final DirectoryPageV2 page = loadPage(nodeIndex, false, atomicOperation);

    try {
      final int localNodeIndex = getLocalNodeIndex(nodeIndex);
      for (int i = 0; i < LEVEL_SIZE; i++) {
        node[i] = page.getPointer(localNodeIndex, i);
      }
    } finally {
      releasePage(page, false, atomicOperation);
    }

    return node;
  }

  void setNode(final int nodeIndex, final long[] node, final OAtomicOperation atomicOperation) throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, true, atomicOperation);
    try {
      final int localNodeIndex = getLocalNodeIndex(nodeIndex);
      for (int i = 0; i < LEVEL_SIZE; i++) {
        page.setPointer(localNodeIndex, i, node[i]);
      }
    } finally {
      releasePage(page, true, atomicOperation);
    }
  }

  long getNodePointer(final int nodeIndex, final int index, final OAtomicOperation atomicOperation) throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, false, atomicOperation);
    try {
      return page.getPointer(getLocalNodeIndex(nodeIndex), index);
    } finally {
      releasePage(page, false, atomicOperation);
    }
  }

  void setNodePointer(final int nodeIndex, final int index, final long pointer, final OAtomicOperation atomicOperation)
      throws IOException {
    final DirectoryPageV2 page = loadPage(nodeIndex, true, atomicOperation);
    try {
      page.setPointer(getLocalNodeIndex(nodeIndex), index, pointer);
    } finally {
      releasePage(page, true, atomicOperation);
    }
  }

  public void clear(final OAtomicOperation atomicOperation) throws IOException {
    truncateFile(atomicOperation, fileId);

    init(atomicOperation);
  }

  public void flush() {
    writeCache.flush(fileId);
  }

  private DirectoryPageV2 loadPage(final int nodeIndex, final boolean exclusiveLock, final OAtomicOperation atomicOperation)
      throws IOException {
    if (nodeIndex < DirectoryFirstPageV2.NODES_PER_PAGE) {
      final OCacheEntry cacheEntry;

      if (exclusiveLock) {
        cacheEntry = loadPageForWrite(atomicOperation, fileId, firstEntryIndex, true, true);
      } else {
        cacheEntry = loadPageForRead(atomicOperation, fileId, firstEntryIndex, true);
      }

      return new DirectoryFirstPageV2(cacheEntry);
    }

    final int pageIndex = nodeIndex / DirectoryPageV2.NODES_PER_PAGE;

    final OCacheEntry cacheEntry;

    if (exclusiveLock) {
      cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true, true);
    } else {
      cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, true);
    }

    return new DirectoryPageV2(cacheEntry);
  }

  private void releasePage(final DirectoryPageV2 page, final boolean exclusiveLock, final OAtomicOperation atomicOperation)
      throws IOException {
    final OCacheEntry cacheEntry = page.getCacheEntry();

    if (exclusiveLock) {
      releasePageFromWrite(atomicOperation, cacheEntry);
    } else {
      releasePageFromRead(atomicOperation, cacheEntry);
    }

  }

  private static int getLocalNodeIndex(final int nodeIndex) {
    if (nodeIndex < DirectoryFirstPageV2.NODES_PER_PAGE) {
      return nodeIndex;
    }

    return (nodeIndex - DirectoryFirstPageV2.NODES_PER_PAGE) % DirectoryPageV2.NODES_PER_PAGE;
  }
}
