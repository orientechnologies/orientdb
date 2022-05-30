package com.orientechnologies.orient.core.storage.cluster.v2;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

public final class FreeSpaceMapPage extends ODurablePage {

  private static final int CELL_SIZE = 1;
  static final int CELLS_PER_PAGE;
  private static final int LEVELS;
  private static final int LEAVES_START_OFFSET;

  static {
    final int pageCells = MAX_PAGE_SIZE_BYTES / CELL_SIZE;

    LEVELS = Integer.SIZE - Integer.numberOfLeadingZeros(pageCells) - 1;

    final int totalCells = (MAX_PAGE_SIZE_BYTES - NEXT_FREE_POSITION) / CELL_SIZE;
    // amount of leaves in a tree
    CELLS_PER_PAGE = totalCells - ((1 << (LEVELS - 1)) - 1);

    LEAVES_START_OFFSET = nodeOffset(LEVELS, 0);
  }

  public FreeSpaceMapPage(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() {
    final byte[] zeros = new byte[MAX_PAGE_SIZE_BYTES - NEXT_FREE_POSITION];
    setBinaryValue(NEXT_FREE_POSITION, zeros);
  }

  public int findPage(int requiredSize) {
    int nodeIndex = 0;

    final int maxValue = 0xFF & getByteValue(nodeOffset(1, 0));
    if (maxValue == 0 || maxValue < requiredSize) {
      return -1;
    }

    for (int level = 2; level <= LEVELS; level++) {
      final int leftNodeIndex = nodeIndex << 1;
      if (leftNodeIndex >= CELLS_PER_PAGE) {
        return -1;
      }
      final int leftNodeOffset = nodeOffset(level, leftNodeIndex);

      final int leftMax = 0xFF & getByteValue(leftNodeOffset);
      if (leftMax >= requiredSize) {
        nodeIndex <<= 1;
      } else {
        final int rightNodeIndex = (nodeIndex << 1) + 1;

        if (rightNodeIndex >= CELLS_PER_PAGE) {
          return -1;
        }

        final int rightNodeOffset = nodeOffset(level, rightNodeIndex);
        assert (0xFF & getByteValue(rightNodeOffset)) >= requiredSize;
        nodeIndex = (nodeIndex << 1) + 1;
      }
    }

    return nodeIndex;
  }

  public int updatePageMaxFreeSpace(final int pageIndex, final int freeSpace) {
    assert freeSpace < (1 << (CELL_SIZE * 8));
    assert pageIndex >= 0;

    if (pageIndex >= CELLS_PER_PAGE) {
      throw new IllegalArgumentException("Page index " + pageIndex + " exceeds tree capacity");
    }

    int nodeOffset = LEAVES_START_OFFSET + pageIndex * CELL_SIZE;
    int nodeIndex = pageIndex;
    int nodeValue = freeSpace;

    final int oldFreeSpace = 0xFF & getByteValue(nodeOffset);

    for (int level = LEVELS; level > 0; level--) {
      final int prevValue = 0xFF & getByteValue(nodeOffset);
      if (prevValue == nodeValue) {
        return 0xFF & getByteValue(nodeOffset(1, 0));
      }

      setByteValue(nodeOffset, (byte) nodeValue);
      if (level == 1) {
        return nodeValue;
      }

      final int siblingIndex;

      if ((nodeIndex & 1) == 0) {
        siblingIndex = nodeIndex + 1;
      } else {
        siblingIndex = nodeIndex - 1;
      }

      final int siblingOffset = nodeOffset(level, siblingIndex);
      final int siblingValue;

      if (siblingOffset + 2 <= MAX_PAGE_SIZE_BYTES) {
        siblingValue = 0xFF & getByteValue(siblingOffset);
      } else {
        siblingValue = nodeValue;
      }

      nodeValue = Math.max(nodeValue, siblingValue);
      nodeIndex = nodeIndex >> 1;
      nodeOffset = nodeOffset(level - 1, nodeIndex);
    }

    // unreachable
    assert false;
    return 0;
  }

  private static int nodeOffset(int nodeLevel, int nodeIndex) {
    return NEXT_FREE_POSITION + ((1 << (nodeLevel - 1)) - 1 + nodeIndex) * CELL_SIZE;
  }
}
