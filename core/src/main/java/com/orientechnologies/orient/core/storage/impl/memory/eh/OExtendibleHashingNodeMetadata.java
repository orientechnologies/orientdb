package com.orientechnologies.orient.core.storage.impl.memory.eh;

/**
 * @author Andrey Lomakin
 * @since 30.01.13
 */
public class OExtendibleHashingNodeMetadata {
  private byte maxLeftChildDepth;
  private byte maxRightChildDepth;

  private byte nodeLocalDepth;

  public OExtendibleHashingNodeMetadata(byte maxLeftChildDepth, byte maxRightChildDepth, byte nodeLocalDepth) {
    this.maxLeftChildDepth = maxLeftChildDepth;
    this.maxRightChildDepth = maxRightChildDepth;
    this.nodeLocalDepth = nodeLocalDepth;
  }

  public int getMaxLeftChildDepth() {
    return maxLeftChildDepth & 0xFF;
  }

  public void setMaxLeftChildDepth(int maxLeftChildDepth) {
    this.maxLeftChildDepth = (byte) maxLeftChildDepth;
  }

  public int getMaxRightChildDepth() {
    return maxRightChildDepth & 0xFF;
  }

  public void setMaxRightChildDepth(int maxRightChildDepth) {
    this.maxRightChildDepth = (byte) maxRightChildDepth;
  }

  public int getNodeLocalDepth() {
    return nodeLocalDepth & 0xFF;
  }

  public void setNodeLocalDepth(int nodeLocalDepth) {
    this.nodeLocalDepth = (byte) nodeLocalDepth;
  }

  public void incrementLocalNodeDepth() {
    nodeLocalDepth++;
  }
}
