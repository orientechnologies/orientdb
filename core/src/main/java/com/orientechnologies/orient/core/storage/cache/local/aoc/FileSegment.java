package com.orientechnologies.orient.core.storage.cache.local.aoc;

public final class FileSegment {
  final int segmentIndex;
  int freeSpace;

  public FileSegment(int segmentIndex) {
    this.segmentIndex = segmentIndex;
  }
}
