package com.orientechnologies.orient.core.storage.index.learned;

public class ApproximatePosition {
  private final int position;
  private final int low;
  private final int high;

  public ApproximatePosition(final int position, final int low, final int high) {
    this.position = position;
    this.low = low;
    this.high = high;
  }

  public Integer getRangeLowerBound() {
    return 0;
  }

  public Integer getRangeUpperBound() {
    return 0;
  }
}
