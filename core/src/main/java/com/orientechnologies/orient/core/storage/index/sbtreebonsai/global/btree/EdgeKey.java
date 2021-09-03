package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree;

public final class EdgeKey implements Comparable<EdgeKey> {

  public final long ridBagId;
  public final int targetCluster;
  public final long targetPosition;

  public EdgeKey(long ridBagId, int targetCluster, long targetPosition) {
    this.ridBagId = ridBagId;
    this.targetCluster = targetCluster;
    this.targetPosition = targetPosition;
  }

  @Override
  public String toString() {
    return "EdgeKey{"
        + " ridBagId="
        + ridBagId
        + ", targetCluster="
        + targetCluster
        + ", targetPosition="
        + targetPosition
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EdgeKey edgeKey = (EdgeKey) o;

    if (ridBagId != edgeKey.ridBagId) {
      return false;
    }
    if (targetCluster != edgeKey.targetCluster) {
      return false;
    }
    return targetPosition == edgeKey.targetPosition;
  }

  @Override
  public int hashCode() {
    int result = (int) (ridBagId ^ (ridBagId >>> 32));
    result = 31 * result + targetCluster;
    result = 31 * result + (int) (targetPosition ^ (targetPosition >>> 32));
    return result;
  }

  @Override
  public int compareTo(final EdgeKey other) {
    if (ridBagId != other.ridBagId) {
      if (ridBagId < other.ridBagId) {
        return -1;
      } else {
        return 1;
      }
    }

    if (targetCluster != other.targetCluster) {
      if (targetCluster < other.targetCluster) {
        return -1;
      } else {
        return 1;
      }
    }

    if (targetPosition < other.targetPosition) {
      return -1;
    } else if (targetPosition > other.targetPosition) {
      return 1;
    }

    return 0;
  }
}
