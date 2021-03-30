package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree;

final class EdgeKey implements Comparable<EdgeKey> {

  final int ownerId;
  final int targetCluster;
  final long targetPosition;

  EdgeKey(int ownerId, int targetCluster, long targetPosition) {
    this.ownerId = ownerId;
    this.targetCluster = targetCluster;
    this.targetPosition = targetPosition;
  }

  @Override
  public String toString() {
    return "EdgeKey{"
        + " ownerId="
        + ownerId
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

    if (ownerId != edgeKey.ownerId) {
      return false;
    }
    if (targetCluster != edgeKey.targetCluster) {
      return false;
    }
    return targetPosition == edgeKey.targetPosition;
  }

  @Override
  public int hashCode() {
    int result = ownerId;
    result = 31 * result + targetCluster;
    result = 31 * result + (int) (targetPosition ^ (targetPosition >>> 32));
    return result;
  }

  @Override
  public int compareTo(final EdgeKey other) {
    if (ownerId != other.ownerId) {
      if (ownerId < other.ownerId) {
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
