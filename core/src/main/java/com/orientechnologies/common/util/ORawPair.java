package com.orientechnologies.common.util;

/**
 * Container for pair of non null objects.
 */
public class ORawPair<V1, V2> {
  private final V1 first;
  private final V2 second;

  public ORawPair(V1 first, V2 second) {
    this.first = first;
    this.second = second;
  }

  public V1 getFirst() {
    return first;
  }

  public V2 getSecond() {
    return second;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    ORawPair<?, ?> oRawPair = (ORawPair<?, ?>) o;

    if (!first.equals(oRawPair.first))
      return false;
    return second.equals(oRawPair.second);

  }

  @Override
  public int hashCode() {
    int result = first.hashCode();
    result = 31 * result + second.hashCode();
    return result;
  }
}
