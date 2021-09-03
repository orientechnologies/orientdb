package com.orientechnologies.common.util;

import java.util.Objects;

public class ORawTriple<K, T, V> {
  public final K first;
  public final T second;
  public final V third;

  public ORawTriple(K first, T second, V third) {
    this.first = first;
    this.second = second;
    this.third = third;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ORawTriple<?, ?, ?> that = (ORawTriple<?, ?, ?>) o;
    return Objects.equals(first, that.first)
        && Objects.equals(second, that.second)
        && Objects.equals(third, that.third);
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second, third);
  }
}
