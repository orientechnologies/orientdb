package com.orientechnologies.common.util;

import java.util.Objects;

public final class OQuarto<T1, T2, T3, T4> {
  public final T1 one;
  public final T2 two;
  public final T3 three;
  public final T4 four;

  public OQuarto(T1 one, T2 two, T3 three, T4 four) {
    this.one = one;
    this.two = two;
    this.three = three;
    this.four = four;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OQuarto<?, ?, ?, ?> oQuarto = (OQuarto<?, ?, ?, ?>) o;
    return Objects.equals(one, oQuarto.one)
        && Objects.equals(two, oQuarto.two)
        && Objects.equals(three, oQuarto.three)
        && Objects.equals(four, oQuarto.four);
  }

  @Override
  public int hashCode() {
    return Objects.hash(one, two, three, four);
  }

  @Override
  public String toString() {
    return "OQuarto{" + "one=" + one + ", two=" + two + ", three=" + three + ", four=" + four + '}';
  }
}
