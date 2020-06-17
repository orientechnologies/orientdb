package com.orientechnologies.orient.core.metadata.schema.validation;

import java.util.Map;

public class ValidationMapComparable implements Comparable<Object> {

  private int size;

  public ValidationMapComparable(int size) {
    this.size = size;
  }

  @Override
  public int compareTo(Object o) {
    return size - ((Map<Object, Object>) o).size();
  }
}
