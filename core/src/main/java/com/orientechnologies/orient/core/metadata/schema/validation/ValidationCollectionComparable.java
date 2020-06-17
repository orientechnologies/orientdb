package com.orientechnologies.orient.core.metadata.schema.validation;

import java.util.Collection;

public class ValidationCollectionComparable implements Comparable<Object> {

  private int size;

  public ValidationCollectionComparable(int size) {
    this.size = size;
  }

  @Override
  public int compareTo(Object o) {
    return size - ((Collection<Object>) o).size();
  }
}
