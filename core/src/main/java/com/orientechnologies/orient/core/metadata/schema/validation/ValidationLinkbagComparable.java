package com.orientechnologies.orient.core.metadata.schema.validation;

import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;

public class ValidationLinkbagComparable implements Comparable<Object> {

  private int size;

  public ValidationLinkbagComparable(int size) {
    this.size = size;
  }

  @Override
  public int compareTo(Object o) {
    return size - ((ORidBag) o).size();
  }
}
