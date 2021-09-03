package com.orientechnologies.orient.core.sql.method.misc;

import java.util.Iterator;
import java.util.List;

/**
 * A simple implementation of an Iterable that is used for testing the asList, asSet, and asMap
 * methods.
 *
 * @author Michael MacFadden
 */
class TestIterable<T> implements Iterable<T> {
  private List<T> values;

  TestIterable(List<T> values) {
    this.values = values;
  }

  @Override
  public Iterator<T> iterator() {
    return values.iterator();
  }
}
