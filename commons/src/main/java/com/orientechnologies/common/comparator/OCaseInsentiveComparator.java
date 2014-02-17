package com.orientechnologies.common.comparator;

import java.util.Comparator;

/**
 * Compares strings without taking into account their case.
 */
public class OCaseInsentiveComparator implements Comparator<String> {
  public int compare(final String stringOne, final String stringTwo) {
    return stringOne.compareToIgnoreCase(stringTwo);
  }
}
