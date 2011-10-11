package com.orientechnologies.common.util;

import java.util.Comparator;
import java.util.Locale;

/**
 * Compares strings without taking into account their case.
 */
public class OCaseIncentiveComparator implements Comparator<String> {
    public int compare(final String stringOne, final String stringTwo) {
        return stringOne.toLowerCase().compareTo(stringTwo.toLowerCase());
    }
}
