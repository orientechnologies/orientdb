/*
 * cron4j - A pure Java cron-like scheduler
 * 
 * Copyright (C) 2007-2010 Carlo Pelliccia (www.sauronsoftware.it)
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License 2.1 for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License version 2.1 along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.orientechnologies.orient.server.schedule;

import java.util.ArrayList;

/**
 * <p>
 * A ValueMatcher whose rules are in a plain array of integer values. When asked to validate a value, this ValueMatcher checks if it
 * is in the array.
 * </p>
 * 
 * @author Carlo Pelliccia
 */

public class OIntArrayValueMatcher implements OValueMatcher {

  /**
   * The accepted values.
   */
  private int[] values;

  /**
   * Builds the ValueMatcher.
   * 
   * @param integers
   *          An ArrayList of Integer elements, one for every value accepted by the matcher. The match() method will return true
   *          only if its parameter will be one of this list.
   */
  public OIntArrayValueMatcher(ArrayList integers) {
    int size = integers.size();
    values = new int[size];
    for (int i = 0; i < size; i++) {
      try {
        values[i] = ((Integer) integers.get(i)).intValue();
      } catch (Exception e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }
  }

  /**
   * Returns true if the given value is included in the matcher list.
   */
  public boolean match(int value) {
    for (int i = 0; i < values.length; i++) {
      if (values[i] == value) {
        return true;
      }
    }
    return false;
  }

}
