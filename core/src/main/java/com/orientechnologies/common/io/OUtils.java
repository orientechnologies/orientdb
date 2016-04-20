/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.common.io;

import java.util.IllegalFormatException;

public class OUtils {
  public static boolean equals(final Object a, final Object b) {
    if (a == b)
      return true;

    if (a != null)
      return a.equals(b);
    return b.equals(a);
  }

  public static String camelCase(final String iText) {
    return Character.toUpperCase(iText.charAt(0)) + iText.substring(1);
  }

  /**
   * Parses the size specifier formatted in the JVM style, like 1024k or 4g.
   * Following units are supported: k or K – kilobytes, m or M – megabytes, g or G – gigabytes.
   * If no unit provided, it is bytes.
   *
   * @param text the text to parse.
   * @return the parsed size value.
   * @throws IllegalArgumentException if size specifier is not recognized as valid.
   */
  public static long parseVmArgsSize(String text) throws IllegalArgumentException {
    if (text == null)
      throw new IllegalArgumentException("text can't be null");
    if (text.length() == 0)
      throw new IllegalArgumentException("text can't be empty");

    final char unit = text.charAt(text.length() - 1);
    if (Character.isDigit(unit))
      return Long.parseLong(text);

    final long value = Long.parseLong(text.substring(0, text.length() - 1));
    switch (Character.toLowerCase(unit)) {
    case 'g':
      return value * OFileUtils.GIGABYTE;
    case 'm':
      return value * OFileUtils.MEGABYTE;
    case 'k':
      return value * OFileUtils.KILOBYTE;
    }

    throw new IllegalArgumentException("text '" + text + "' is not a size specifier.");
  }
}
