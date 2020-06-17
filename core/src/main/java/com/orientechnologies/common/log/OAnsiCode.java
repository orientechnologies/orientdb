/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.common.log;

import com.orientechnologies.common.parser.OVariableParser;
import com.orientechnologies.common.parser.OVariableParserListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import java.util.Locale;

/**
 * Console ANSI utility class that supports most of the ANSI amenities.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public enum OAnsiCode {
  RESET("\u001B[0m"),

  // COLORS
  BLACK("\u001B[30m"),
  RED("\u001B[31m"),
  GREEN("\u001B[32m"),
  YELLOW("\u001B[33m"),
  BLUE("\u001B[34m"),
  MAGENTA("\u001B[35m"),
  CYAN("\u001B[36m"),
  WHITE("\u001B[37m"),

  HIGH_INTENSITY("\u001B[1m"),
  LOW_INTENSITY("\u001B[2m"),

  ITALIC("\u001B[3m"),
  UNDERLINE("\u001B[4m"),
  BLINK("\u001B[5m"),
  RAPID_BLINK("\u001B[6m"),
  REVERSE_VIDEO("\u001B[7m"),
  INVISIBLE_TEXT("\u001B[8m"),

  BACKGROUND_BLACK("\u001B[40m"),
  BACKGROUND_RED("\u001B[41m"),
  BACKGROUND_GREEN("\u001B[42m"),
  BACKGROUND_YELLOW("\u001B[43m"),
  BACKGROUND_BLUE("\u001B[44m"),
  BACKGROUND_MAGENTA("\u001B[45m"),
  BACKGROUND_CYAN("\u001B[46m"),
  BACKGROUND_WHITE("\u001B[47m"),

  NULL("");

  private String code;

  OAnsiCode(final String code) {
    this.code = code;
  }

  @Override
  public String toString() {
    return code;
  }

  private static final boolean supportsColors;

  public static boolean isSupportsColors() {
    return supportsColors;
  }

  static {
    final String ansiSupport = OGlobalConfiguration.LOG_SUPPORTS_ANSI.getValueAsString();
    if ("true".equalsIgnoreCase(ansiSupport))
      // FORCE ANSI SUPPORT
      supportsColors = true;
    else if ("auto".equalsIgnoreCase(ansiSupport)) {
      // AUTOMATIC CHECK
      if (System.console() != null && !System.getProperty("os.name").contains("Windows"))
        supportsColors = true;
      else supportsColors = false;
    } else
      // DO NOT SUPPORT ANSI
      supportsColors = false;
  }

  public static String format(final String message) {
    return format(message, supportsColors);
  }

  public static String format(final String message, final boolean supportsColors) {
    return (String)
        OVariableParser.resolveVariables(
            message,
            "$ANSI{",
            "}",
            new OVariableParserListener() {
              @Override
              public Object resolve(final String iVariable) {
                final int pos = iVariable.indexOf(' ');

                final String text = pos > -1 ? iVariable.substring(pos + 1) : "";

                if (supportsColors) {
                  final String code = pos > -1 ? iVariable.substring(0, pos) : iVariable;

                  final StringBuilder buffer = new StringBuilder();

                  final String[] codes = code.split(":");
                  for (int i = 0; i < codes.length; ++i)
                    buffer.append(OAnsiCode.valueOf(codes[i].toUpperCase(Locale.ENGLISH)));

                  if (pos > -1) {
                    buffer.append(text);
                    buffer.append(OAnsiCode.RESET);
                  }

                  return buffer.toString();
                }

                return text;
              }
            });
  }
}
