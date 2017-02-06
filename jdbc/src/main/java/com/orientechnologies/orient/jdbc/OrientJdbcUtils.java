/**
 * Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more information: http://www.orientechnologies.com
 */
package com.orientechnologies.orient.jdbc;

import java.util.regex.Pattern;

/**
 * Created by frank on 06/02/2017.
 */
public class OrientJdbcUtils {


  public static boolean like(final String str, final String expr) {
    String regex = quotemeta(expr);
    regex = regex.replace("_", ".").replace("%", ".*?");
    Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    return p.matcher(str).matches();
  }

  public static String quotemeta(String s) {
    if (s == null) {
      throw new IllegalArgumentException("String cannot be null");
    }

    int len = s.length();
    if (len == 0) {
      return "";
    }

    StringBuilder sb = new StringBuilder(len * 2);
    for (int i = 0; i < len; i++) {
      char c = s.charAt(i);
      if ("[](){}.*+?$^|#\\".indexOf(c) != -1) {
        sb.append("\\");
      }
      sb.append(c);
    }
    return sb.toString();
  }
}
