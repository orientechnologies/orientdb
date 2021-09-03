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

package com.orientechnologies.common.parser;

import com.orientechnologies.common.log.OLogManager;

/**
 * Resolve entity class and descriptors using the paths configured.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (luca.garulli--at--assetdata.it)
 */
public class OVariableParser {
  public static Object resolveVariables(
      final String iText,
      final String iBegin,
      final String iEnd,
      final OVariableParserListener iListener) {
    return resolveVariables(iText, iBegin, iEnd, iListener, null);
  }

  public static Object resolveVariables(
      final String iText,
      final String iBegin,
      final String iEnd,
      final OVariableParserListener iListener,
      final Object iDefaultValue) {
    if (iListener == null)
      throw new IllegalArgumentException("Missed VariableParserListener listener");

    int beginPos = iText.lastIndexOf(iBegin);
    if (beginPos == -1) return iText;

    int endPos = iText.indexOf(iEnd, beginPos + 1);
    if (endPos == -1) return iText;

    String pre = iText.substring(0, beginPos);
    String var = iText.substring(beginPos + iBegin.length(), endPos);
    String post = iText.substring(endPos + iEnd.length());

    Object resolved = iListener.resolve(var);

    if (resolved == null) {
      if (iDefaultValue == null)
        OLogManager.instance()
            .info(null, "[OVariableParser.resolveVariables] Property not found: %s", var);
      else resolved = iDefaultValue;
    }

    if (pre.length() > 0 || post.length() > 0) {
      final String path = pre + (resolved != null ? resolved.toString() : "") + post;
      return resolveVariables(path, iBegin, iEnd, iListener);
    }

    return resolved;
  }
}
