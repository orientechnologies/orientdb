/*
 * Copyright 2006 Luca Garulli (luca.garulli--at--assetdata.it)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.common.parser;

import com.orientechnologies.common.log.OLogManager;

/**
 * Resolve entity class and descriptors using the paths configured.
 * 
 * @author Luca Garulli (luca.garulli--at--assetdata.it)
 */
public class OVariableParser {
	public static String resolveVariables(String iText, String iBegin, String iEnd, OVariableParserListener iListener) {
		if (iListener == null)
			throw new IllegalArgumentException("Missed VariableParserListener listener");

		int beginPos = iText.lastIndexOf(iBegin);
		if (beginPos == -1)
			return iText;

		int endPos = iText.indexOf(iEnd, beginPos + 1);
		if (endPos == -1)
			return iText;

		String pre = iText.substring(0, beginPos);
		String var = iText.substring(beginPos + iBegin.length(), endPos);
		String post = iText.substring(endPos + iEnd.length());

		String resolved = iListener.resolve(var);

		if (resolved == null) {
			OLogManager.instance().error(null, "[OVariableParser.resolveVariables] Error on resolving property: %s", var);
			resolved = "null";
		}

		String path = pre + resolved + post;

		return resolveVariables(path, iBegin, iEnd, iListener);
	}
}
