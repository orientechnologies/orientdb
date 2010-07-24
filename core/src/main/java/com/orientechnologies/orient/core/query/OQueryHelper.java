/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.query;

public class OQueryHelper {
	protected static final String	WILDCARD_ANYCHAR	= "?";
	protected static final String	WILDCARD_ANY			= "%";

	public static boolean like(String currentValue, String iValue) {
		if (currentValue == null || currentValue.length() == 0)
			// EMPTY FIELD
			return false;

		int anyPos = iValue.indexOf(WILDCARD_ANY);
		int charAnyPos = iValue.indexOf(WILDCARD_ANYCHAR);

		if (anyPos == -1 && charAnyPos == -1)
			// NO WILDCARDS: DO EQUALS
			return currentValue.equals(iValue);

		String value = currentValue.toString();
		if (value == null || value.length() == 0)
			// NOTHING TO MATCH
			return false;

		if (iValue.startsWith(WILDCARD_ANY) && iValue.endsWith(WILDCARD_ANY)) {
			// %XXXXX%
			iValue = iValue.substring(WILDCARD_ANY.length(), iValue.length() - WILDCARD_ANY.length());
			return currentValue.indexOf(iValue) > -1;

		} else if (iValue.startsWith(WILDCARD_ANY)) {
			// %XXXXX
			iValue = iValue.substring(WILDCARD_ANY.length());
			return value.endsWith(iValue);

		} else if (iValue.endsWith(WILDCARD_ANY)) {
			// XXXXX%
			iValue = iValue.substring(0, iValue.length() - WILDCARD_ANY.length());
			return value.startsWith(iValue);
		}

		return false;
	}
}
