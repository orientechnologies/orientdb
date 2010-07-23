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

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

public class OQueryHelper {
	protected static final String			WILDCARD_ANYCHAR			= "?";
	protected static final String			WILDCARD_ANY					= "%";

	public static final String				OPEN_BRACE						= "(";
	public static final String				CLOSED_BRACE					= ")";
	public static final String				OPEN_COLLECTION				= "[";
	public static final String				CLOSED_COLLECTION			= "]";
	public static final char					COLLECTION_SEPARATOR	= ',';
	public static final char					PARAMETER_SEPARATOR		= ',';
	public static final List<String>	EMPTY_PARAMETERS			= new ArrayList<String>();

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

	public static List<String> getParameters(final String iText) {
		return getParameters(iText, 0);
	}

	public static List<String> getParameters(final String iText, final int iBeginPosition) {
		int openPos = iText.indexOf(OPEN_BRACE, iBeginPosition);
		if (openPos == -1)
			return EMPTY_PARAMETERS;

		int closePos = iText.indexOf(CLOSED_BRACE, openPos + 1);
		if (closePos == -1)
			return EMPTY_PARAMETERS;

		if (closePos - openPos == 1)
			// EMPTY STRING: TREATS AS EMPTY
			return EMPTY_PARAMETERS;

		final List<String> pars = OStringSerializerHelper.split(iText.substring(openPos + 1, closePos), PARAMETER_SEPARATOR, ' ');

		// REMOVE TAIL AND END SPACES
		for (int i = 0; i < pars.size(); ++i)
			pars.set(i, pars.get(i));

		return pars;
	}

	public static List<String> getCollection(final String iText) {
		int openPos = iText.indexOf(OPEN_COLLECTION);
		if (openPos == -1)
			return EMPTY_PARAMETERS;

		int closePos = iText.indexOf(CLOSED_COLLECTION, openPos + 1);
		if (closePos == -1)
			return EMPTY_PARAMETERS;

		return OStringSerializerHelper.split(iText.substring(openPos + 1, closePos), COLLECTION_SEPARATOR, ' ');
	}
}
