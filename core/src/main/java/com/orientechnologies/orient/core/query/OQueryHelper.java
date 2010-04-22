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
	protected static final String	WILDCARD_ANYCHAR			= "?";
	protected static final String	WILDCARD_ANY					= "%";

	public static final String		OPEN_BRACE						= "(";
	public static final String		CLOSED_BRACE					= ")";
	public static final String		OPEN_COLLECTION				= "[";
	public static final String		CLOSED_COLLECTION			= "]";
	public static final String		COLLECTION_SEPARATOR	= ",";
	public static final String		PARAMETER_SEPARATOR		= ",";
	public static final String[]	EMPTY_PARAMETERS			= new String[] {};

	public static boolean like(String currentValue, String iValue) {
		int anyPos = iValue.indexOf(WILDCARD_ANY);
		int charAnyPos = iValue.indexOf(WILDCARD_ANYCHAR);

		if (anyPos == -1 && charAnyPos == -1)
			// NO WILDCARDS: DO EQUALS
			return currentValue.equals(iValue);

		String value = currentValue.toString();
		if (value == null || value.length() == 0)
			return false;

		if (iValue.startsWith(WILDCARD_ANY)) {
			iValue = iValue.substring(WILDCARD_ANY.length());
			if (!value.endsWith(iValue))
				return false;
		}

		if (iValue.endsWith(WILDCARD_ANY)) {
			iValue = iValue.substring(0, iValue.length() - WILDCARD_ANY.length());
			return value.startsWith(iValue);
		} else
			return true;
	}

	public static String[] getParameters(final String iText) {
		int openPos = iText.indexOf(OPEN_BRACE);
		if (openPos == -1)
			return EMPTY_PARAMETERS;

		int closePos = iText.indexOf(CLOSED_BRACE, openPos + 1);
		if (closePos == -1)
			return EMPTY_PARAMETERS;

		if (closePos - openPos == 1)
			// EMPTY STRING: TREATS AS EMPTY
			return EMPTY_PARAMETERS;

		String[] pars = iText.substring(openPos + 1, closePos).split(PARAMETER_SEPARATOR);

		// REMOVE TAIL AND END SPACES
		for (int i = 0; i < pars.length; ++i)
			pars[i] = pars[i].trim();

		return pars;
	}

	public static Object getCollection(final String iText) {
		int openPos = iText.indexOf(OPEN_COLLECTION);
		if (openPos == -1)
			return EMPTY_PARAMETERS;

		int closePos = iText.indexOf(CLOSED_COLLECTION, openPos + 1);
		if (closePos == -1)
			return EMPTY_PARAMETERS;

		// TODO BY IMPROVING IT CONSIDERING COMMAS INSIDE STRINGS!
		return iText.substring(openPos + 1, closePos).split(COLLECTION_SEPARATOR);
	}
}
