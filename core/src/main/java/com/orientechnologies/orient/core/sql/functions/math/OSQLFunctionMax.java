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
package com.orientechnologies.orient.core.sql.functions.math;

import com.orientechnologies.common.types.ORef;

/**
 * Compute the maximum value for a field. Uses the context to save the last maximum number. When different Number class are used,
 * take the class with most precision.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionMax extends OSQLFunctionMathAbstract {
	public static final String	NAME	= "max";

	public OSQLFunctionMax() {
		super(NAME, 1, 1);
	}

	public Object execute(final ORef<Object> iContext, final Object[] iParameters) {
		Number value = (Number) iParameters[0];

		if (value != null && value instanceof Number) {
			if (iContext.value == null)
				// FIRST TIME
				iContext.value = value;
			else {
				Number contextValue = getContextValue(iContext, value.getClass());
				if (contextValue instanceof Integer) {
					if (((Integer) contextValue).compareTo((Integer) value) > 0)
						iContext.value = value;

				} else if (contextValue instanceof Long) {
					if (((Long) contextValue).compareTo((Long) value) > 0)
						iContext.value = value;

				} else if (contextValue instanceof Float) {
					if (((Float) contextValue).compareTo((Float) value) > 0)
						iContext.value = value;

				} else if (contextValue instanceof Double) {
					if (((Double) contextValue).compareTo((Double) value) > 0)
						iContext.value = value;
				}
			}
		}
		return null;
	}

	public String getSyntax() {
		return "Syntax error: distance(<field-x>,<field-y>,<x-value>,<y-value>[,<unit>])";
	}
}
