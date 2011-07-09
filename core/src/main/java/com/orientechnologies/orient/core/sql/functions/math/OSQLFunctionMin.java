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

import com.orientechnologies.orient.core.record.ORecord;

/**
 * Compute the minimum value for a field. Uses the context to save the last minimum number. When different Number class are used,
 * take the class with most precision.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionMin extends OSQLFunctionMathAbstract {
	public static final String	NAME	= "min";

	private Number							context;

	public OSQLFunctionMin() {
		super(NAME, 1, 1);
	}

	public Object execute(ORecord<?> iCurrentRecord, final Object[] iParameters) {
		Number value = (Number) iParameters[0];

		if (value != null && value instanceof Number) {
			if (context == null)
				// FIRST TIME
				context = value;
			else {
				Number contextValue = getContextValue(context, value.getClass());
				if (contextValue instanceof Integer) {
					if (((Integer) contextValue).compareTo((Integer) value) > 0)
						context = value;

				} else if (contextValue instanceof Long) {
					if (((Long) contextValue).compareTo((Long) value) > 0)
						context = value;

				} else if (contextValue instanceof Short) {
					if (((Short) contextValue).compareTo((Short) value) > 0)
						context = value;

				} else if (contextValue instanceof Float) {
					if (((Float) contextValue).compareTo((Float) value) > 0)
						context = value;

				} else if (contextValue instanceof Double) {
					if (((Double) contextValue).compareTo((Double) value) > 0)
						context = value;
				}
			}
		}
		return value;
	}

	public boolean aggregateResults() {
		return true;
	}

	public String getSyntax() {
		return "Syntax error: min(<field>)";
	}

	@Override
	public Object getResult() {
		return context;
	}
}
