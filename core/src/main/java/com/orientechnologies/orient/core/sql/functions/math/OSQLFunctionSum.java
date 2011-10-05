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

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Compute the averahe value for a field. Uses the context to save the last average number. When different Number class are used,
 * take the class with most precision.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionSum extends OSQLFunctionMathAbstract {
	public static final String	NAME	= "sum";

	private Number							sum;

	public OSQLFunctionSum() {
		super(NAME, 1, 1);
	}

	public Object execute(ORecord<?> iCurrentRecord, final Object[] iParameters, OCommandExecutor iRequester) {
		Number value = (Number) iParameters[0];

		if (value != null && value instanceof Number) {
			if (sum == null)
				// FIRST TIME
				sum = value;
			else {
				Number contextValue = getContextValue(sum, value.getClass());
				if (contextValue instanceof Integer) {
					sum = sum.intValue() + value.intValue();

				} else if (contextValue instanceof Long) {
					sum = sum.longValue() + value.longValue();

				} else if (contextValue instanceof Short) {
					sum = sum.shortValue() + value.shortValue();

				} else if (contextValue instanceof Float) {
					sum = sum.floatValue() + value.floatValue();

				} else if (contextValue instanceof Double) {
					sum = sum.doubleValue() + value.doubleValue();
				}
			}
		}
		return value;
	}

	public boolean aggregateResults() {
		return true;
	}

	public String getSyntax() {
		return "Syntax error: sum(<field>)";
	}

	@Override
	public Object getResult() {
		return sum;
	}
}
