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
 * Compute the maximum value for a field. Uses the context to save the last maximum number. When different Number class are used,
 * take the class with most precision.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionMax extends OSQLFunctionMathAbstract {
	public static final String	NAME	= "max";

	private Comparable<Object>	context;

	public OSQLFunctionMax() {
		super(NAME, 1, 1);
	}

	@SuppressWarnings("unchecked")
	public Object execute(ORecord<?> iCurrentRecord, final Object[] iParameters, OCommandExecutor iRequester) {
		if (iParameters[0] == null || !(iParameters[0] instanceof Comparable<?>))
			// PRECONDITIONS
			return null;

		final Comparable<Object> value = (Comparable<Object>) iParameters[0];

		if (context == null)
			// FIRST TIME
			context = value;
		else if (context.compareTo(value) < 0)
			// BIGGER
			context = value;

		return value;
	}

	public boolean aggregateResults() {
		return true;
	}

	public String getSyntax() {
		return "Syntax error: max(<field>)";
	}

	@Override
	public Object getResult() {
		return context;
	}
}
