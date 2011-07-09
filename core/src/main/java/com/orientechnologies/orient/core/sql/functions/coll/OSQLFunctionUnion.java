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
package com.orientechnologies.orient.core.sql.functions.coll;

import java.util.Collection;
import java.util.HashSet;

import com.orientechnologies.orient.core.record.ORecord;

/**
 * This operator can work as aggregate or inline. If only one argument is passed than aggregates, otherwise executes, and returns, a
 * UNION of the collections received as parameters. Works also with no collection values.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionUnion extends OSQLFunctionCollAbstract {
	public static final String	NAME	= "union";

	public OSQLFunctionUnion() {
		super(NAME, 1, -1);
	}

	public Object execute(ORecord<?> iCurrentRecord, final Object[] iParameters) {
		if (iParameters.length == 1) {
			// AGGREGATION MODE (STATEFULL)
			final Object value = iParameters[0];
			if (value != null) {
				if (context == null)
					context = new HashSet<Object>();

				if (value instanceof Collection<?>)
					// INSERT EVERY SINGLE COLLECTION ITEM
					context.addAll((Collection<?>) value);
				else
					context.add(value);
			}

			return null;
		} else {
			// IN-LINE MODE (STATELESS)
			final HashSet<Object> result = new HashSet<Object>();
			for (Object value : iParameters) {
				if (value != null)
					if (value instanceof Collection<?>)
						// INSERT EVERY SINGLE COLLECTION ITEM
						result.addAll((Collection<?>) value);
					else
						result.add(value);
			}

			return result;
		}
	}

	public String getSyntax() {
		return "Syntax error: union(<field>*)";
	}
}
