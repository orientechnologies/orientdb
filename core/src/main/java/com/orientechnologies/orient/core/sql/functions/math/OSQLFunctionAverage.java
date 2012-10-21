/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.List;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Compute the average value for a field. Uses the context to save the last average number. When different Number class are used,
 * take the class with most precision.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionAverage extends OSQLFunctionMathAbstract {
	public static final String	NAME	= "avg";

	private Number							sum;
	private int									total	= 0;

	public OSQLFunctionAverage() {
		super(NAME, 1, 1);
	}

	public Object execute(OIdentifiable iCurrentRecord, final Object[] iParameters, OCommandContext iContext) {
		Number value = (Number) iParameters[0];

		total++;

		if (value != null && value instanceof Number) {
			if (sum == null)
				// FIRST TIME
				sum = value;
			else
				sum = OType.increment(sum, value);
		}
		return null;
	}

	public String getSyntax() {
		return "Syntax error: avg(<field>)";
	}

	@Override
	public Object getResult() {
		if (returnDistributedResult()) {
			if (sum instanceof Integer)
				return "i" + sum.intValue() + "/" + total;
			else if (sum instanceof Long)
				return "l" + sum.longValue() + "/" + total;
			else if (sum instanceof Float)
				return "f" + sum.floatValue() + "/" + total;
			else if (sum instanceof Double)
				return "d" + sum.doubleValue() + "/" + total;
		} else {
			if (sum instanceof Integer)
				return sum.intValue() / total;
			else if (sum instanceof Long)
				return sum.longValue() / total;
			else if (sum instanceof Float)
				return sum.floatValue() / total;
			else if (sum instanceof Double)
				return sum.doubleValue() / total;
		}
		return null;
	}

	@Override
	public Object mergeDistributedResult(List<Object> resultsToMerge) {
		Number sum = null;
		long total = 0;
		for (Object iParameter : resultsToMerge) {
			final String str = (String) iParameter;
			final char kind = str.charAt(0);
			final String sumAsString = str.substring(1, str.indexOf('/'));
			final String totalAsString = str.substring(str.indexOf('/') + 1);

			switch (kind) {
			case 'i':
				if (sum == null) {
					sum = Integer.parseInt(sumAsString);
				} else {
					sum = sum.intValue() + Integer.parseInt(sumAsString);
				}
				break;
			case 'l':
				if (sum == null) {
					sum = Long.parseLong(sumAsString);
				} else {
					sum = sum.longValue() + Long.parseLong(sumAsString);
				}
				break;
			case 'f':
				if (sum == null) {
					sum = Float.parseFloat(sumAsString);
				} else {
					sum = sum.floatValue() + Float.parseFloat(sumAsString);
				}
				break;
			case 'd':
				if (sum == null) {
					sum = Double.parseDouble(sumAsString);
				} else {
					sum = sum.doubleValue() + Double.parseDouble(sumAsString);
				}
				break;
			}

			total += Long.parseLong(totalAsString);
		}

		if (sum instanceof Integer)
			return sum.intValue() / total;
		else if (sum instanceof Long)
			return sum.longValue() / total;
		else if (sum instanceof Float)
			return sum.floatValue() / total;
		else if (sum instanceof Double)
			return sum.doubleValue() / total;

		return null;
	}
}
