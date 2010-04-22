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
package com.orientechnologies.orient.core.query.operator;

import com.orientechnologies.orient.core.query.sql.OSQLValueAll;
import com.orientechnologies.orient.core.query.sql.OSQLCondition;
import com.orientechnologies.orient.core.query.sql.OSQLValueAny;

/**
 * Base equality operator. It's an abstract class able to compare the equality between two values.
 * 
 * @author luca
 * 
 */
public abstract class OQueryOperatorEquality extends OQueryOperator {

	protected OQueryOperatorEquality(String iKeyword, int iPrecedence, boolean iLogical) {
		super(iKeyword, iPrecedence, iLogical);
	}

	protected abstract boolean evaluateExpression(final OSQLCondition iCondition, final Object iLeft, final Object iRight);

	public boolean evaluate(final OSQLCondition iCondition, final Object iLeft, final Object iRight) {
		if (iLeft instanceof OSQLValueAll) {
			// ALL VALUES
			final OSQLValueAll allValues = (OSQLValueAll) iLeft;
			if (allValues.values.length == 0)
				return false;

			for (Object v : allValues.values)
				if (v == null || !evaluateExpression(iCondition, v, iRight))
					return false;
			return true;
		} else if (iRight instanceof OSQLValueAll) {
			// ALL VALUES
			final OSQLValueAll allValues = (OSQLValueAll) iRight;
			if (allValues.values.length == 0)
				return false;

			for (Object v : allValues.values)
				if (v == null || !evaluateExpression(iCondition, v, iLeft))
					return false;
			return true;

		} else if (iLeft instanceof OSQLValueAny) {
			// ANY VALUES
			final OSQLValueAny anyValue = (OSQLValueAny) iLeft;
			if (anyValue.values.length == 0)
				return false;

			for (Object v : anyValue.values)
				if (v != null && evaluateExpression(iCondition, iRight, v))
					return true;
			return false;
		} else if (iRight instanceof OSQLValueAny) {
			// ANY VALUES
			final OSQLValueAny anyValue = (OSQLValueAny) iRight;
			if (anyValue.values.length == 0)
				return false;

			for (Object v : anyValue.values)
				if (v != null && evaluateExpression(iCondition, iLeft, v))
					return true;
			return false;

		} else
			// SINGLE SIMPLE ITEM
			return evaluateExpression(iCondition, iLeft, iRight);
	}

}
