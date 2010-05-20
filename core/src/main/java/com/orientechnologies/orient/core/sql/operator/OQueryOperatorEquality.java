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
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.orient.core.query.OQueryRuntimeValueMulti;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemFieldAll;

/**
 * Base equality operator. It's an abstract class able to compare the equality between two values.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OQueryOperatorEquality extends OQueryOperator {

	protected OQueryOperatorEquality(String iKeyword, int iPrecedence, boolean iLogical) {
		super(iKeyword, iPrecedence, iLogical);
	}

	protected abstract boolean evaluateExpression(final OSQLFilterCondition iCondition, final Object iLeft, final Object iRight);

	@Override
	public boolean evaluate(final OSQLFilterCondition iCondition, final Object iLeft, final Object iRight) {
		if (iLeft instanceof OQueryRuntimeValueMulti) {
			// LEFT = MULTI
			OQueryRuntimeValueMulti left = (OQueryRuntimeValueMulti) iLeft;

			if (left.values.length == 0)
				return false;

			if (left.getDefinition().getName().equals(OSQLFilterItemFieldAll.NAME)) {
				// ALL VALUES
				for (Object v : left.values)
					if (v == null || !evaluateExpression(iCondition, v, iRight))
						return false;
				return true;
			} else {
				// ANY VALUES
				for (Object v : left.values)
					if (v != null && evaluateExpression(iCondition, v, iRight))
						return true;
				return false;
			}

		} else if (iRight instanceof OQueryRuntimeValueMulti) {
			// RIGHT = MULTI
			OQueryRuntimeValueMulti right = (OQueryRuntimeValueMulti) iRight;

			if (right.values.length == 0)
				return false;

			if (right.getDefinition().getName().equals(OSQLFilterItemFieldAll.NAME)) {
				// ALL VALUES
				for (Object v : right.values)
					if (v == null || !evaluateExpression(iCondition, iLeft, v))
						return false;
				return true;
			} else {
				// ANY VALUES
				for (Object v : right.values)
					if (v != null && evaluateExpression(iCondition, iLeft, v))
						return true;
				return false;
			}
		} else
			// SINGLE SIMPLE ITEM
			return evaluateExpression(iCondition, iLeft, iRight);
	}
}
