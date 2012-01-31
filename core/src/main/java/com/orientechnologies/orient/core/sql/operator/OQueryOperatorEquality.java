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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
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

	protected OQueryOperatorEquality(final String iKeyword, final int iPrecedence, final boolean iLogical) {
		super(iKeyword, iPrecedence, false);
	}

	protected OQueryOperatorEquality(final String iKeyword, final int iPrecedence, final boolean iLogical,
			final int iExpectedRightWords) {
		super(iKeyword, iPrecedence, false, iExpectedRightWords);
	}

	protected abstract boolean evaluateExpression(final OIdentifiable iRecord, final OSQLFilterCondition iCondition,
			final Object iLeft, final Object iRight, OCommandContext iContext);

	@Override
	public Object evaluateRecord(final OIdentifiable iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
			final Object iRight, OCommandContext iContext) {
		if (iLeft instanceof OQueryRuntimeValueMulti) {
			// LEFT = MULTI
			final OQueryRuntimeValueMulti left = (OQueryRuntimeValueMulti) iLeft;

			if (left.values.length == 0)
				return false;

			if (left.getDefinition().getRoot().equals(OSQLFilterItemFieldAll.NAME)) {
				// ALL VALUES
				for (final Object v : left.values)
					if (v == null || !evaluateExpression(iRecord, iCondition, v, iRight, iContext))
						return false;
				return true;
			} else {
				// ANY VALUES
				for (final Object v : left.values)
					if (v != null && evaluateExpression(iRecord, iCondition, v, iRight, iContext))
						return true;
				return false;
			}

		} else if (iRight instanceof OQueryRuntimeValueMulti) {
			// RIGHT = MULTI
			final OQueryRuntimeValueMulti right = (OQueryRuntimeValueMulti) iRight;

			if (right.values.length == 0)
				return false;

			if (right.getDefinition().getRoot().equals(OSQLFilterItemFieldAll.NAME)) {
				// ALL VALUES
				for (final Object v : right.values)
					if (v == null || !evaluateExpression(iRecord, iCondition, iLeft, v, iContext))
						return false;
				return true;
			} else {
				// ANY VALUES
				for (final Object v : right.values)
					if (v != null && evaluateExpression(iRecord, iCondition, iLeft, v, iContext))
						return true;
				return false;
			}
		} else
			// SINGLE SIMPLE ITEM
			return evaluateExpression(iRecord, iCondition, iLeft, iRight, iContext);
	}
}
