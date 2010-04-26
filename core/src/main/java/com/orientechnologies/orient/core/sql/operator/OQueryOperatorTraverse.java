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

import java.util.Collection;

import com.orientechnologies.orient.core.query.OQueryRuntimeValueMulti;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

/**
 * TRAVERSE operator.
 * 
 * @author luca
 * 
 */
public class OQueryOperatorTraverse extends OQueryOperatorEqualityNotNulls {
	private int	startDeepLevel	= 0;	// FIRST
	private int	endDeepLevel		= -1; // INFINITE

	public OQueryOperatorTraverse() {
		super("TRAVERSE", 5, false);
	}

	public OQueryOperatorTraverse(final int startDeepLevel, final int endDeepLevel) {
		this();
		this.startDeepLevel = startDeepLevel;
		this.endDeepLevel = endDeepLevel;
	}

	protected boolean evaluateExpression(final OSQLFilterCondition iCondition, final Object iLeft, final Object iRight) {
		final OSQLFilterCondition condition;
		final Object target;

		if (iCondition.getLeft() instanceof OSQLFilterCondition) {
			condition = (OSQLFilterCondition) iCondition.getLeft();
			target = iRight;
		} else {
			condition = (OSQLFilterCondition) iCondition.getRight();
			target = iLeft;
		}

		return traverse(condition, target, 0);
	}

	@SuppressWarnings("unchecked")
	private boolean traverse(final OSQLFilterCondition condition, final Object target, final int iLevel) {
		if (target instanceof ODocument) {
			if (iLevel >= startDeepLevel && (Boolean) condition.evaluate((ODocument) target) == Boolean.TRUE)
				return true;

		} else {
			if (iLevel >= endDeepLevel)
				return false;

			if (target instanceof OQueryRuntimeValueMulti) {

				OQueryRuntimeValueMulti multi = (OQueryRuntimeValueMulti) target;
				for (Object o : multi.values) {
					if (traverse(condition, o, iLevel + 1) == Boolean.TRUE)
						return true;
				}
			} else if (target instanceof Collection<?>) {

				Collection<ODocument> collection = (Collection<ODocument>) target;
				for (ODocument o : collection) {
					if (traverse(condition, o, iLevel + 1) == Boolean.TRUE)
						return true;
				}
			}
		}
		return false;
	}

	@Override
	public OQueryOperator configure(String[] params) {
		if (params == null)
			return this;

		final int start = params.length > 0 ? Integer.parseInt(params[0]) : startDeepLevel;
		final int end = params.length > 1 ? Integer.parseInt(params[1]) : endDeepLevel;

		return new OQueryOperatorTraverse(start, end);
	}

	public int getStartDeepLevel() {
		return startDeepLevel;
	}

	public int getEndDeepLevel() {
		return endDeepLevel;
	}
}
