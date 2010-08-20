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
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.query.OQueryRuntimeValueMulti;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

/**
 * TRAVERSE operator.
 * 
 * @author Luca Garulli
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

	@Override
	public String getSyntax() {
		return "<left> TRAVERSE[( <begin-deep-level> [, <maximum-deep-level>] )] ( <conditions> )";
	}

	@Override
	protected boolean evaluateExpression(final ORecordInternal<?> iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
			final Object iRight) {
		final OSQLFilterCondition condition;
		final Object target;

		if (iCondition.getLeft() instanceof OSQLFilterCondition) {
			condition = (OSQLFilterCondition) iCondition.getLeft();
			target = iRight;
		} else {
			condition = (OSQLFilterCondition) iCondition.getRight();
			target = iLeft;
		}

		return traverse(iRecord, condition, target, 0);
	}

	@SuppressWarnings("unchecked")
	private boolean traverse(final ORecordInternal<?> iRecord, final OSQLFilterCondition iCondition, Object iTarget, final int iLevel) {
		if (iTarget instanceof ORID)
			// TRANSFORM THE ORID IN ODOCUMENT
			iTarget = new ODocument(iRecord.getDatabase(), (ORID) iTarget);

		if (iTarget instanceof ODocument) {
			if (iLevel >= startDeepLevel && (Boolean) iCondition.evaluate((ODocument) iTarget) == Boolean.TRUE)
				return true;
		} else {
			if (iLevel >= endDeepLevel)
				return false;

			if (iTarget instanceof OQueryRuntimeValueMulti) {

				OQueryRuntimeValueMulti multi = (OQueryRuntimeValueMulti) iTarget;
				for (Object o : multi.values) {
					if (traverse(iRecord, iCondition, o, iLevel + 1) == Boolean.TRUE)
						return true;
				}
			} else if (iTarget instanceof Collection<?>) {

				Collection<ODocument> collection = (Collection<ODocument>) iTarget;
				for (ODocument o : collection) {
					if (traverse(iRecord, iCondition, o, iLevel + 1) == Boolean.TRUE)
						return true;
				}
			} else if (iTarget instanceof Map<?, ?>) {

				Map<String, ODocument> map = (Map<String, ODocument>) iTarget;
				for (ODocument o : map.values()) {
					if (traverse(iRecord, iCondition, o, iLevel + 1) == Boolean.TRUE)
						return true;
				}
			}
		}
		return false;
	}

	@Override
	public OQueryOperator configure(final List<String> iParams) {
		if (iParams == null)
			return this;

		final int start = iParams.size() > 0 ? Integer.parseInt(iParams.get(0)) : startDeepLevel;
		final int end = iParams.size() > 1 ? Integer.parseInt(iParams.get(1)) : endDeepLevel;

		return new OQueryOperatorTraverse(start, end);
	}

	public int getStartDeepLevel() {
		return startDeepLevel;
	}

	public int getEndDeepLevel() {
		return endDeepLevel;
	}
}
