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
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

/**
 * CONTAINS operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorContains extends OQueryOperatorEqualityNotNulls {

	public OQueryOperatorContains() {
		super("CONTAINS", 5, false);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected boolean evaluateExpression(final OIdentifiable iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
			final Object iRight, OCommandContext iContext) {
		final OSQLFilterCondition condition;
		if (iCondition.getLeft() instanceof OSQLFilterCondition)
			condition = (OSQLFilterCondition) iCondition.getLeft();
		else if (iCondition.getRight() instanceof OSQLFilterCondition)
			condition = (OSQLFilterCondition) iCondition.getRight();
		else
			condition = null;

		if (iLeft instanceof Iterable<?>) {

			final Iterable<Object> iterable = (Iterable<Object>) iLeft;

			if (condition != null) {
				// CHECK AGAINST A CONDITION
				for (final Object o : iterable) {
					if ((Boolean) condition.evaluate((OIdentifiable) o, iContext) == Boolean.TRUE)
						return true;
				}
			} else {
				// CHECK AGAINST A SINGLE VALUE
				for (final Object o : iterable) {
					if (OQueryOperatorEquals.equals(iRight, o))
						return true;
				}
			}
		} else if (iRight instanceof Iterable<?>) {

			// CHECK AGAINST A CONDITION
			final Iterable<OIdentifiable> iterable = (Iterable<OIdentifiable>) iRight;

			if (condition != null) {
				for (final OIdentifiable o : iterable) {
					if ((Boolean) condition.evaluate(o, iContext) == Boolean.TRUE)
						return true;
				}
			} else {
				// CHECK AGAINST A SINGLE VALUE
				for (final Object o : iterable) {
					if (OQueryOperatorEquals.equals(iLeft, o))
						return true;
				}
			}
		}
		return false;
	}

	@Override
	public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
		if (!(iLeft instanceof OSQLFilterCondition) && !(iRight instanceof OSQLFilterCondition))
			return OIndexReuseType.INDEX_METHOD;

		return OIndexReuseType.NO_INDEX;
	}

	@Override
	public ORID getBeginRidRange(Object iLeft, Object iRight) {
		return null;
	}

	@Override
	public ORID getEndRidRange(Object iLeft, Object iRight) {
		return null;
	}

}
