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

import java.util.Collection;

import com.orientechnologies.orient.core.query.sql.OSQLCondition;
import com.orientechnologies.orient.core.query.sql.OSQLValueMultiAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * TRAVERSE operator.
 * 
 * @author luca
 * 
 */
public class OQueryOperatorTraverse extends OQueryOperatorEqualityNotNulls {

	public OQueryOperatorTraverse() {
		super("TRAVERSE", 5, false);
	}

	protected boolean evaluateExpression(OSQLCondition iCondition, final Object iLeft, final Object iRight) {
		final OSQLCondition condition;
		final Object target;

		if (iCondition.getLeft() instanceof OSQLCondition) {
			condition = (OSQLCondition) iCondition.getLeft();
			target = iRight;
		} else {
			condition = (OSQLCondition) iCondition.getRight();
			target = iRight;
		}

		return traverse(condition, target);
	}

	@SuppressWarnings("unchecked")
	private boolean traverse(final OSQLCondition condition, final Object target) {
		if (target instanceof ODocument) {
			if ((Boolean) condition.evaluate((ODocument) target) == Boolean.TRUE)
				return true;

		} else if (target instanceof OSQLValueMultiAbstract) {

			OSQLValueMultiAbstract multi = (OSQLValueMultiAbstract) target;
			for (Object o : multi.values) {
				if (traverse(condition, o) == Boolean.TRUE)
					return true;
			}
		} else if (target instanceof Collection<?>) {

			Collection<ODocument> collection = (Collection<ODocument>) target;
			for (ODocument o : collection) {
				if (traverse(condition, o) == Boolean.TRUE)
					return true;
			}
		}
		return false;
	}
}
