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

/**
 * IN operator.
 * 
 * @author luca
 * 
 */
public class OQueryOperatorIn extends OQueryOperatorEqualityNotNulls {

	public OQueryOperatorIn() {
		super("IN", 5, false);
	}

	@SuppressWarnings("unchecked")
	protected boolean evaluateExpression(OSQLCondition iCondition, final Object iLeft, final Object iRight) {
		if (iLeft instanceof Collection<?>) {
			Collection<Object> collection = (Collection<Object>) iLeft;
			for (Object o : collection) {
				if (iRight.equals(o))
					return true;
			}
		} else if (iRight instanceof Collection<?>) {

			Collection<Object> collection = (Collection<Object>) iRight;
			for (Object o : collection) {
				if (iLeft.equals(o))
					return true;
			}
		}
		return false;
	}

}
