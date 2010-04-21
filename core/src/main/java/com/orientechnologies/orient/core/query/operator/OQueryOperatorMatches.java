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

import com.orientechnologies.orient.core.query.sql.OSQLCondition;

/**
 * MATCHES operator. Matches the left value against the regular expression contained in the second one.
 * 
 * @author luca
 * 
 */
public class OQueryOperatorMatches extends OQueryOperatorEqualityNotNulls {

	public OQueryOperatorMatches() {
		super("MATCHES", 5, false);
	}

	protected boolean evaluateExpression(OSQLCondition iCondition, final Object iLeft, final Object iRight) {
		return iLeft.toString().matches((String) iRight);
	}
}
