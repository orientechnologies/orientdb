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

/**
 * Query Operators. Remember to handle the operator in OQueryItemCondition.
 * 
 * @author luca
 * 
 */
public abstract class OQueryOperator {
	// protected static final OQueryItemOperator[] OPERATORS = { AND, OR, NOT, EQUALS, MINOR, MAJOR_EQUALS, MAJOR, MINOR_EQUALS, LIKE,
	// IS, IN, CONTAINS, CONTAINSALL, TRAVERSE };

	public String		keyword;
	public int			precedence;
	public boolean	logical;

	protected OQueryOperator(String iKeyword, int iPrecedence, boolean iLogical) {
		keyword = iKeyword;
		precedence = iPrecedence;
		logical = iLogical;
	}

	public abstract boolean evaluate(final Object iLeft, final Object iRight);

	@Override
	public String toString() {
		return keyword;
	}
}
