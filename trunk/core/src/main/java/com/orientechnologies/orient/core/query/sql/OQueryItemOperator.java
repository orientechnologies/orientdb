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
package com.orientechnologies.orient.core.query.sql;

public enum OQueryItemOperator {
	EQUALS("=", 5, false), MINOR("<", 5, false), MINOR_EQUALS("<=", 5, false), MAJOR(">", 5, false), MAJOR_EQUALS(">=", 5, false), NOT_EQUALS(
			"<>", 5, false), LIKE("LIKE", 5, false), MATCHES("MATCHES", 5, false), AND("AND", 7, true), OR("OR", 6, true), NOT("NOT", 10,
			true), IS("IS", 5, false), IN("IN", 5, false), CONTAINS("CONTAINS", 5, false), CONTAINSALL("CONTAINSALL", 5, false);

	protected static final OQueryItemOperator[]	OPERATORS	= { AND, OR, NOT, EQUALS, MINOR, MAJOR_EQUALS, MAJOR, MINOR_EQUALS, LIKE,
			IS, IN, CONTAINS, CONTAINSALL										};

	public String																keyword;
	public int																	precedence;
	public boolean															logical;

	OQueryItemOperator(String iKeyword, int iPrecedence, boolean iLogical) {
		keyword = iKeyword;
		precedence = iPrecedence;
		logical = iLogical;
	}

	@Override
	public String toString() {
		return keyword;
	}
}
