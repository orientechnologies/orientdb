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

import java.util.List;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

/**
 * Query Operators. Remember to handle the operator in OQueryItemCondition.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OQueryOperator {
	public final String	keyword;
	public final int		precedence;
	public final int		expectedRightWords;

	protected OQueryOperator(final String iKeyword, final int iPrecedence, final boolean iLogical) {
		keyword = iKeyword;
		precedence = iPrecedence;
		expectedRightWords = 1;
	}

	protected OQueryOperator(final String iKeyword, final int iPrecedence, final boolean iLogical, final int iExpectedRightWords) {
		keyword = iKeyword;
		precedence = iPrecedence;
		expectedRightWords = iExpectedRightWords;
	}

	public abstract Object evaluateRecord(final ORecordInternal<?> iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
			final Object iRight);

	public abstract OIndexReuseType getIndexReuseType(Object iLeft, Object iRight);

	@Override
	public String toString() {
		return keyword;
	}

	/**
	 * Default State-less implementation: does not save parameters and just return itself
	 * 
	 * @param iParams
	 * @return
	 */
	public OQueryOperator configure(final List<String> iParams) {
		return this;
	}

	public String getSyntax() {
		return "<left> " + keyword + " <right>";
  }

  public abstract ORID getBeginRidRange(final Object iLeft, final Object iRight);

  public abstract ORID getEndRidRange(final Object iLeft, final Object iRight);
}
