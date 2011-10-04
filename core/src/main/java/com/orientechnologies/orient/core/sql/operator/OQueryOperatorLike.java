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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.query.OQueryHelper;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

/**
 * LIKE operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorLike extends OQueryOperatorEqualityNotNulls {

	public OQueryOperatorLike() {
		super("LIKE", 5, false);
	}

	@Override
	protected boolean evaluateExpression(final ORecordInternal<?> iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
			final Object iRight) {
		if (OMultiValue.isMultiValue(iLeft) || OMultiValue.isMultiValue(iRight))
			return false;

		return OQueryHelper.like(iLeft.toString(), iRight.toString());
	}

	@Override
	public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
		return OIndexReuseType.NO_INDEX;
	}
}
