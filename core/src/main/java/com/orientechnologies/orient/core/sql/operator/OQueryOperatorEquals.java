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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

/**
 * EQUALS operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorEquals extends OQueryOperatorEqualityNotNulls {

	public OQueryOperatorEquals() {
		super("=", 5, false);
	}

	@Override
	protected boolean evaluateExpression(final ORecordInternal<?> iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
			final Object iRight) {
		return equals(iLeft, iRight);
	}

	public static boolean equals(final Object iLeft, final Object iRight) {
		if (iLeft instanceof ORecord<?> && iRight instanceof ORID)
			// RECORD & ORID
			return ((ORecord<?>) iLeft).getIdentity().equals(iRight);
		else if (iRight instanceof ORecord<?> && iLeft instanceof ORID)
			// ORID && RECORD
			return ((ORecord<?>) iRight).getIdentity().equals(iLeft);
		else {
			// ALL OTHER CASES
			final Object right = OType.convert(iRight, iLeft.getClass());
			if (right == null)
				return false;
			return iLeft.equals(right);
		}
	}

	@Override
	public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
		if (iLeft instanceof OIdentifiable && iRight instanceof OIdentifiable)
			return OIndexReuseType.NO_INDEX;
		if (iRight == null || iLeft == null)
			return OIndexReuseType.NO_INDEX;

		return OIndexReuseType.INDEX_METHOD;
	}

}
