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
package com.orientechnologies.orient.core.sql.operator.math;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.operator.OIndexReuseType;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;

/**
 * PLUS "+" operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorPlus extends OQueryOperator {

	public OQueryOperatorPlus() {
		super("+", 9, false);
	}

	@Override
	public Object evaluateRecord(final ORecordInternal<?> iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
			final Object iRight) {
		if (iRight == null)
			return iLeft;
		if (iLeft == null)
			return iRight;

		if (iLeft instanceof String)
			return (String) iLeft + iRight.toString();
		else if (iRight instanceof String)
			return iLeft.toString() + (String) iRight;
		else if (iLeft instanceof Number && iRight instanceof Number) {
			final Number l = (Number) iLeft;
			final Number r = (Number) iRight;
			if (l instanceof Integer)
				return l.intValue() + r.intValue();
			else if (l instanceof Long)
				return l.longValue() + r.longValue();
			else if (l instanceof Short)
				return l.shortValue() + r.shortValue();
			else if (l instanceof Float)
				return l.floatValue() + r.floatValue();
			else if (l instanceof Double)
				return l.doubleValue() + r.doubleValue();
		}

		return null;
	}

    @Override
  public OIndexReuseType getIndexReuseType(Object iLeft, Object iRight) {
      if (iLeft instanceof Number && iRight instanceof Number)
          return OIndexReuseType.INDEX_KEY;
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
