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

import java.util.Iterator;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;

/**
 * BETWEEN operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorBetween extends OQueryOperatorEqualityNotNulls {

	public OQueryOperatorBetween() {
		super("BETWEEN", 5, false, 3);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected boolean evaluateExpression(final ORecordInternal<?> iRecord, final OSQLFilterCondition iCondition, final Object iLeft,
			final Object iRight) {
    validate(iRight);

    final Iterator<?> valueIterator = OMultiValue.getMultiValueIterator(iRight);

		final Object right1 = OType.convert(valueIterator.next(), iLeft.getClass());
		if (right1 == null)
			return false;
		valueIterator.next();
		final Object right2 = OType.convert(valueIterator.next(), iLeft.getClass());
		if (right2 == null)
			return false;

		return ((Comparable<Object>) iLeft).compareTo(right1) >= 0 && ((Comparable<Object>) iLeft).compareTo(right2) <= 0;
	}

  private void validate(Object iRight) {
    if (!OMultiValue.isMultiValue(iRight.getClass())) {
      throw new IllegalArgumentException("Found '" + iRight + "' while was expected: " + getSyntax());
    }

    if (OMultiValue.getSize(iRight) != 3)
      throw new IllegalArgumentException("Found '" + OMultiValue.toString(iRight) + "' while was expected: " + getSyntax());
  }

  @Override
	public String getSyntax() {
		return "<left> " + keyword + " <minRange> AND <maxRange>";
	}

	@Override
	public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
		return OIndexReuseType.INDEX_METHOD;
	}

  @Override
  public ORID getBeginRidRange(final Object iLeft, final Object iRight) {
    validate(iRight);

    if (iLeft instanceof OSQLFilterItemField &&
            ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot())) {
      final Iterator<?> valueIterator = OMultiValue.getMultiValueIterator(iRight);

      final Object right1 = valueIterator.next();
      if (right1 != null)
        return (ORID)right1;

      valueIterator.next();

      return (ORID)valueIterator.next();
    }

    return null;
  }

  @Override
  public ORID getEndRidRange(final Object iLeft,final Object iRight) {
    validate(iRight);

    validate(iRight);

    if(iLeft instanceof OSQLFilterItemField &&
            ODocumentHelper.ATTRIBUTE_RID.equals(((OSQLFilterItemField) iLeft).getRoot())) {
      final Iterator<?> valueIterator = OMultiValue.getMultiValueIterator(iRight);

      final Object right1 = valueIterator.next();

      valueIterator.next();

      final Object right2 = valueIterator.next();

      if(right2 == null)
        return (ORID)right1;

      return (ORID)right2;
    }

    return null;
  }
}
