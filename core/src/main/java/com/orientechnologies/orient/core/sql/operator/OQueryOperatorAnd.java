/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;

/**
 * AND operator.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OQueryOperatorAnd extends OQueryOperator {

  public OQueryOperatorAnd() {
    super("AND", 4, false);
  }

  @Override
  public Object evaluateRecord(
      final OIdentifiable iRecord,
      ODocument iCurrentResult,
      final OSQLFilterCondition iCondition,
      final Object iLeft,
      final Object iRight,
      OCommandContext iContext,
      final ODocumentSerializer serializer) {
    if (iLeft == null) return false;
    return (Boolean) iLeft && (Boolean) iRight;
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    if (iLeft == null || iRight == null) return OIndexReuseType.NO_INDEX;
    return OIndexReuseType.INDEX_INTERSECTION;
  }

  @Override
  public ORID getBeginRidRange(final Object iLeft, final Object iRight) {
    final ORID leftRange;
    final ORID rightRange;

    if (iLeft instanceof OSQLFilterCondition)
      leftRange = ((OSQLFilterCondition) iLeft).getBeginRidRange();
    else leftRange = null;

    if (iRight instanceof OSQLFilterCondition)
      rightRange = ((OSQLFilterCondition) iRight).getBeginRidRange();
    else rightRange = null;

    if (leftRange == null && rightRange == null) return null;
    else if (leftRange == null) return rightRange;
    else if (rightRange == null) return leftRange;
    else return leftRange.compareTo(rightRange) <= 0 ? rightRange : leftRange;
  }

  @Override
  public ORID getEndRidRange(final Object iLeft, final Object iRight) {
    final ORID leftRange;
    final ORID rightRange;

    if (iLeft instanceof OSQLFilterCondition)
      leftRange = ((OSQLFilterCondition) iLeft).getEndRidRange();
    else leftRange = null;

    if (iRight instanceof OSQLFilterCondition)
      rightRange = ((OSQLFilterCondition) iRight).getEndRidRange();
    else rightRange = null;

    if (leftRange == null && rightRange == null) return null;
    else if (leftRange == null) return rightRange;
    else if (rightRange == null) return leftRange;
    else return leftRange.compareTo(rightRange) >= 0 ? rightRange : leftRange;
  }

  @Override
  public boolean canShortCircuit(Object l) {
    if (Boolean.FALSE.equals(l)) {
      return true;
    }
    return false;
  }
}
