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
 * NOT operator.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OQueryOperatorNot extends OQueryOperator {
  private OQueryOperator next;

  public OQueryOperatorNot() {
    super("NOT", 10, true);
    next = null;
  }

  public OQueryOperatorNot(final OQueryOperator iNext) {
    this();
    next = iNext;
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
    if (next != null)
      return !(Boolean)
          next.evaluateRecord(iRecord, null, iCondition, iLeft, iRight, iContext, serializer);

    if (iLeft == null) return false;
    return !(Boolean) iLeft;
  }

  @Override
  public OIndexReuseType getIndexReuseType(final Object iLeft, final Object iRight) {
    return OIndexReuseType.NO_INDEX;
  }

  @Override
  public ORID getBeginRidRange(Object iLeft, Object iRight) {
    if (iLeft instanceof OSQLFilterCondition) {
      final ORID beginRange = ((OSQLFilterCondition) iLeft).getBeginRidRange();
      final ORID endRange = ((OSQLFilterCondition) iLeft).getEndRidRange();

      if (beginRange == null && endRange == null) return null;
      else if (beginRange == null) return endRange;
      else if (endRange == null) return null;
      else return null;
    }

    return null;
  }

  @Override
  public ORID getEndRidRange(Object iLeft, Object iRight) {
    if (iLeft instanceof OSQLFilterCondition) {
      final ORID beginRange = ((OSQLFilterCondition) iLeft).getBeginRidRange();
      final ORID endRange = ((OSQLFilterCondition) iLeft).getEndRidRange();

      if (beginRange == null && endRange == null) return null;
      else if (beginRange == null) return null;
      else if (endRange == null) return beginRange;
      else return null;
    }

    return null;
  }

  public OQueryOperator getNext() {
    return next;
  }
}
