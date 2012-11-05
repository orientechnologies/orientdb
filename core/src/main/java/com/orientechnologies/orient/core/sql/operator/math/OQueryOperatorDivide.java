/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.operator.OIndexReuseType;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;

/**
 * DIVIDE "/" operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OQueryOperatorDivide extends OQueryOperator {

  public OQueryOperatorDivide() {
    super("/", 10, false);
  }

  @Override
  public Object evaluateRecord(final OIdentifiable iRecord, ODocument iCurrentResult, final OSQLFilterCondition iCondition,
      Object iLeft, Object iRight, OCommandContext iContext) {
    if (iRight == null || iLeft == null)
      return null;

    if (iLeft instanceof Date)
      iLeft = ((Date) iLeft).getTime();
    if (iRight instanceof Date)
      iRight = ((Date) iRight).getTime();

    if (iLeft instanceof Number && iRight instanceof Number) {
      final Number l = (Number) iLeft;
      final Number r = (Number) iRight;
      if (l instanceof Integer)
        return l.intValue() / r.intValue();
      else if (l instanceof Long)
        return l.longValue() / r.longValue();
      else if (l instanceof Short)
        return l.shortValue() / r.shortValue();
      else if (l instanceof Float)
        return l.floatValue() / r.floatValue();
      else if (l instanceof Double)
        return l.doubleValue() / r.doubleValue();
      else if (l instanceof BigDecimal) {
        if (r instanceof BigDecimal)
          return ((BigDecimal) l).divide((BigDecimal) r, RoundingMode.HALF_UP);
        else if (r instanceof Float)
          return ((BigDecimal) l).divide(new BigDecimal(r.floatValue()), RoundingMode.HALF_UP);
        else if (r instanceof Double)
          return ((BigDecimal) l).divide(new BigDecimal(r.doubleValue()), RoundingMode.HALF_UP);
        else if (r instanceof Long)
          return ((BigDecimal) l).divide(new BigDecimal(r.longValue()), RoundingMode.HALF_UP);
        else if (r instanceof Integer)
          return ((BigDecimal) l).divide(new BigDecimal(r.intValue()), RoundingMode.HALF_UP);
        else if (r instanceof Short)
          return ((BigDecimal) l).divide(new BigDecimal(r.shortValue()), RoundingMode.HALF_UP);
      }
    }

    return null;
  }

  @Override
  public OIndexReuseType getIndexReuseType(Object iLeft, Object iRight) {
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
