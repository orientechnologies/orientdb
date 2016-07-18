/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.orient.core.sql.operator.math;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.operator.OIndexReuseType;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;

import java.math.BigDecimal;
import java.util.Date;

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
      Class maxPrecisionClass = OQueryOperatorMultiply.getMaxPrecisionClass(l, r);
      if (Integer.class.equals(maxPrecisionClass))
        return l.intValue() / r.intValue();
      else if (Long.class.equals(maxPrecisionClass))
        return l.longValue() / r.longValue();
      else if (Short.class.equals(maxPrecisionClass))
        return l.shortValue() / r.shortValue();
      else if (Float.class.equals(maxPrecisionClass))
        return l.floatValue() / r.floatValue();
      else if (Double.class.equals(maxPrecisionClass))
        return l.doubleValue() / r.doubleValue();
      else if (BigDecimal.class.equals(maxPrecisionClass)) {
        return (OQueryOperatorMultiply.toBigDecimal(l)).divide(OQueryOperatorMultiply.toBigDecimal(r));
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
