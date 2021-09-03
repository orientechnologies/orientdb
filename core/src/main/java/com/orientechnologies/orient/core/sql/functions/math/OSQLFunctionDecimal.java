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
package com.orientechnologies.orient.core.sql.functions.math;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/**
 * Evaluates a complex expression.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionDecimal extends OSQLFunctionMathAbstract {
  public static final String NAME = "decimal";
  private Object result;

  public OSQLFunctionDecimal() {
    super(NAME, 1, 1);
  }

  public Object execute(
      Object iThis,
      final OIdentifiable iRecord,
      final Object iCurrentResult,
      final Object[] iParams,
      OCommandContext iContext) {
    Object inputValue = iParams[0];
    if (inputValue == null) {
      result = null;
    }

    if (inputValue instanceof BigDecimal) {
      result = inputValue;
    }
    if (inputValue instanceof BigInteger) {
      result = new BigDecimal((BigInteger) inputValue);
    }
    if (inputValue instanceof Integer) {
      result = new BigDecimal(((Integer) inputValue));
    }

    if (inputValue instanceof Long) {
      result = new BigDecimal(((Long) inputValue));
    }

    if (inputValue instanceof Number) {
      result = new BigDecimal(((Number) inputValue).doubleValue());
    }

    try {
      if (inputValue instanceof String) {
        result = new BigDecimal((String) inputValue);
      }

    } catch (Exception ignore) {
      result = null;
    }
    return getResult();
  }

  public boolean aggregateResults() {
    return false;
  }

  public String getSyntax() {
    return "decimal(<val>)";
  }

  @Override
  public Object getResult() {
    return result;
  }

  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    return null;
  }
}
