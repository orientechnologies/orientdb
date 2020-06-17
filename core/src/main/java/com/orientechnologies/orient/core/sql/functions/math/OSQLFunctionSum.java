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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.List;

/**
 * Computes the sum of field. Uses the context to save the last sum number. When different Number
 * class are used, take the class with most precision.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionSum extends OSQLFunctionMathAbstract {
  public static final String NAME = "sum";

  private Number sum;

  public OSQLFunctionSum() {
    super(NAME, 1, -1);
  }

  public Object execute(
      Object iThis,
      final OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      final Object[] iParams,
      OCommandContext iContext) {
    if (iParams.length == 1) {
      if (iParams[0] instanceof Number) sum((Number) iParams[0]);
      else if (OMultiValue.isMultiValue(iParams[0]))
        for (Object n : OMultiValue.getMultiValueIterable(iParams[0])) sum((Number) n);
    } else {
      sum = null;
      for (int i = 0; i < iParams.length; ++i) sum((Number) iParams[i]);
    }
    return sum;
  }

  protected void sum(final Number value) {
    if (value != null) {
      if (sum == null)
        // FIRST TIME
        sum = value;
      else sum = OType.increment(sum, value);
    }
  }

  @Override
  public boolean aggregateResults() {
    return configuredParameters.length == 1;
  }

  public String getSyntax() {
    return "sum(<field> [,<field>*])";
  }

  @Override
  public Object getResult() {
    return sum == null ? 0 : sum;
  }

  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    Number sum = null;
    for (Object iParameter : resultsToMerge) {
      final Number value = (Number) iParameter;

      if (value != null) {
        if (sum == null)
          // FIRST TIME
          sum = value;
        else sum = OType.increment(sum, value);
      }
    }
    return sum;
  }
}
