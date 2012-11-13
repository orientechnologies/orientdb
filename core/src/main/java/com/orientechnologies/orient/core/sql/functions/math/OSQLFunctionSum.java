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
package com.orientechnologies.orient.core.sql.functions.math;

import java.util.List;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Computes the sum of field. Uses the context to save the last sum number. When different Number class are used, take the class
 * with most precision.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionSum extends OSQLFunctionMathAbstract {
  public static final String NAME = "sum";

  private Number             sum;

  public OSQLFunctionSum() {
    super(NAME, 1, -1);
  }

  public Object execute(final OIdentifiable iCurrentRecord, ODocument iCurrentResult, final Object[] iParameters,
      OCommandContext iContext) {
    if (iParameters.length == 1) {
      if (iParameters[0] instanceof Number)
        sum((Number) iParameters[0]);
      else if (OMultiValue.isMultiValue(iParameters[0]))
        for (Object n : OMultiValue.getMultiValueIterable(iParameters[0]))
          sum((Number) n);
      return sum;
    } else {
      sum = null;
      for (int i = 0; i < iParameters.length; ++i)
        sum((Number) iParameters[i]);
      return sum;
    }
  }

  protected void sum(final Number value) {
    if (value != null) {
      if (sum == null)
        // FIRST TIME
        sum = value;
      else
        sum = OType.increment(sum, value);
    }
  }

  @Override
  public boolean aggregateResults() {
    return configuredParameters.length == 1;
  }

  public String getSyntax() {
    return "Syntax error: sum(<field> [,<field>*])";
  }

  @Override
  public Object getResult() {
    return sum;
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
        else
          sum = OType.increment(sum, value);
      }
    }
    return sum;
  }
}
