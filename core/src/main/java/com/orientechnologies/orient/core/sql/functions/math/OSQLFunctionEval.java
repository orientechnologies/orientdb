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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import java.util.List;

/**
 * Evaluates a complex expression.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionEval extends OSQLFunctionMathAbstract {
  public static final String NAME = "eval";

  private OSQLPredicate predicate;

  public OSQLFunctionEval() {
    super(NAME, 1, 1);
  }

  public Object execute(
      Object iThis,
      final OIdentifiable iRecord,
      final Object iCurrentResult,
      final Object[] iParams,
      OCommandContext iContext) {
    if (iParams.length < 1) {
      throw new OCommandExecutionException("invalid ");
    }
    if (predicate == null) predicate = new OSQLPredicate(String.valueOf(iParams[0]));

    final ODocument currentResult =
        iCurrentResult instanceof ODocument ? (ODocument) iCurrentResult : null;
    try {
      return predicate.evaluate(
          iRecord != null ? iRecord.getRecord() : null, currentResult, iContext);
    } catch (ArithmeticException e) {
      OLogManager.instance().error(this, "Division by 0", e);
      // DIVISION BY 0
      return 0;
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error during division", e);
      return null;
    }
  }

  public boolean aggregateResults() {
    return false;
  }

  public String getSyntax() {
    return "eval(<expression>)";
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    return null;
  }
}
