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
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OOrBlock;
import java.util.List;
import java.util.Optional;

/**
 * Evaluates a complex expression.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionEval extends OSQLFunctionMathAbstract {
  private static final OLogger logger = OLogManager.instance().logger(OSQLFunctionEval.class);

  public static final String NAME = "eval";

  private OOrBlock predicate;
  private OExpression expression;

  public OSQLFunctionEval() {
    super(NAME, 1, 1);
  }

  public Object execute(
      Object iThis,
      final OResult iRecord,
      final Object iCurrentResult,
      final Object[] iParams,
      OCommandContext iContext) {
    if (iParams.length < 1) {
      throw new OCommandExecutionException("invalid ");
    }

    if (predicate == null && expression == null) {
      Optional<OOrBlock> res = OSQLEngine.maybeParsePredicate(String.valueOf(iParams[0]));
      if (res.isPresent()) {
        this.predicate = res.get();
      } else {
        expression = OSQLEngine.parseExpression(String.valueOf(iParams[0]));
      }
    }

    try {
      if (predicate != null) {
        return predicate.evaluate(iRecord, iContext);
      } else {
        return expression.execute(iRecord, iContext);
      }
    } catch (ArithmeticException e) {
      logger.error("Division by 0", e);
      // DIVISION BY 0
      return 0;
    } catch (Exception e) {
      logger.error("Error during division", e);
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
  public Object getResult(OCommandContext ctx) {
    return null;
  }

  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    return null;
  }
}
