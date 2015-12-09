/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.spatial.functions;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.spatial.strategy.SpatialQueryBuilderDWithin;
import com.spatial4j.core.shape.Shape;

import java.util.Collection;

/**
 * Created by Enrico Risa on 12/08/15.
 */
public class OSTDWithinFunction extends OSpatialFunctionAbstract {

  public static final String NAME = "st_dwithin";

  public OSTDWithinFunction() {
    super(NAME, 3, 3);
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {

    Shape shape = factory.fromObject(iParams[0]);

    Shape shape1 = factory.fromObject(iParams[1]);

    Double distance = (Double) iParams[2];

    return factory.operation().isWithInDistance(shape, shape1, distance);
  }

  @Override
  public String getSyntax() {
    return null;
  }

  @Override
  public Iterable<OIdentifiable> searchFromTarget(OFromClause target, OBinaryCompareOperator operator, Object rightValue,
      OCommandContext ctx, OExpression... args) {
    return results(target, args, ctx);
  }

  @Override
  public long estimate(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {

    Collection resultSet = results(target, args, ctx);
    return resultSet == null ? -1 : resultSet.size();
  }

  @Override
  protected String operator() {
    return SpatialQueryBuilderDWithin.NAME;
  }
}
