/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.functions;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.spatial.strategy.SpatialQueryBuilderWithin;
import org.locationtech.spatial4j.shape.Shape;

/** Created by Enrico Risa on 12/08/15. */
public class OSTWithinFunction extends OSpatialFunctionAbstractIndexable {

  public static final String NAME = "st_within";

  public OSTWithinFunction() {
    super(NAME, 2, 2);
  }

  @Override
  public Object execute(
      Object iThis,
      OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] iParams,
      OCommandContext iContext) {

    if (containsNull(iParams)) {
      return null;
    }
    Shape shape = factory.fromObject(iParams[0]);

    Shape shape1 = factory.fromObject(iParams[1]);

    return factory.operation().within(shape, shape1);
  }

  @Override
  public String getSyntax() {
    return null;
  }

  @Override
  public Iterable<OIdentifiable> searchFromTarget(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {
    return results(target, args, ctx, rightValue);
  }

  @Override
  protected String operator() {
    return SpatialQueryBuilderWithin.NAME;
  }
}
