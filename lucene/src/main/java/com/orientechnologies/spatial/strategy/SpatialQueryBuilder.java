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
package com.orientechnologies.spatial.strategy;

import com.orientechnologies.orient.core.index.OIndexEngineException;
import com.orientechnologies.spatial.engine.OLuceneSpatialIndexContainer;
import com.orientechnologies.spatial.query.OSpatialQueryContext;
import com.orientechnologies.spatial.shape.OShapeBuilder;
import java.util.HashMap;
import java.util.Map;

/** Created by Enrico Risa on 10/08/15. */
public class SpatialQueryBuilder extends SpatialQueryBuilderAbstract {

  private Map<String, SpatialQueryBuilderAbstract> operators =
      new HashMap<String, SpatialQueryBuilderAbstract>();

  public SpatialQueryBuilder(OLuceneSpatialIndexContainer manager, OShapeBuilder factory) {
    super(manager, factory);
    initOperators(manager, factory);
  }

  private void initOperators(OLuceneSpatialIndexContainer manager, OShapeBuilder factory) {
    addOperator(new SpatialQueryBuilderWithin(manager, factory));
    addOperator(new SpatialQueryBuilderContains(manager, factory));
    addOperator(new SpatialQueryBuilderNear(manager, factory));
    addOperator(new SpatialQueryBuilderDWithin(manager, factory));
    addOperator(new SpatialQueryBuilderIntersects(manager, factory));
    addOperator(new SpatialQueryBuilderDistanceSphere(manager, factory));
    addOperator(new SpatialQueryBuilderOverlap(manager, factory));
  }

  private void addOperator(SpatialQueryBuilderAbstract builder) {
    operators.put(builder.getName(), builder);
  }

  public OSpatialQueryContext build(Map<String, Object> query) throws Exception {

    SpatialQueryBuilderAbstract operation = parseOperation(query);

    return operation.build(query);
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException();
  }

  private SpatialQueryBuilderAbstract parseOperation(Map<String, Object> query) {

    String operator = (String) query.get(GEO_FILTER);
    SpatialQueryBuilderAbstract spatialQueryBuilder = operators.get(operator);
    if (spatialQueryBuilder == null) {
      throw new OIndexEngineException("Operator " + operator + " not supported.", null);
    }
    return spatialQueryBuilder;
  }
}
