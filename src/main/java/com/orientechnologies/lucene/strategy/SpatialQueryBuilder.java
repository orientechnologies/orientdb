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

package com.orientechnologies.lucene.strategy;

import com.orientechnologies.lucene.manager.OLuceneSpatialIndexContainer;
import com.orientechnologies.lucene.query.SpatialQueryContext;
import com.orientechnologies.lucene.shape.OShapeBuilder;
import com.orientechnologies.orient.core.index.OIndexEngineException;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Enrico Risa on 10/08/15.
 */
public class SpatialQueryBuilder extends SpatialQueryBuilderAbstract {

  private Map<String, SpatialQueryBuilderAbstract> operators = new HashMap<String, SpatialQueryBuilderAbstract>();

  public SpatialQueryBuilder(OLuceneSpatialIndexContainer manager, OShapeBuilder factory) {
    super(manager, factory);
    initOperators(manager, factory);
  }

  private void initOperators(OLuceneSpatialIndexContainer manager, OShapeBuilder factory) {
    operators.put("within", new SpatialQueryBuilderWithin(manager, factory));
    operators.put("contains", new SpatialQueryBuilderContains(manager, factory));
    operators.put("near", new SpatialQueryBuilderNear(manager, factory));
    operators.put("&&", new SpatialQueryBuilderOverlap(manager, factory));
  }

  public SpatialQueryContext build(Map<String, Object> query) throws Exception {

    SpatialQueryBuilderAbstract operation = parseOperation(query);

    return operation.build(query);
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException();
  }

  private SpatialQueryBuilderAbstract parseOperation(Map<String, Object> query) {

    String operator = (String) query.get(GEO_FILTER);
    SpatialQueryBuilderAbstract spatialQueryBuilderAbstract = operators.get(operator);
    if (spatialQueryBuilderAbstract == null) {
      throw new OIndexEngineException("Operator " + operator + " not supported.", null);
    }
    return spatialQueryBuilderAbstract;
  }
}
