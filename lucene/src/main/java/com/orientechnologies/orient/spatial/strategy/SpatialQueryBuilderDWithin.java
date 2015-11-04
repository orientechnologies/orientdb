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

package com.orientechnologies.orient.spatial.strategy;

import com.orientechnologies.lucene.engine.OLuceneSpatialIndexContainer;
import com.orientechnologies.lucene.query.SpatialQueryContext;
import com.orientechnologies.orient.spatial.shape.OShapeBuilder;
import com.orientechnologies.orient.spatial.shape.OShapeFactory;
import com.spatial4j.core.shape.Shape;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;

import java.util.Map;

/**
 * Created by Enrico Risa on 11/08/15.
 */
public class SpatialQueryBuilderDWithin extends SpatialQueryBuilderAbstract {

  public static final String NAME = "dwithin";

  public SpatialQueryBuilderDWithin(OLuceneSpatialIndexContainer manager, OShapeBuilder factory) {
    super(manager, factory);
  }

  @Override
  public SpatialQueryContext build(Map<String, Object> query) throws Exception {
    Shape shape = parseShape(query);

    System.out.println("qui:: " + shape);
    SpatialStrategy strategy = manager.strategy();
    if (isOnlyBB(strategy)) {
      shape = shape.getBoundingBox();
    }
    SpatialArgs args = new SpatialArgs(SpatialOperation.IsWithin, shape);


    Geometry geo = OShapeFactory.INSTANCE.toGeometry(shape);

    Filter filter = strategy.makeFilter(args);
    return new SpatialQueryContext(null, manager.searcher(), new MatchAllDocsQuery(), filter);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
