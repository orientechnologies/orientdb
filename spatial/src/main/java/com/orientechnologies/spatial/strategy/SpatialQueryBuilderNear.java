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

package com.orientechnologies.spatial.strategy;

import com.orientechnologies.spatial.engine.OLuceneSpatialIndexContainer;
import com.orientechnologies.lucene.query.SpatialQueryContext;
import com.orientechnologies.spatial.shape.OShapeBuilder;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;

import java.util.Map;

/**
 * Created by Enrico Risa on 11/08/15.
 */
public class SpatialQueryBuilderNear extends SpatialQueryBuilderAbstract {

  public static final String NAME = "near";

  public SpatialQueryBuilderNear(OLuceneSpatialIndexContainer manager, OShapeBuilder factory) {
    super(manager, factory);
  }

  @Override
  public SpatialQueryContext build(Map<String, Object> query) throws Exception {
    Shape shape = parseShape(query);

    double distance = 0;

    Number n = (Number) query.get(MAX_DISTANCE);
    if (n != null) {
      distance = n.doubleValue();
    }

    Point p = (Point) shape;

    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, factory.context().makeCircle(p.getX(), p.getY(),
        DistanceUtils.dist2Degrees(distance, DistanceUtils.EARTH_MEAN_RADIUS_KM)));
    Filter filter = manager.strategy().makeFilter(args);
    ValueSource valueSource = manager.strategy().makeDistanceValueSource(p);
    IndexSearcher searcher = manager.searcher();
    Sort distSort = new Sort(valueSource.getSortField(false)).rewrite(searcher);
    return new SpatialQueryContext(null, searcher, new MatchAllDocsQuery(), filter, distSort).setSpatialArgs(args);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
