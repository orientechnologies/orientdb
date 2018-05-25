/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.strategy;

import com.orientechnologies.spatial.engine.OLuceneSpatialIndexContainer;
import com.orientechnologies.spatial.query.OSpatialQueryContext;
import com.orientechnologies.spatial.shape.OShapeBuilder;
import com.orientechnologies.spatial.shape.OShapeFactory;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.locationtech.spatial4j.shape.Shape;

import java.util.Map;

/**
 * Created by Marko Djurovic
 */
public class SpatialQueryBuilderSTOverlaps extends SpatialQueryBuilderAbstract {

  public static final String NAME = "overlaps";

  public SpatialQueryBuilderSTOverlaps(OLuceneSpatialIndexContainer manager, OShapeBuilder factory) {
    super(manager, factory);
  }

  // TODO check PGIS
  @Override
  public OSpatialQueryContext build(Map<String, Object> query) throws Exception {
    OShapeFactory shapeFactoty = OShapeFactory.INSTANCE;
//    SpatialContext ctx = shapeFactoty.context();
    
    Shape shape = parseShape(query);
    SpatialStrategy strategy = manager.strategy();
    
    if (isOnlyBB(strategy)) {
      shape = shape.getBoundingBox();
    }
    
//    SpatialPrefixTree gridGeohash = new GeohashPrefixTree(ctx, 11);
//    
//    RecursivePrefixTreeStrategy indexStrategy = new RecursivePrefixTreeStrategy(gridGeohash, "recursive_geohash");
//    SerializedDVStrategy geometryStrategy = new SerializedDVStrategy(ctx, "location");
//    SpatialStrategy strategy = new CompositeSpatialStrategy("location", indexStrategy, geometryStrategy);
    
    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, shape);
    Query intersectsQuery = strategy.makeQuery(args);
    Query withinQuery = strategy.makeQuery(new SpatialArgs(SpatialOperation.IsWithin, shape));
    Query containsQuery = strategy.makeQuery(new SpatialArgs(SpatialOperation.Contains, shape));
    BooleanQuery q = new BooleanQuery.Builder().add(intersectsQuery, BooleanClause.Occur.MUST)
        .add(withinQuery, BooleanClause.Occur.MUST_NOT)
        .add(containsQuery, BooleanClause.Occur.MUST_NOT)
        .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD).
        build();

    return new OSpatialQueryContext(null, manager.searcher(), q);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
