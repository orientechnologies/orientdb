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

import com.orientechnologies.spatial.engine.OLuceneSpatialIndexContainer;
import com.orientechnologies.spatial.query.OSpatialQueryContext;
import com.orientechnologies.spatial.shape.OShapeBuilder;
import java.util.Map;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.locationtech.spatial4j.shape.Shape;

/** Created by Enrico Risa on 11/08/15. */
public class SpatialQueryBuilderContains extends SpatialQueryBuilderAbstract {

  public static final String NAME = "contains";

  public SpatialQueryBuilderContains(OLuceneSpatialIndexContainer manager, OShapeBuilder factory) {
    super(manager, factory);
  }

  @Override
  public OSpatialQueryContext build(Map<String, Object> query) throws Exception {
    Shape shape = parseShape(query);
    SpatialStrategy strategy = manager.strategy();

    if (isOnlyBB(strategy)) {
      shape = shape.getBoundingBox();
    }
    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, shape);

    Query filterQuery = strategy.makeQuery(args);

    BooleanQuery q =
        new BooleanQuery.Builder()
            .add(filterQuery, BooleanClause.Occur.MUST)
            .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD)
            .build();

    return new OSpatialQueryContext(null, manager.searcher(), q);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
