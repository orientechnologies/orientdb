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
import com.orientechnologies.orient.core.index.OIndexEngineException;
import com.orientechnologies.spatial.shape.OShapeBuilder;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.bbox.BBoxStrategy;

import java.util.Map;

/**
 * Created by Enrico Risa on 11/08/15.
 */
public abstract class SpatialQueryBuilderAbstract {

  public final static String             GEO_FILTER   = "geo_filter";
  public final static String             SHAPE        = "shape";
  public final static String             SHAPE_TYPE   = "type";
  public final static String             COORDINATES  = "coordinates";
  public final static String             MAX_DISTANCE = "maxDistance";
  protected OLuceneSpatialIndexContainer manager;
  protected OShapeBuilder                factory;

  public SpatialQueryBuilderAbstract(OLuceneSpatialIndexContainer manager, OShapeBuilder factory) {
    this.manager = manager;
    this.factory = factory;
  }

  public abstract SpatialQueryContext build(Map<String, Object> query) throws Exception;

  protected Shape parseShape(Map<String, Object> query) {

    Object geometry = query.get(SHAPE);

    if (geometry == null) {
      throw new OIndexEngineException("Invalid spatial query. Missing shape field " + query, null);
    }
    return factory.fromObject(geometry);
  }

  protected boolean isOnlyBB(SpatialStrategy spatialStrategy) {
    return spatialStrategy instanceof BBoxStrategy;
  }

  public abstract String getName();
}
