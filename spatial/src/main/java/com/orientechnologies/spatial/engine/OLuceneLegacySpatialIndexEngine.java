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

package com.orientechnologies.spatial.engine;

import com.orientechnologies.lucene.collections.LuceneResultSet;
import com.orientechnologies.lucene.collections.OSpatialCompositeKey;
import com.orientechnologies.lucene.query.QueryContext;
import com.orientechnologies.lucene.query.SpatialQueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngineException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.spatial.shape.OShapeBuilder;
import com.orientechnologies.orient.spatial.shape.legacy.OShapeBuilderLegacy;
import com.orientechnologies.orient.spatial.shape.legacy.OShapeBuilderLegacyImpl;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.document.Document;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.*;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Enrico Risa on 26/09/15.
 */
public class OLuceneLegacySpatialIndexEngine extends OLuceneSpatialIndexEngineAbstract {

  OShapeBuilderLegacy legacyBuilder = OShapeBuilderLegacyImpl.INSTANCE; ;

  public OLuceneLegacySpatialIndexEngine(String indexName, OShapeBuilder factory) {
    super(indexName, factory);
  }

  private Object legacySearch(Object key) throws IOException {
    if (key instanceof OSpatialCompositeKey) {
      final OSpatialCompositeKey newKey = (OSpatialCompositeKey) key;

      final SpatialOperation strategy = newKey.getOperation() != null ? newKey.getOperation() : SpatialOperation.Intersects;

      if (SpatialOperation.Intersects.equals(strategy))
        return searchIntersect(newKey, newKey.getMaxDistance(), newKey.getContext());
      else if (SpatialOperation.IsWithin.equals(strategy))
        return searchWithin(newKey, newKey.getContext());

    } else if (key instanceof OCompositeKey) {
      return searchIntersect((OCompositeKey) key, 0, null);
    }
    throw new OIndexEngineException("Unknown key" + key, null);

  }

  public Object searchIntersect(OCompositeKey key, double distance, OCommandContext context) throws IOException {

    double lat = ((Double) OType.convert(((OCompositeKey) key).getKeys().get(0), Double.class)).doubleValue();
    double lng = ((Double) OType.convert(((OCompositeKey) key).getKeys().get(1), Double.class)).doubleValue();
    SpatialOperation operation = SpatialOperation.Intersects;

    Point p = ctx.makePoint(lng, lat);
    SpatialArgs args = new SpatialArgs(operation, ctx.makeCircle(lng, lat,
        DistanceUtils.dist2Degrees(distance, DistanceUtils.EARTH_MEAN_RADIUS_KM)));
    Filter filter = strategy.makeFilter(args);
    IndexSearcher searcher = searcher();
    ValueSource valueSource = strategy.makeDistanceValueSource(p);
    Sort distSort = new Sort(valueSource.getSortField(false)).rewrite(searcher);

    return new LuceneResultSet(this,
        new SpatialQueryContext(context, searcher, new MatchAllDocsQuery(), filter, distSort).setSpatialArgs(args));
  }

  public Object searchWithin(OSpatialCompositeKey key, OCommandContext context) throws IOException {

    Set<OIdentifiable> result = new HashSet<OIdentifiable>();

    Shape shape = legacyBuilder.makeShape(key, ctx);
    if (shape == null)
      return null;
    SpatialArgs args = new SpatialArgs(SpatialOperation.IsWithin, shape);
    IndexSearcher searcher = searcher();

    Filter filter = strategy.makeFilter(args);

    return new LuceneResultSet(this, new SpatialQueryContext(context, searcher, new MatchAllDocsQuery(), filter));
  }

  @Override
  public void onRecordAddedToResultSet(QueryContext queryContext, OContextualRecordId recordId, Document doc, ScoreDoc score) {

    SpatialQueryContext spatialContext = (SpatialQueryContext) queryContext;
    if (spatialContext.spatialArgs != null) {
      Point docPoint = (Point) ctx.readShape(doc.get(strategy.getFieldName()));
      double docDistDEG = ctx.getDistCalc().distance(spatialContext.spatialArgs.getShape().getCenter(), docPoint);
      final double docDistInKM = DistanceUtils.degrees2Dist(docDistDEG, DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM);
      recordId.setContext(new HashMap<String, Object>() {
        {
          put("distance", docDistInKM);
        }
      });
    }
  }

  @Override
  public Object getInTx(Object key, OLuceneTxChanges changes) {
    try {
      return legacySearch(key);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Object get(Object key) {
    return getInTx(key, null);
  }

  @Override
  public void put(Object key, Object value) {

    if (key instanceof OCompositeKey) {
      OCompositeKey compositeKey = (OCompositeKey) key;
      Collection<OIdentifiable> container = (Collection<OIdentifiable>) value;
      for (OIdentifiable oIdentifiable : container) {
        addDocument(newGeoDocument(oIdentifiable, legacyBuilder.makeShape(compositeKey, ctx)));
      }
    } else {

    }

  }

  @Override
  protected SpatialStrategy createSpatialStrategy(OIndexDefinition indexDefinition, ODocument metadata) {
    return new RecursivePrefixTreeStrategy(new GeohashPrefixTree(ctx, 11), "location");
  }
}
