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
package com.orientechnologies.spatial.engine;

import com.orientechnologies.lucene.collections.OLuceneResultSet;
import com.orientechnologies.lucene.query.OLuceneQueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngineException;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.spatial.collections.OSpatialCompositeKey;
import com.orientechnologies.spatial.query.OSpatialQueryContext;
import com.orientechnologies.spatial.shape.OShapeBuilder;
import com.orientechnologies.spatial.shape.legacy.OShapeBuilderLegacy;
import com.orientechnologies.spatial.shape.legacy.OShapeBuilderLegacyImpl;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static com.orientechnologies.lucene.builder.OLuceneQueryBuilder.EMPTY_METADATA;

/**
 * Created by Enrico Risa on 26/09/15.
 */
public class OLuceneLegacySpatialIndexEngine extends OLuceneSpatialIndexEngineAbstract {

  OShapeBuilderLegacy legacyBuilder = OShapeBuilderLegacyImpl.INSTANCE;

  public OLuceneLegacySpatialIndexEngine(OStorage storage, String indexName, OShapeBuilder factory) {
    super(storage, indexName, factory);
  }

  private Set<OIdentifiable> legacySearch(Object key, OLuceneTxChanges changes) throws IOException {
    if (key instanceof OSpatialCompositeKey) {
      final OSpatialCompositeKey newKey = (OSpatialCompositeKey) key;

      final SpatialOperation strategy = newKey.getOperation() != null ? newKey.getOperation() : SpatialOperation.Intersects;

      if (SpatialOperation.Intersects.equals(strategy))
        return searchIntersect(newKey, newKey.getMaxDistance(), newKey.getContext(), changes);
      else if (SpatialOperation.IsWithin.equals(strategy))
        return searchWithin(newKey, newKey.getContext(), changes);

    } else if (key instanceof OCompositeKey) {
      return searchIntersect((OCompositeKey) key, 0, null, changes);
    }
    throw new OIndexEngineException("Unknown key" + key, null);

  }

  public Set<OIdentifiable> searchIntersect(OCompositeKey key, double distance, OCommandContext context, OLuceneTxChanges changes)
      throws IOException {

    double lat = ((Double) OType.convert(((OCompositeKey) key).getKeys().get(0), Double.class)).doubleValue();
    double lng = ((Double) OType.convert(((OCompositeKey) key).getKeys().get(1), Double.class)).doubleValue();
    SpatialOperation operation = SpatialOperation.Intersects;

    Point p = ctx.makePoint(lng, lat);
    SpatialArgs args = new SpatialArgs(operation,
        ctx.makeCircle(lng, lat, DistanceUtils.dist2Degrees(distance, DistanceUtils.EARTH_MEAN_RADIUS_KM)));
    Query filterQuery = strategy.makeQuery(args);

    IndexSearcher searcher = searcher();
    DoubleValuesSource valueSource = strategy.makeDistanceValueSource(p);
    Sort distSort = new Sort(valueSource.getSortField(false)).rewrite(searcher);

    BooleanQuery q = new BooleanQuery.Builder().add(filterQuery, BooleanClause.Occur.MUST)
        .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD).build();

    OLuceneQueryContext queryContext = new OSpatialQueryContext(context, searcher, q, Arrays.asList(distSort.getSort()))
        .setSpatialArgs(args).withChanges(changes);
    return new OLuceneResultSet(this, queryContext, EMPTY_METADATA);
  }

  public Set<OIdentifiable> searchWithin(OSpatialCompositeKey key, OCommandContext context, OLuceneTxChanges changes)
      throws IOException {

    Set<OIdentifiable> result = new HashSet<OIdentifiable>();

    Shape shape = legacyBuilder.makeShape(key, ctx);
    if (shape == null)
      return null;
    SpatialArgs args = new SpatialArgs(SpatialOperation.IsWithin, shape);
    IndexSearcher searcher = searcher();

    Query filterQuery = strategy.makeQuery(args);

    BooleanQuery query = new BooleanQuery.Builder().add(filterQuery, BooleanClause.Occur.MUST)
        .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD).build();

    OLuceneQueryContext queryContext = new OSpatialQueryContext(context, searcher, query).withChanges(changes);

    return new OLuceneResultSet(this, queryContext, EMPTY_METADATA);

  }

  @Override
  public void onRecordAddedToResultSet(OLuceneQueryContext queryContext, OContextualRecordId recordId, Document doc,
      ScoreDoc score) {

    OSpatialQueryContext spatialContext = (OSpatialQueryContext) queryContext;
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
  public Set<OIdentifiable> getInTx(Object key, OLuceneTxChanges changes) {
    try {
      updateLastAccess();
      openIfClosed();
      return legacySearch(key, changes);
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
      updateLastAccess();
      openIfClosed();
      OCompositeKey compositeKey = (OCompositeKey) key;
      Collection<OIdentifiable> container = (Collection<OIdentifiable>) value;
      for (OIdentifiable oIdentifiable : container) {
        addDocument(newGeoDocument(oIdentifiable, legacyBuilder.makeShape(compositeKey, ctx)));
      }
    } else {

    }

  }

  @Override
  public void update(Object key, OIndexKeyUpdater<Object> updater) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean validatedPut(Object key, ORID value, Validator<Object, ORID> validator) {
    throw new UnsupportedOperationException("Validated put is not supported by OLuceneLegacySpatialIndexEngine");
  }

  @Override
  public Document buildDocument(Object key, OIdentifiable value) {
    return newGeoDocument(value, legacyBuilder.makeShape((OCompositeKey) key, ctx));
  }

  @Override
  protected SpatialStrategy createSpatialStrategy(OIndexDefinition indexDefinition, ODocument metadata) {
    return new RecursivePrefixTreeStrategy(new GeohashPrefixTree(ctx, 11), "location");
  }

  @Override
  public boolean isLegacy() {
    return true;
  }
}
