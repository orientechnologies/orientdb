/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 * <p>
 * For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.engine;

import static com.orientechnologies.lucene.builder.OLuceneQueryBuilder.EMPTY_METADATA;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.lucene.collections.OLuceneResultSet;
import com.orientechnologies.lucene.collections.OLuceneResultSetEmpty;
import com.orientechnologies.lucene.query.OLuceneQueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngineException;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.spatial.query.OSpatialQueryContext;
import com.orientechnologies.spatial.shape.OShapeBuilder;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.spatial.SpatialStrategy;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;

public class OLuceneGeoSpatialIndexEngine extends OLuceneSpatialIndexEngineAbstract {

  public OLuceneGeoSpatialIndexEngine(
      OStorage storage, String name, int id, OShapeBuilder factory) {
    super(storage, name, id, factory);
  }

  @Override
  protected SpatialStrategy createSpatialStrategy(
      OIndexDefinition indexDefinition, ODocument metadata) {

    return strategyFactory.createStrategy(ctx, getDatabase(), indexDefinition, metadata);
  }

  @Override
  public Object get(Object key) {
    return getInTx(key, null);
  }

  @Override
  public Set<OIdentifiable> getInTx(Object key, OLuceneTxChanges changes) {
    updateLastAccess();
    openIfClosed();
    try {
      if (key instanceof Map) {
        //noinspection unchecked
        return newGeoSearch((Map<String, Object>) key, changes);
      }
    } catch (Exception e) {
      if (e instanceof OException) {
        OException forward = (OException) e;
        throw forward;
      } else {
        throw OException.wrapException(new OIndexEngineException("Error parsing lucene query"), e);
      }
    }

    return new OLuceneResultSetEmpty();
  }

  private Set<OIdentifiable> newGeoSearch(Map<String, Object> key, OLuceneTxChanges changes)
      throws Exception {

    OLuceneQueryContext queryContext = queryStrategy.build(key).withChanges(changes);
    return new OLuceneResultSet(this, queryContext, EMPTY_METADATA);
  }

  @Override
  public void onRecordAddedToResultSet(
      OLuceneQueryContext queryContext,
      OContextualRecordId recordId,
      Document doc,
      ScoreDoc score) {

    OSpatialQueryContext spatialContext = (OSpatialQueryContext) queryContext;
    if (spatialContext.spatialArgs != null) {
      updateLastAccess();
      openIfClosed();
      @SuppressWarnings("deprecation")
      Point docPoint = (Point) ctx.readShape(doc.get(strategy.getFieldName()));
      double docDistDEG =
          ctx.getDistCalc().distance(spatialContext.spatialArgs.getShape().getCenter(), docPoint);
      final double docDistInKM =
          DistanceUtils.degrees2Dist(docDistDEG, DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM);
      recordId.setContext(
          new HashMap<String, Object>() {
            {
              put("distance", docDistInKM);
            }
          });
    }
  }

  @Override
  public void put(OAtomicOperation atomicOperation, Object key, Object value) {

    if (key instanceof OIdentifiable) {
      openIfClosed();
      ODocument location = ((OIdentifiable) key).getRecord();
      @SuppressWarnings("unchecked")
      Collection<OIdentifiable> container = (Collection<OIdentifiable>) value;
      for (OIdentifiable oIdentifiable : container) {
        updateLastAccess();
        addDocument(newGeoDocument(oIdentifiable, factory.fromDoc(location)));
      }
    }
  }

  @Override
  public void update(
      OAtomicOperation atomicOperation, Object key, OIndexKeyUpdater<Object> updater) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean validatedPut(
      OAtomicOperation atomicOperation, Object key, ORID value, Validator<Object, ORID> validator) {
    throw new UnsupportedOperationException(
        "Validated put is not supported by OLuceneGeoSpatialIndexEngine");
  }

  @Override
  public Document buildDocument(Object key, OIdentifiable value) {
    ODocument location = ((OIdentifiable) key).getRecord();
    return newGeoDocument(value, factory.fromDoc(location));
  }

  @Override
  public boolean isLegacy() {
    return false;
  }

  @Override
  public void updateUniqueIndexVersion(Object key) {
    // not implemented
  }

  @Override
  public int getUniqueIndexVersion(Object key) {
    return 0; // not implemented
  }
}
