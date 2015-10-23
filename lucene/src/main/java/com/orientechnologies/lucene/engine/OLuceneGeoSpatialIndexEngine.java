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

package com.orientechnologies.lucene.engine;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.collections.LuceneResultSetFactory;
import com.orientechnologies.lucene.query.QueryContext;
import com.orientechnologies.lucene.query.SpatialQueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.spatial.shape.OShapeBuilder;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.spatial.SpatialStrategy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class OLuceneGeoSpatialIndexEngine extends OLuceneSpatialIndexEngineAbstract {

  public OLuceneGeoSpatialIndexEngine(String name, OShapeBuilder factory) {
    super(name, factory);
  }

  @Override
  protected SpatialStrategy createSpatialStrategy(OIndexDefinition indexDefinition, ODocument metadata) {

    return strategyFactory.createStrategy(ctx, getDatabase(), indexDefinition, metadata);

  }

  @Override
  public Object get(Object key) {
    return getInTx(key, null);
  }

  @Override
  public Object getInTx(Object key, OLuceneTxChanges changes) {
    try {
      if (key instanceof Map) {
        return newGeoSearch((Map<String, Object>) key, changes);

      } else {

      }
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on getting entry against Lucene index", e);
    }

    return null;
  }

  private Object newGeoSearch(Map<String, Object> key, OLuceneTxChanges changes) throws Exception {

    QueryContext queryContext = queryStrategy.build(key).setChanges(changes);
    return LuceneResultSetFactory.INSTANCE.create(this, queryContext);

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
  public void put(Object key, Object value) {

    if (key instanceof OIdentifiable) {

      ODocument location = ((OIdentifiable) key).getRecord();
      Collection<OIdentifiable> container = (Collection<OIdentifiable>) value;
      for (OIdentifiable oIdentifiable : container) {
        addDocument(newGeoDocument(oIdentifiable, factory.fromDoc(location)));
      }
    } else {

    }
  }

  @Override
  public Document buildDocument(Object key, OIdentifiable value) {
    ODocument location = ((OIdentifiable) key).getRecord();
    return newGeoDocument(value, factory.fromDoc(location));
  }
}
