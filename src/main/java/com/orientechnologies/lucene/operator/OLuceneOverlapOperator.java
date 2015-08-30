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

package com.orientechnologies.lucene.operator;

import com.orientechnologies.lucene.collections.OSpatialCompositeKey;
import com.orientechnologies.orient.spatial.strategy.SpatialQueryBuilderAbstract;
import com.orientechnologies.orient.spatial.strategy.SpatialQueryBuilderOverlap;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexCursorCollectionValue;
import com.orientechnologies.orient.core.index.OIndexCursorSingleValue;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.spatial.query.SpatialOperation;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OLuceneOverlapOperator extends OLuceneSpatialOperator {

  public OLuceneOverlapOperator() {
    super("&&", 5, false);
  }

  @Override
  public Collection<OIdentifiable> filterRecords(ODatabase<?> iRecord, List<String> iTargetClasses, OSQLFilterCondition iCondition,
      Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public OIndexCursor executeIndexQuery(OCommandContext iContext, OIndex<?> index, List<Object> keyParams, boolean ascSortOrder) {
    OIndexCursor cursor;
    Object key;
    key = keyParams.get(0);
    Object indexParam = key;
    if (key instanceof Map) {
      ((Map) key).put(SpatialQueryBuilderAbstract.GEO_FILTER, SpatialQueryBuilderOverlap.NAME);
    } else if (key instanceof ODocument) {
      Map<String, Object> newKey = new HashMap<String,Object>();
      newKey.put(SpatialQueryBuilderAbstract.GEO_FILTER, SpatialQueryBuilderOverlap.NAME);
      newKey.put("shape", key);
      indexParam = newKey;
    }
    Object indexResult = index.get(indexParam);
    if (indexResult == null || indexResult instanceof OIdentifiable)
      cursor = new OIndexCursorSingleValue((OIdentifiable) indexResult, new OSpatialCompositeKey(keyParams));
    else
      cursor = new OIndexCursorCollectionValue(((Collection<OIdentifiable>) indexResult).iterator(), new OSpatialCompositeKey(
          keyParams));

    return cursor;
  }

  @Override
  public Object evaluateRecord(OIdentifiable iRecord, ODocument iCurrentResult, OSQLFilterCondition iCondition, Object iLeft,
      Object iRight, OCommandContext iContext) {
    Shape shape = factory.fromDoc((ODocument) iLeft);

    Object right = iCondition.getRight();
    Shape shape1 = null;

    if (iRight instanceof ODocument) {
      shape1 = factory.fromDoc((ODocument) iRight);
    } else {
      if (right instanceof Map) {
        Map map = (Map) iCondition.getRight();
        shape1 = factory.fromMapGeoJson((Map) map.get("shape"));
      }
    }
    return SpatialOperation.BBoxIntersects.evaluate(shape, shape1);
  }
}
