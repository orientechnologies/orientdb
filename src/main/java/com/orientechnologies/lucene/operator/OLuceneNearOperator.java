/*
 * Copyright 2014 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.lucene.operator;

import com.orientechnologies.lucene.collections.OSpatialCompositeKey;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OIndexSearchResult;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemVariable;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.operator.OIndexReuseType;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorBetween;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorEqualityNotNulls;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorIn;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class OLuceneNearOperator extends OQueryOperatorEqualityNotNulls {

  public OLuceneNearOperator() {
    super("NEAR", 5, false, 1, true);
  }

  @Override
  protected boolean evaluateExpression(OIdentifiable iRecord, OSQLFilterCondition iCondition, Object iLeft, Object iRight,
      OCommandContext iContext) {

    SpatialContext ctx = SpatialContext.GEO;
    Object[] points = parseParams(iRecord, iCondition);
    Point p = ctx.makePoint((Double) points[3], (Double) points[2]);

    double docDistDEG = ctx.getDistCalc().distance(p, (Double) points[1], (Double) points[0]);
    double docDistInKM = DistanceUtils.degrees2Dist(docDistDEG, DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM);
    iContext.setVariable("$distance", docDistInKM);
    return true;
  }

  private Object[] parseParams(OIdentifiable iRecord, OSQLFilterCondition iCondition) {

    ODocument oDocument = (ODocument) iRecord;
    Collection left = (Collection) iCondition.getLeft();
    Collection right = (Collection) iCondition.getRight();
    Object[] params = new Object[(left.size() * 2) - 2];
    int i = 0;
    for (Object obj : left) {
      if (obj instanceof OSQLFilterItemField) {
        String fName = ((OSQLFilterItemField) obj).getFieldChain().getItemName(0);
        params[i] = oDocument.field(fName);
        i++;
      }
    }
    for (Object obj : right) {
      if (obj instanceof Number) {
        params[i] = ((Double) OType.convert(obj, Double.class)).doubleValue();
        ;
        i++;
      }
    }
    return params;
  }

  @Override
  public OIndexCursor executeIndexQuery(OCommandContext iContext, OIndex<?> index, List<Object> keyParams, boolean ascSortOrder) {

    OIndexCursor cursor;
    OIndexDefinition definition = index.getDefinition();
    int idxSize = definition.getFields().size();
    int paramsSize = keyParams.size();

    double distance = 0;
    Object spatial = iContext.getVariable("spatial");
    if (spatial != null) {

      if (spatial instanceof Number) {
        distance = ((Double) OType.convert(spatial, Double.class)).doubleValue();
      } else if (spatial instanceof Map) {
        Map<String, Object> params = (Map<String, Object>) spatial;

        Object dst = params.get("maxDistance");
        if (dst != null && dst instanceof Number) {
          distance = ((Double) OType.convert(dst, Double.class)).doubleValue();
        }
      }
    }
    Object indexResult = index.get(new OSpatialCompositeKey(keyParams).setMaxDistance(distance));
    if (indexResult == null || indexResult instanceof OIdentifiable)
      cursor = new OIndexCursor.OIndexCursorSingleValue((OIdentifiable) indexResult, new OSpatialCompositeKey(keyParams));
    else
      cursor = new OIndexCursor.OIndexCursorCollectionValue(((Collection<OIdentifiable>) indexResult).iterator(),
          new OSpatialCompositeKey(keyParams));
    return cursor;
  }

  @Override
  public OIndexReuseType getIndexReuseType(Object iLeft, Object iRight) {
    return OIndexReuseType.INDEX_OPERATOR;
  }

  @Override
  public ORID getBeginRidRange(Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public ORID getEndRidRange(Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public String getSyntax() {
    return "<left> NEAR[(<begin-deep-level> [,<maximum-deep-level> [,<fields>]] )] ( <conditions> )";
  }

  @Override
  public OIndexSearchResult getOIndexSearchResult(OClass iSchemaClass, OSQLFilterCondition iCondition,
      List<OIndexSearchResult> iIndexSearchResults, OCommandContext context) {
    return OLuceneOperatorUtil.buildOIndexSearchResult(iSchemaClass, iCondition, iIndexSearchResults, context);
  }

}
