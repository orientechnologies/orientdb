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
package com.orientechnologies.spatial.operator;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.lucene.operator.OLuceneOperatorUtil;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.orientechnologies.orient.core.sql.OIndexSearchResult;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.operator.OIndexReuseType;
import com.orientechnologies.orient.core.sql.operator.OQueryTargetOperator;
import com.orientechnologies.spatial.collections.OSpatialCompositeKey;
import com.orientechnologies.spatial.shape.legacy.OShapeBuilderLegacy;
import com.orientechnologies.spatial.shape.legacy.OShapeBuilderLegacyImpl;
import java.util.List;
import java.util.stream.Stream;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

public class OLuceneWithinOperator extends OQueryTargetOperator {

  private OShapeBuilderLegacy<Shape> shapeFactory = OShapeBuilderLegacyImpl.INSTANCE;

  public OLuceneWithinOperator() {
    super("WITHIN", 5, false);
  }

  @Override
  public Object evaluateRecord(
      OIdentifiable iRecord,
      ODocument iCurrentResult,
      OSQLFilterCondition iCondition,
      Object iLeft,
      Object iRight,
      OCommandContext iContext,
      final ODocumentSerializer serializer) {
    List<Number> left = (List<Number>) iLeft;

    double lat = left.get(0).doubleValue();
    double lon = left.get(1).doubleValue();

    Shape shape = SpatialContext.GEO.makePoint(lon, lat);

    Shape shape1 =
        shapeFactory.makeShape(new OSpatialCompositeKey((List<?>) iRight), SpatialContext.GEO);

    return shape.relate(shape1) == SpatialRelation.WITHIN;
  }

  @Override
  public OIndexReuseType getIndexReuseType(Object iLeft, Object iRight) {
    return OIndexReuseType.INDEX_OPERATOR;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> executeIndexQuery(
      OCommandContext iContext, OIndex index, List<Object> keyParams, boolean ascSortOrder) {
    iContext.setVariable("$luceneIndex", true);
    //noinspection resource
    return index
        .getInternal()
        .getRids(new OSpatialCompositeKey(keyParams).setOperation(SpatialOperation.IsWithin))
        .map((rid) -> new ORawPair<>(new OSpatialCompositeKey(keyParams), rid));
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
  public OIndexSearchResult getOIndexSearchResult(
      OClass iSchemaClass,
      OSQLFilterCondition iCondition,
      List<OIndexSearchResult> iIndexSearchResults,
      OCommandContext context) {
    return OLuceneOperatorUtil.buildOIndexSearchResult(
        iSchemaClass, iCondition, iIndexSearchResults, context);
  }
}
