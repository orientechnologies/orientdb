package com.orientechnologies.lucene.operator;

import java.util.Collection;
import java.util.List;

import com.orientechnologies.lucene.collections.OSpatialCompositeKey;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OIndexSearchResult;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.operator.OIndexReuseType;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorEqualityNotNulls;

/**
 * Created by enricorisa on 14/04/14.
 */
public class OLuceneTextOperator extends OQueryOperatorEqualityNotNulls {

  public OLuceneTextOperator() {
    super("LUCENE", 5, false, 1, true);
  }

  @Override
  protected boolean evaluateExpression(OIdentifiable iRecord, OSQLFilterCondition iCondition, Object iLeft, Object iRight,
      OCommandContext iContext) {
    return true;
  }

  @Override
  public OIndexCursor executeIndexQuery(OCommandContext iContext, OIndex<?> index, List<Object> keyParams, boolean ascSortOrder) {
    OIndexCursor cursor;
    Object indexResult = index.get(new OCompositeKey(keyParams));
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
  public OIndexSearchResult getOIndexSearchResult(OClass iSchemaClass, OSQLFilterCondition iCondition,
      List<OIndexSearchResult> iIndexSearchResults, OCommandContext context) {
    return OLuceneOperatorUtil.buildOIndexSearchResult(iSchemaClass, iCondition, iIndexSearchResults, context);
  }
}
