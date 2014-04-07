package com.orientechnologies.lucene.operator;

import com.orientechnologies.lucene.collections.OSpatialCompositeKey;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.operator.OIndexReuseType;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorEqualityNotNulls;
import org.apache.lucene.spatial.query.SpatialOperation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by enricorisa on 07/04/14.
 */
public class OLuceneWithinOperator extends OQueryOperatorEqualityNotNulls {

  protected OLuceneWithinOperator() {
    super("WITHIN", 5, false, 1, true);
  }

  @Override
  protected boolean evaluateExpression(OIdentifiable iRecord, OSQLFilterCondition iCondition, Object iLeft, Object iRight,
      OCommandContext iContext) {
    return true;
  }

  @Override
  public OIndexReuseType getIndexReuseType(Object iLeft, Object iRight) {
    return OIndexReuseType.INDEX_CUSTOM;
  }

  @Override
  public boolean executeIndexQuery(OCommandContext iContext, OIndex<?> index, List<Object> keyParams, boolean ascSortOrder,
      OIndex.IndexValuesResultListener resultListener) {
    OIndexDefinition definition = index.getDefinition();
    int idxSize = definition.getFields().size();
    int paramsSize = keyParams.size();

    List<Object> params = new ArrayList<Object>();

    for (Object param : keyParams) {
      if (param instanceof Collection) {
        for (Object p : (Collection) param) {
          params.add(p);
        }
      } else {
        return false;
      }
    }
    Object result = index.get(new OSpatialCompositeKey(params).setOperation(SpatialOperation.IsWithin));
    convertIndexResult(result, resultListener);
    return true;
  }

  private void convertIndexResult(Object indexResult, OIndex.IndexValuesResultListener resultListener) {
    if (indexResult instanceof Collection) {
      for (OIdentifiable identifiable : (Collection<OIdentifiable>) indexResult) {
        if (!resultListener.addResult(identifiable))
          return;
      }
    } else if (indexResult != null)
      resultListener.addResult((OIdentifiable) indexResult);
  }

  @Override
  public ORID getBeginRidRange(Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public ORID getEndRidRange(Object iLeft, Object iRight) {
    return null;
  }
}
