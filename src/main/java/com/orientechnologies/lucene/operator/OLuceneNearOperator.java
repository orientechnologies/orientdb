package com.orientechnologies.lucene.operator;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.operator.OIndexReuseType;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorEqualityNotNulls;

/**
 * Created by enricorisa on 28/03/14.
 */
public class OLuceneNearOperator extends OQueryOperatorEqualityNotNulls {

  public OLuceneNearOperator() {
    super("NEAR", 5, false, 1, true);
  }

  @Override
  protected boolean evaluateExpression(OIdentifiable iRecord, OSQLFilterCondition iCondition, Object iLeft, Object iRight,
      OCommandContext iContext) {
    return false;
  }

  @Override
  public Object evaluateRecord(OIdentifiable iRecord, ODocument iCurrentResult, OSQLFilterCondition iCondition, Object iLeft,
      Object iRight, OCommandContext iContext) {
    return null;
  }

  @Override
  public OIndexReuseType getIndexReuseType(Object iLeft, Object iRight) {
    return null;
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
}
