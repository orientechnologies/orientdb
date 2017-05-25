package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.functions.OIndexableSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OFromClause;

/**
 * Created by frank on 25/05/2017.
 */
public abstract class OLuceneSearchFunctionTemplate extends OSQLFunctionAbstract implements OIndexableSQLFunction {

  public OLuceneSearchFunctionTemplate(String iName, int iMinParams, int iMaxParams) {
    super(iName, iMinParams, iMaxParams);

  }

  @Override
  public boolean canExecuteInline(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {
    return allowsIndexedExecution(target, operator, rightValue, ctx, args);
  }

  @Override
  public boolean allowsIndexedExecution(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {
    OLuceneFullTextIndex index = searchForIndex(target, ctx, args);
    return index != null;
  }

  @Override
  public boolean shouldExecuteAfterSearch(OFromClause target, OBinaryCompareOperator operator, Object rightValue,
      OCommandContext ctx, OExpression... args) {
    return false;
  }

  @Override
  public long estimate(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {

    OLuceneFullTextIndex index = searchForIndex(target, ctx, args);
    if (index != null)
      return index.getSize();

    return 0;
  }

  protected abstract OLuceneFullTextIndex searchForIndex(OFromClause target, OCommandContext ctx, OExpression... args);

}
