package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.collections.OLuceneResultSet;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.functions.OIndexableSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.parser.*;

/**
 * Created by frank on 15/01/2017.
 */
public class OLuceneSearchFunction extends OSQLFunctionAbstract implements OIndexableSQLFunction {
  public static final String NAME = "LUCENE_SEARCH";

  public OLuceneSearchFunction() {
    super(NAME, 2, 2);
  }

  @Override
  public String getName() {
    return NAME;
  }

  protected OIndex searchForIndex(OFromClause target, OExpression[] args) {

    // TODO Check if target is a class otherwise exception

    OFromItem item = target.getItem();
    OIdentifier identifier = item.getIdentifier();
    String indexName = args[0].toString();

    System.out.println("indexName = " + indexName);
    OIndex<?> index = getDb().getMetadata().getIndexManager().getIndex(indexName);
    return index;
  }

  protected ODatabaseDocumentInternal getDb() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {

    return null;
  }

  @Override
  public String getSyntax() {
    return "SYNTAXT FOR LUCENE_SEARCH";
  }

  @Override
  public Iterable<OIdentifiable> searchFromTarget(OFromClause target, OBinaryCompareOperator operator, Object rightValue,
      OCommandContext ctx, OExpression... args) {

    OIndex index = searchForIndex(target, args);

    System.out.println("target = " + target);
    System.out.println("operator = " + operator);
    System.out.println("rightValue = " + rightValue);
    System.out.println("ctx = " + ctx);
    System.out.println("args = " + args);

    if (index != null) {

      OExpression exp = args[1];
      System.out.println("exp = " + exp);
      return (OLuceneResultSet) index.get(exp.toString());
    }
    return null;
  }

  @Override
  public long estimate(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {
    System.out.println("estimate");
    System.out.println("target = " + target);
    return 0;
  }

  @Override
  public boolean canExecuteWithoutIndex(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {
    return true;
  }

  @Override
  public boolean allowsIndexedExecution(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {
    return true;
  }

  @Override
  public boolean shouldExecuteAfterSearch(OFromClause target, OBinaryCompareOperator operator, Object rightValue,
      OCommandContext ctx, OExpression... args) {
    return true;
  }
}
