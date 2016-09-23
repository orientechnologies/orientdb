package com.orientechnologies.lucene.functions;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.collections.OLuceneResultSet;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.functions.OIndexableSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OFromClause;

import java.util.Collection;

/**
 * Created by frank on 19/02/2016.
 */
public class OLuceneSearchFunction extends OSQLFunctionAbstract implements OIndexableSQLFunction {

  public static final String NAME = "lucene_match";

  public OLuceneSearchFunction() {
    super(NAME, 1, 1);
  }

  @Override
  public Iterable<OIdentifiable> searchFromTarget(OFromClause target, OBinaryCompareOperator operator, Object rightValue,
      OCommandContext ctx, OExpression... args) {

    OIndex oIndex = searchForIndex(target, args);

    if (oIndex != null) {

      OExpression exp = args[0];
      return (OLuceneResultSet) oIndex.get(exp.toString());
    }
    return null;
  }

  @Override
  public long estimate(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {

    OLogManager.instance().info(this, "------>>> estimate");
    return 1L;
  }

  @Override
  public boolean canExecuteWithoutIndex(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {
    return false;
  }

  @Override
  public boolean allowsIndexedExecution(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {
    return false;
  }

  @Override
  public boolean shouldExecuteAfterSearch(OFromClause target, OBinaryCompareOperator operator, Object rightValue,
      OCommandContext ctx, OExpression... args) {
    return false;
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {

    //        OLogManager.instance().info(this, "ithis :: " + iThis);
    OLogManager.instance().info(this, "currentRecord :: " + iCurrentRecord);
    //    OLogManager.instance().info(this, "currentRes :: " + iCurrentResult);
    //    OLogManager.instance().info(this, "params :: " + iParams);
    //    OLogManager.instance().info(this, "ctx :: " + iContext);

    return iCurrentResult;
  }

  @Override
  public String getSyntax() {
    OLogManager.instance().info(this, "syntax");
    return null;
  }

  protected OIndex searchForIndex(OFromClause target, OExpression[] args) {

    // TODO Check if target is a class otherwise exception

    String fieldName = args[0].toString();

    OLogManager.instance().info(this, "query:: " + fieldName);

    Collection<? extends OIndex<?>> indexes = getDb().getMetadata().getIndexManager().getIndexes();
    OLogManager.instance().info(this, "iindexes:: " + indexes.size());
    for (OIndex<?> index : indexes) {
      if (index.getInternal() instanceof OLuceneFullTextIndex) {
        return index;
      }
    }
    return null;
  }

  protected ODatabaseDocumentInternal getDb() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

}
