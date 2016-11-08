package com.orientechnologies.lucene.functions;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.OLuceneIndexFactory;
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

  public static final String NAME = "SEARCH";

  public OLuceneSearchFunction() {
    super(NAME, 1, 1);
  }

  @Override
  public Iterable<OIdentifiable> searchFromTarget(OFromClause target, OBinaryCompareOperator operator, Object rightValue,
      OCommandContext ctx, OExpression... args) {

    OIndex oIndex = searchForIndex();

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
    return false;
  }

  protected OIndex searchForIndex() {

    // TODO Check if target is a class otherwise exception

    Collection<? extends OIndex<?>> indexes = getDb().getMetadata().getIndexManager().getIndexes();
    for (OIndex<?> index : indexes) {
      if (index.getInternal() instanceof OLuceneFullTextIndex) {
        if (index.getAlgorithm().equalsIgnoreCase(OLuceneIndexFactory.LUCENE_ALL_ALGORITHM)) {
          return index;
        }
      }
    }
    return null;
  }

  protected ODatabaseDocumentInternal getDb() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {

    OIndex index = searchForIndex();
    if (index != null) {

      return index.get(iParams[0]);
    }
    return null;

  }

  @Override
  public String getSyntax() {
    OLogManager.instance().info(this, "syntax");
    return null;
  }

}
