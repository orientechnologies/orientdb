package com.orientechnologies.lucene.functions;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.collections.LuceneResultSet;
import com.orientechnologies.lucene.index.OLuceneFullTextExpIndex;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.functions.OIndexableSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.Set;

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

    OLogManager.instance().info(this, "target :: " + target);
    OLogManager.instance().info(this, "operator :: " + operator);
    OLogManager.instance().info(this, "right :: " + rightValue);
    OLogManager.instance().info(this, "ctx:: " + ctx);
    OLogManager.instance().info(this, "args :: " + args);

    return results(target, args, ctx);
  }

  @Override
  public long estimate(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {

    OLogManager.instance().info(this, "------>>> estimate");
    return 1L;
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {

    //    OLogManager.instance().info(this, "ithis :: " + iThis);
    //    OLogManager.instance().info(this, "currentRecord :: " + iCurrentRecord);
    //    OLogManager.instance().info(this, "currentRes :: " + iCurrentResult);
    //    OLogManager.instance().info(this, "params :: " + iParams);
    //    OLogManager.instance().info(this, "ctx :: " + iContext);

    return null;
  }

  @Override
  public String getSyntax() {
    OLogManager.instance().info(this, "syntax");
    return null;
  }

  protected OIndex searchForIndex(OFromClause target, OExpression[] args) {

    // TODO Check if target is a class otherwise exception

    OFromItem item = target.getItem();
    OBaseIdentifier identifier = item.getIdentifier();
    String fieldName = args[0].toString();

    Set<OIndex<?>> indexes = getDb().getMetadata().getIndexManager().getClassInvolvedIndexes(identifier.toString(), fieldName);
    for (OIndex<?> index : indexes) {
      if (index.getInternal() instanceof OLuceneFullTextExpIndex) {
        return index;
      }
    }
    return null;
  }

  protected ODatabaseDocumentInternal getDb() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  protected LuceneResultSet results(OFromClause target, OExpression[] args, OCommandContext ctx) {
    OIndex oIndex = searchForIndex(target, args);
    if (oIndex != null) {
      OLogManager.instance().info(this, "index:: " + oIndex);

      return (LuceneResultSet) oIndex.get(args);
    }
    return null;
  }
}
