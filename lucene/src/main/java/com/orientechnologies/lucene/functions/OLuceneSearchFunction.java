package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.collections.OLuceneResultSet;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OIndexableSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OFromClause;

/**
 * Created by frank on 15/01/2017.
 */
public class OLuceneSearchFunction extends OSQLFunctionAbstract implements OIndexableSQLFunction {
  public static final String NAME = "SEARCH_INDEX";

  public OLuceneSearchFunction() {
    super(NAME, 2, 3);
  }

  @Override
  public String getName() {
    return NAME;
  }

  protected OIndex searchForIndex(OFromClause target, OExpression[] args) {

    // TODO:  Check if target is a class otherwise exception (seems useless)
    OMetadataInternal metadata = getDb().getMetadata();

//    OFromItem item = target.getItem();
//    OIdentifier identifier = item.getIdentifier();
//
//    OClass aClass = metadata.getSchema().getClass(identifier.getValue());

    String indexName = args[0].toString();

    OIndex<?> index = metadata.getIndexManager().getIndex(indexName);
    return index;
  }

  protected ODatabaseDocumentInternal getDb() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {
    System.out.println("execute::" + iCurrentRecord.getIdentity());
    return true;
  }

  @Override
  public String getSyntax() {
    return "SEARCH_INDEX( indexName, [ metdatada {} ] )";
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
      System.out.println("query= " + exp);

      if (args.length == 3) {
        ODocument metadata = new ODocument().fromJSON(args[2].toString());
      }
      OLuceneResultSet luceneResultSet = (OLuceneResultSet) index.get(exp.toString());

      System.out.println("luceneResultSet.size() = " + luceneResultSet.size());
      return luceneResultSet;
    }
    return null;
  }

  @Override
  public Object getResult() {
    System.out.println("getResult");
    return super.getResult();
  }

  @Override
  public long estimate(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {
    System.out.println("estimate");
    return 0;
  }

  @Override
  public boolean canExecuteWithoutIndex(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {
    System.out.println("canExecuteWithoutIndex");
    return true;
  }

  @Override
  public boolean allowsIndexedExecution(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {
    System.out.println("allowsIndexedExecution:: " + true);
    return true;
  }

  @Override
  public boolean shouldExecuteAfterSearch(OFromClause target, OBinaryCompareOperator operator, Object rightValue,
      OCommandContext ctx, OExpression... args) {
    System.out.println("shouldExecuteAfterSearch= " + target);
    return false;
  }
}
