package com.orientechnologies.lucene.functions;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.builder.OLuceneQueryBuilder;
import com.orientechnologies.lucene.collections.OLuceneCompositeKey;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.lucene.query.OLuceneKeyAndMetadata;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OIndexableSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OFromClause;

import java.util.*;

import static com.orientechnologies.lucene.OLuceneCrossClassIndexFactory.LUCENE_CROSS_CLASS;

/**
 * This function uses the CrossClassIndex to search documents across all the Lucene indexes defined in a database
 * <p>
 * Created by frank on 19/02/2016.
 */
public class OLuceneCrossClassSearchFunction extends OSQLFunctionAbstract implements OIndexableSQLFunction {

  public static final String NAME = "SEARCH_CROSS";

  public OLuceneCrossClassSearchFunction() {
    super(NAME, 1, 1);
  }

  @Override
  public Iterable<OIdentifiable> searchFromTarget(OFromClause target, OBinaryCompareOperator operator, Object rightValue,
      OCommandContext ctx, OExpression... args) {

    OLuceneFullTextIndex fullTextIndex = searchForIndex();

    OExpression expression = args[0];
    String query = (String) expression.execute((OIdentifiable) null, ctx);

    if (fullTextIndex != null) {

    ODocument metadata = getMetadata(args);
      Set<OIdentifiable> luceneResultSet = fullTextIndex
          .get(new OLuceneKeyAndMetadata(new OLuceneCompositeKey(Arrays.asList(query)).setContext(ctx), metadata));

      return luceneResultSet;
    }
    return Collections.emptySet();
  }

  @Override
  public long estimate(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {
    return 1L;
  }

  @Override
  public boolean canExecuteInline(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {
    return false;
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

  protected OLuceneFullTextIndex searchForIndex() {

    Collection<? extends OIndex> indexes = getDb().getMetadata().getIndexManager().getIndexes();
    for (OIndex index : indexes) {
      if (index.getInternal() instanceof OLuceneFullTextIndex) {
        if (index.getAlgorithm().equalsIgnoreCase(LUCENE_CROSS_CLASS)) {
          return (OLuceneFullTextIndex) index;
        }
      }
    }
    return null;
  }

  protected ODatabaseDocumentInternal getDb() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  private ODocument getMetadata(OExpression[] args) {
    if (args.length == 2) {
      return new ODocument().fromJSON(args[1].toString());
    }
    return OLuceneQueryBuilder.EMPTY_METADATA;
  }

  @Override
  public Object execute(Object iThis, OIdentifiable currentRecord, Object currentResult, Object[] params,
      OCommandContext ctx) {

    OLuceneFullTextIndex fullTextIndex = searchForIndex();

    String query = (String) params[0];

    if (fullTextIndex != null) {

      ODocument metadata = getMetadata(params);

      Set<OIdentifiable> luceneResultSet = fullTextIndex
          .get(new OLuceneKeyAndMetadata(new OLuceneCompositeKey(Arrays.asList(query)).setContext(ctx), metadata));

      return luceneResultSet;
    }
    return Collections.emptySet();

  }

  private ODocument getMetadata(Object[] params) {

    if (params.length == 2) {
      return new ODocument()
          .fromMap((Map<String, ?>) params[1]);
    }

    return OLuceneQueryBuilder.EMPTY_METADATA;

  }

  @Override
  public String getSyntax() {
    OLogManager.instance().info(this, "syntax");
    return "SEARCH_CROSS('<lucene query>', {metadata})";
  }

}
