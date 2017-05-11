package com.orientechnologies.lucene.functions;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.collections.OLuceneCompositeKey;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.functions.OIndexableSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.parser.*;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.orientechnologies.lucene.functions.OLuceneFunctionsUtils.getOrCreateMemoryIndex;

/**
 * Created by frank on 15/01/2017.
 */
public class OLuceneSearchMoreLikeThisFunction extends OSQLFunctionAbstract implements OIndexableSQLFunction {

  public static final String NAME = "search_more";

  public OLuceneSearchMoreLikeThisFunction() {
    super(NAME, 1, 2);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Object execute(Object iThis,
      OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] params,
      OCommandContext ctx) {

    OResult result = (OResult) iThis;

    OElement element = result.toElement();

    String className = element.getSchemaType().get().getName();

    OLuceneFullTextIndex index = searchForIndex(ctx, className);

    if (index == null)
      return false;

    String query = (String) params[0];

    MemoryIndex memoryIndex = getOrCreateMemoryIndex(ctx);

    List<Object> key = index.getDefinition().getFields()
        .stream()
        .map(s -> element.getProperty(s))
        .collect(Collectors.toList());

    try {
      for (IndexableField field : index.buildDocument(key).getFields()) {
        memoryIndex.addField(field, index.indexAnalyzer());
      }

      return memoryIndex.search(index.buildQuery(query)) > 0.0f;
    } catch (ParseException e) {
      OLogManager.instance().error(this, "error occurred while building query", e);

    }
    return null;

  }

  @Override
  public String getSyntax() {
    return "SEARCH_MORE( [rids], [ metdatada {} ] )";
  }

  @Override
  public boolean filterResult() {
    return true;
  }

  @Override
  public Iterable<OIdentifiable> searchFromTarget(OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {

    OLuceneFullTextIndex index = searchForIndex(target, ctx);

    IndexSearcher searcher = index.searcher();

    OExpression expression = args[0];

    List<ORID> rids = (List<ORID>) expression.execute((OIdentifiable) null, ctx);

    List<String> ridsAsString = rids.stream()
        .map(r -> r.toString())
        .collect(Collectors.toList());

    String queryOthers =
        "RID:( " + QueryParser.escape(String.join(" ", ridsAsString)) + ")";

    Set<OIdentifiable> oIdentifiables = index.get(queryOthers);

    MoreLikeThis mlt = new MoreLikeThis(searcher.getIndexReader());

    mlt.setAnalyzer(index.queryAnalyzer());
    mlt.setFieldNames(index.getDefinition().getFields().toArray(new String[] {}));
    mlt.setMinTermFreq(1);
    mlt.setMinDocFreq(1);

    BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
    oIdentifiables.stream()
        .forEach(oi -> {

              index.getDefinition().getFields()
                  .stream().forEach(fieldName -> {
                try {
                  OElement element = oi.getRecord().load();
                  String property = element.getProperty(fieldName);
                  Query fieldQuery = mlt.like(fieldName, new StringReader(property));

                  queryBuilder.add(fieldQuery, BooleanClause.Occur.SHOULD);
                } catch (IOException e) {
                  //Fixme: do something usefull
                  e.printStackTrace();
                }

              });

            }

        );

    ridsAsString.stream()
        .forEach(rid ->
            {
              Term rid1 = new Term("RID", QueryParser.escape(rid));
              queryBuilder.add(new TermQuery(rid1), BooleanClause.Occur.MUST_NOT);
            }

        );

    Query mltQuery = queryBuilder.build();

    if (index != null) {

      if (args.length == 2) {
        ODocument metadata = new ODocument().fromJSON(args[2].toString());

        //TODO handle metadata
        System.out.println("metadata.toJSON() = " + metadata.toJSON());
        Set<OIdentifiable> luceneResultSet = index.get(mltQuery.toString());
      }

      Set<OIdentifiable> luceneResultSet = index.get(new OLuceneCompositeKey(Arrays.asList(mltQuery.toString())).setContext(ctx));

      return luceneResultSet;
    }
    return Collections.emptySet();

  }

  private OLuceneFullTextIndex searchForIndex(OFromClause target, OCommandContext ctx) {
    OFromItem item = target.getItem();

    String className = item.getIdentifier().getStringValue();

    return searchForIndex(ctx, className);
  }

  private OLuceneFullTextIndex searchForIndex(OCommandContext ctx, String className) {
    OMetadata dbMetadata = ctx.getDatabase().activateOnCurrentThread().getMetadata();

    List<OLuceneFullTextIndex> indices = dbMetadata
        .getSchema()
        .getClass(className)
        .getIndexes()
        .stream()
        .filter(idx -> idx instanceof OLuceneFullTextIndex)
        .map(idx -> (OLuceneFullTextIndex) idx)
        .collect(Collectors.toList());

    if (indices.size() > 1) {
      throw new IllegalArgumentException("too many full-text indices on given class: " + className);
    }

    return indices.size() == 0 ? null : indices.get(0);
  }

  @Override
  public Object getResult() {
    System.out.println("getResult");
    return super.getResult();
  }

  @Override
  public long estimate(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {
    OLuceneFullTextIndex index = searchForIndex(target, ctx);

    if (index != null)
      return index.getSize();
    return 0;

  }

  @Override
  public boolean canExecuteWithoutIndex(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {
    return allowsIndexedExecution(target, operator, rightValue, ctx, args);
  }

  @Override
  public boolean allowsIndexedExecution(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {

    OLuceneFullTextIndex index = searchForIndex(target, ctx);

    return index != null;
  }

  @Override
  public boolean shouldExecuteAfterSearch(OFromClause target, OBinaryCompareOperator operator, Object rightValue,
      OCommandContext ctx, OExpression... args) {
    return false;
  }

}
