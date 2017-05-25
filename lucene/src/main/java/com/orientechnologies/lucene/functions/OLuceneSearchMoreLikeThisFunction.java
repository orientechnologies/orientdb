package com.orientechnologies.lucene.functions;

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
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.orient.core.sql.parser.OFromItem;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by frank on 15/01/2017.
 */
public class OLuceneSearchMoreLikeThisFunction extends OSQLFunctionAbstract implements OIndexableSQLFunction {

  public static final String NAME = "search_more";

  public OLuceneSearchMoreLikeThisFunction() {
    super(OLuceneSearchMoreLikeThisFunction.NAME, 1, 2);
  }

  @Override
  public String getName() {
    return OLuceneSearchMoreLikeThisFunction.NAME;
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

    OLuceneFullTextIndex index = this.searchForIndex(ctx, className);

    System.out.println("element = " + element.toJSON());
    if (index == null)
      return false;

    List<ORID> query = (List<ORID>) params[0];

//    MemoryIndex memoryIndex = getOrCreateMemoryIndex(ctx);
//
//    List<Object> key = index.getDefinition().getFields()
//        .stream()
//        .map(s -> element.getProperty(s))
//        .collect(Collectors.toList());
//
//    try {
//      for (IndexableField field : index.buildDocument(key).getFields()) {
//        memoryIndex.addField(field, index.indexAnalyzer());
//      }
//
//      return memoryIndex.search(index.buildQuery(query)) > 0.0f;
//    } catch (ParseException e) {
//      OLogManager.instance().error(this, "error occurred while building query", e);
//
//    }
    return true;

  }

  @Override
  public String getSyntax() {
    return "SEARCH_MORE( [rids], [ metdatada {} ] )";
  }

  @Override
  public Iterable<OIdentifiable> searchFromTarget(OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {

    System.out.println("target = " + target);
    System.out.println("rightValue = " + rightValue);
    OLuceneFullTextIndex index = this.searchForIndex(target, ctx);

    if (index == null)
      return Collections.emptySet();

    IndexSearcher searcher = index.searcher();

    OExpression expression = args[0];

    ODocument metadata = this.parseMetadata(args);

    List<String> ridsAsString = this.parseRids(ctx, expression);

    Set<OIdentifiable> others = index.get("RID:( " + QueryParserBase.escape(String.join(" ", ridsAsString)) + ")");

    MoreLikeThis mlt = this.buildMoreLikeThis(index, searcher, metadata);

    Builder queryBuilder = new Builder();

    this.excludeOtherFromResults(ridsAsString, queryBuilder);

    this.addLikeQueries(index, others, mlt, queryBuilder);

    Query mltQuery = queryBuilder.build();

    Set<OIdentifiable> luceneResultSet = index.get(new OLuceneCompositeKey(Arrays.asList(mltQuery.toString())).setContext(ctx));

    System.out.println("luceneResultSet.size() = " + luceneResultSet.size());
    return luceneResultSet;

  }

  private List<String> parseRids(OCommandContext ctx, OExpression expression) {
    List<ORID> rids = (List<ORID>) expression.execute((OIdentifiable) null, ctx);

    return rids.stream()
        .map(r -> r.toString())
        .collect(Collectors.toList());
  }

  private ODocument parseMetadata(OExpression[] args) {
    ODocument metadata = new ODocument();
    if (args.length == 2) {
      metadata.fromJSON(args[1].toString());
    }
    return metadata;
  }

  private MoreLikeThis buildMoreLikeThis(OLuceneFullTextIndex index, IndexSearcher searcher, ODocument metadata) {

    MoreLikeThis mlt = new MoreLikeThis(searcher.getIndexReader());

    mlt.setAnalyzer(index.queryAnalyzer());

    mlt.setFieldNames(Optional.ofNullable(metadata.<List<String>>getProperty("fieldNames"))
        .orElse(index.getDefinition().getFields()).toArray(new String[] {}));

    mlt.setMaxQueryTerms(Optional.ofNullable(metadata.<Integer>getProperty("maxQueryTerms"))
        .orElse(MoreLikeThis.DEFAULT_MAX_QUERY_TERMS));

    mlt.setMinTermFreq(Optional.ofNullable(metadata.<Integer>getProperty("minTermFreq"))
        .orElse(MoreLikeThis.DEFAULT_MIN_TERM_FREQ));

    mlt.setMaxDocFreq(Optional.ofNullable(metadata.<Integer>getProperty("maxDocFreq"))
        .orElse(MoreLikeThis.DEFAULT_MAX_DOC_FREQ));

    mlt.setMinDocFreq(Optional.ofNullable(metadata.<Integer>getProperty("minDocFreq"))
        .orElse(MoreLikeThis.DEFAULT_MAX_DOC_FREQ));

    mlt.setBoost(Optional.ofNullable(metadata.<Boolean>getProperty("boost"))
        .orElse(MoreLikeThis.DEFAULT_BOOST));

    mlt.setBoostFactor(Optional.ofNullable(metadata.<Float>getProperty("boostFactor"))
        .orElse(1f));

    mlt.setMaxWordLen(Optional.ofNullable(metadata.<Integer>getProperty("maxWordLen"))
        .orElse(MoreLikeThis.DEFAULT_MAX_WORD_LENGTH));

    mlt.setMinWordLen(Optional.ofNullable(metadata.<Integer>getProperty("minWordLen"))
        .orElse(MoreLikeThis.DEFAULT_MIN_WORD_LENGTH));

    mlt.setMaxNumTokensParsed(Optional.ofNullable(metadata.<Integer>getProperty("maxNumTokensParsed"))
        .orElse(MoreLikeThis.DEFAULT_MAX_NUM_TOKENS_PARSED));

    mlt.setStopWords((Set<?>) Optional.ofNullable(metadata.getProperty("stopWords"))
        .orElse(MoreLikeThis.DEFAULT_STOP_WORDS));

    return mlt;
  }

  private void addLikeQueries(OLuceneFullTextIndex index, Set<OIdentifiable> others, MoreLikeThis mlt,
      Builder queryBuilder) {
    others.stream()
        .forEach(oi -> Arrays.stream(mlt.getFieldNames())
            .forEach(fieldName -> {
              OElement element = oi.getRecord().load();
              String property = element.getProperty(fieldName);

              try {
                Query fieldQuery = mlt.like(fieldName, new StringReader(property));
                if (!fieldQuery.toString().isEmpty())
                  queryBuilder.add(fieldQuery, Occur.SHOULD);
              } catch (IOException e) {
                e.printStackTrace();
              }

            })

        );
  }

  private void excludeOtherFromResults(List<String> ridsAsString, Builder queryBuilder) {
    ridsAsString.stream()
        .forEach(rid ->
            queryBuilder.add(new TermQuery(new Term("RID", QueryParser.escape(rid))), Occur.MUST_NOT)

        );
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
  public long estimate(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {
    OLuceneFullTextIndex index = this.searchForIndex(target, ctx);

    if (index != null)
      return index.getSize();
    return 0;

  }

  @Override
  public boolean canExecuteInline(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {

    System.out.println("canExecuteInline = " + false);
    return false;
  }

  @Override
  public boolean allowsIndexedExecution(OFromClause target, OBinaryCompareOperator operator, Object rightValue, OCommandContext ctx,
      OExpression... args) {

    OLuceneFullTextIndex index = this.searchForIndex(target, ctx);

    return index != null;
  }

  @Override
  public boolean shouldExecuteAfterSearch(OFromClause target, OBinaryCompareOperator operator, Object rightValue,

      OCommandContext ctx, OExpression... args) {

    System.out.println("shouldExecuteAfterSearch= " + false);
    return false;
  }

}
