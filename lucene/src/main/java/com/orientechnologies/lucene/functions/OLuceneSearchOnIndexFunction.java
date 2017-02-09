package com.orientechnologies.lucene.functions;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.functions.OIndexableSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.orient.core.sql.parser.ParseException;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by frank on 15/01/2017.
 */
public class OLuceneSearchOnIndexFunction extends OSQLFunctionAbstract implements OIndexableSQLFunction {

  public static final String MEMORY_INDEX = "_memoryIndex";

  public static final String NAME = "SEARCH_INDEX";

  public OLuceneSearchOnIndexFunction() {
    super(NAME, 2, 3);
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
    OElement doc = result.toElement();

    String indexName = (String) params[0];
    OLuceneFullTextIndex index = getLuceneFullTextIndex(ctx, indexName);

    String query = (String) params[1];

    MemoryIndex memoryIndex = getMemoryIndex(ctx);

    List<Object> key = index.getDefinition().getFields()
        .stream()
        .map(s -> doc.getProperty(s))
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

  private MemoryIndex getMemoryIndex(OCommandContext ctx) {
    MemoryIndex memoryIndex = (MemoryIndex) ctx.getVariable(MEMORY_INDEX);
    if (memoryIndex == null) {
      memoryIndex = new MemoryIndex();
      ctx.setVariable(MEMORY_INDEX, memoryIndex);
    }
    memoryIndex.reset();
    return memoryIndex;
  }

  @Override
  public String getSyntax() {
    return "SEARCH_INDEX( indexName, [ metdatada {} ] )";
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

    OLuceneFullTextIndex index = searchForIndex(args, ctx);

    if (index != null) {

      if (args.length == 3) {
        ODocument metadata = new ODocument().fromJSON(args[2].toString());

        System.out.println("metadata.toJSON() = " + metadata.toJSON());
      }

      OExpression query = args[1];

      Set<OIdentifiable> luceneResultSet = index.get(query.toString());

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
    return false;
  }

  protected OLuceneFullTextIndex searchForIndex(OExpression[] args, OCommandContext ctx) {
    String indexName = (String) args[0].execute((OIdentifiable) null, ctx);

    return getLuceneFullTextIndex(ctx, indexName);
  }

  private OLuceneFullTextIndex getLuceneFullTextIndex(OCommandContext ctx, String indexName) {
    OMetadata metadata = ctx.getDatabase().activateOnCurrentThread().getMetadata();
    if (!(metadata.getIndexManager().getIndex(indexName) instanceof OLuceneFullTextIndex)) {
      throw new IllegalArgumentException("Not a valid Lucene index:: " + indexName);
    }

    OLuceneFullTextIndex index = (OLuceneFullTextIndex) metadata.getIndexManager().getIndex(indexName);

    return index;
  }

}
