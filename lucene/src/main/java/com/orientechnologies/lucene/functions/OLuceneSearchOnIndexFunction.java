package com.orientechnologies.lucene.functions;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.functions.OIndexableSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.parser.*;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by frank on 15/01/2017.
 */
public class OLuceneSearchOnIndexFunction extends OSQLFunctionAbstract implements OIndexableSQLFunction {

  public static final String MEMORY_INDEX = "_memoryIndex";

  public static final String NAME = "search_index";

  public static final ODocument EMPTY_METADATA = new ODocument();

  public OLuceneSearchOnIndexFunction() {
    super(NAME, 2, 3);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] params, OCommandContext ctx) {

    OResult result = (OResult) iThis;
    OElement doc = result.toElement();

    String indexName = (String) params[0];
    OLuceneFullTextIndex index = searchForIndex(doc.getSchemaType().get().getName(), ctx, indexName);

    String query = (String) params[1];

    MemoryIndex memoryIndex = getOrCreateMemoryIndex(ctx);

    List<Object> key = index.getDefinition().getFields().stream().map(s -> doc.getProperty(s)).collect(Collectors.toList());

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

  private MemoryIndex getOrCreateMemoryIndex(OCommandContext ctx) {
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
  public Iterable<OIdentifiable> searchFromTarget(OFromClause target, OBinaryCompareOperator operator, Object rightValue,
      OCommandContext ctx, OExpression... args) {

    String indexName = (String) args[0].execute((OIdentifiable) null, ctx);

    OLuceneFullTextIndex index = searchForIndex(target, ctx, indexName);

    OExpression query = args[1];
    Object queryVal = query.execute((OIdentifiable) null, ctx);
    String queryValue = queryVal == null ? null : String.valueOf(queryVal);
    if (index != null) {

      if (args.length == 3) {
        ODocument metadata = new ODocument().fromJSON(args[2].toString());

        //TODO handle metadata
        System.out.println("metadata.toJSON() = " + metadata.toJSON());
        Set<OIdentifiable> luceneResultSet = index.get(queryValue);
      }

      Set<OIdentifiable> luceneResultSet = index.get(queryValue);

      return luceneResultSet;
    }
    return Collections.emptySet();
  }

  protected OLuceneFullTextIndex searchForIndex(OFromClause target, OCommandContext ctx, String indexName) {

    OFromItem item = target.getItem();
    OIdentifier identifier = item.getIdentifier();
    return searchForIndex(identifier.getStringValue(), ctx, indexName);
  }

  protected OLuceneFullTextIndex searchForIndex(String className, OCommandContext ctx, String indexName) {

    OIndex<?> index = ctx.getDatabase().getMetadata().getIndexManager().getClassIndex(className, indexName);

    if (index != null && index.getInternal() instanceof OLuceneFullTextIndex) {
      return (OLuceneFullTextIndex) index;
    }

    //TODO maybe better to trhow
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

}
