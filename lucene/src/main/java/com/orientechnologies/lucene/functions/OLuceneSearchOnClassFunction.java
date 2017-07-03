package com.orientechnologies.lucene.functions;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.builder.OLuceneQueryBuilder;
import com.orientechnologies.lucene.collections.OLuceneCompositeKey;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.lucene.query.OLuceneKeyAndMetadata;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.parser.*;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

import java.util.*;
import java.util.stream.Collectors;

import static com.orientechnologies.lucene.functions.OLuceneFunctionsUtils.getOrCreateMemoryIndex;

/**
 * Created by frank on 15/01/2017.
 */
public class OLuceneSearchOnClassFunction extends OLuceneSearchFunctionTemplate {

  public static final String NAME = "search_class";

  public OLuceneSearchOnClassFunction() {
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

      ODocument metadata = getMetadata(params);
      OLuceneKeyAndMetadata keyAndMetadata = new OLuceneKeyAndMetadata(
          new OLuceneCompositeKey(Arrays.asList(query)).setContext(ctx), metadata);

      return memoryIndex.search(index.buildQuery(keyAndMetadata)) > 0.0f;
    } catch (ParseException e) {
      OLogManager.instance().error(this, "error occurred while building query", e);

    }
    return null;

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

    OLuceneFullTextIndex index = searchForIndex(target, ctx);

    OExpression expression = args[0];
    String query = (String) expression.execute((OIdentifiable) null, ctx);

    if (index != null) {

      ODocument metadata = getMetadata(args);

      Set<OIdentifiable> luceneResultSet = index
          .get(new OLuceneKeyAndMetadata(new OLuceneCompositeKey(Arrays.asList(query)).setContext(ctx), metadata));

      return luceneResultSet;
    }
    return Collections.emptySet();

  }

  private ODocument getMetadata(OExpression[] args) {
    if (args.length == 2) {
      return new ODocument().fromJSON(args[1].toString());
    }
    return OLuceneQueryBuilder.EMPTY_METADATA;
  }

  @Override
  protected OLuceneFullTextIndex searchForIndex(OFromClause target, OCommandContext ctx, OExpression... args) {
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

}
