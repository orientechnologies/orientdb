package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.builder.OLuceneQueryBuilder;
import com.orientechnologies.lucene.collections.OLuceneCompositeKey;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.lucene.query.OLuceneKeyAndMetadata;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.orient.core.sql.parser.OFromItem;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

/** Created by frank on 15/01/2017. */
public class OLuceneSearchOnIndexFunction extends OLuceneSearchFunctionTemplate {

  public static final String MEMORY_INDEX = "_memoryIndex";

  public static final String NAME = "search_index";

  public OLuceneSearchOnIndexFunction() {
    super(NAME, 2, 3);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Object execute(
      Object iThis,
      OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] params,
      OCommandContext ctx) {
    OElement element = iThis instanceof OElement ? (OElement) iThis : ((OResult) iThis).toElement();

    String indexName = (String) params[0];

    OLuceneFullTextIndex index = searchForIndex(ctx, indexName);

    if (index == null) return false;

    String query = (String) params[1];

    MemoryIndex memoryIndex = getOrCreateMemoryIndex(ctx);

    List<Object> key =
        index.getDefinition().getFields().stream()
            .map(s -> element.getProperty(s))
            .collect(Collectors.toList());

    for (IndexableField field : index.buildDocument(key).getFields()) {
      memoryIndex.addField(field, index.indexAnalyzer());
    }

    ODocument metadata = getMetadata(params);
    OLuceneKeyAndMetadata keyAndMetadata =
        new OLuceneKeyAndMetadata(
            new OLuceneCompositeKey(Arrays.asList(query)).setContext(ctx), metadata);

    return memoryIndex.search(index.buildQuery(keyAndMetadata)) > 0.0f;
  }

  private ODocument getMetadata(Object[] params) {

    if (params.length == 3) {
      return new ODocument().fromMap((Map<String, ?>) params[2]);
    }

    return OLuceneQueryBuilder.EMPTY_METADATA;
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
  public Iterable<OIdentifiable> searchFromTarget(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {

    OLuceneFullTextIndex index = searchForIndex(target, ctx, args);

    OExpression expression = args[1];
    String query = (String) expression.execute((OIdentifiable) null, ctx);
    if (index != null && query != null) {

      ODocument meta = getMetadata(args, ctx);

      List<OIdentifiable> luceneResultSet;
      try (Stream<ORID> rids =
          index
              .getInternal()
              .getRids(
                  new OLuceneKeyAndMetadata(
                      new OLuceneCompositeKey(Arrays.asList(query)).setContext(ctx), meta))) {
        luceneResultSet = rids.collect(Collectors.toList());
      }

      return luceneResultSet;
    }
    return Collections.emptyList();
  }

  private ODocument getMetadata(OExpression[] args, OCommandContext ctx) {
    if (args.length == 3) {
      return getMetadata(args[2], ctx);
    }
    return OLuceneQueryBuilder.EMPTY_METADATA;
  }

  @Override
  protected OLuceneFullTextIndex searchForIndex(
      OFromClause target, OCommandContext ctx, OExpression... args) {

    OFromItem item = target.getItem();
    OIdentifier identifier = item.getIdentifier();
    return searchForIndex(identifier.getStringValue(), ctx, args);
  }

  private OLuceneFullTextIndex searchForIndex(
      String className, OCommandContext ctx, OExpression... args) {

    String indexName = (String) args[0].execute((OIdentifiable) null, ctx);

    final ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    OIndex index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, className, indexName);

    if (index != null && index.getInternal() instanceof OLuceneFullTextIndex) {
      return (OLuceneFullTextIndex) index;
    }

    return null;
  }

  private OLuceneFullTextIndex searchForIndex(OCommandContext ctx, String indexName) {
    final ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, indexName);

    if (index != null && index.getInternal() instanceof OLuceneFullTextIndex) {
      return (OLuceneFullTextIndex) index;
    }

    return null;
  }

  @Override
  public Object getResult() {
    return super.getResult();
  }
}
