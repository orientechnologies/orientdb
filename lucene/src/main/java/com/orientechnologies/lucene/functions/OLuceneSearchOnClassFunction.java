package com.orientechnologies.lucene.functions;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.collections.OLuceneCompositeKey;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.parser.*;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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

      return memoryIndex.search(index.buildQuery(query)) > 0.0f;
    } catch (ParseException e) {
      OLogManager.instance().error(this, "error occurred while building query", e);

    }
    return null;

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

      if (args.length == 2) {
        ODocument metadata = new ODocument().fromJSON(args[1].toString());

        //TODO handle metadata
        System.out.println("metadata.toJSON() = " + metadata.toJSON());
        Set<OIdentifiable> luceneResultSet = index.get(query.toString());
      }

      Set<OIdentifiable> luceneResultSet = index.get(new OLuceneCompositeKey(Arrays.asList(query)).setContext(ctx));

      return luceneResultSet;
    }
    return Collections.emptySet();

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
