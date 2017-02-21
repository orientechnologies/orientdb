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
import com.orientechnologies.orient.core.sql.parser.*;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

import java.util.*;
import java.util.stream.Collectors;

import static com.orientechnologies.lucene.functions.OLuceneFunctionsUtils.getOrCreateMemoryIndex;

/**
 * Created by frank on 15/01/2017.
 */
public class OLuceneSearchOnFieldsFunction extends OSQLFunctionAbstract implements OIndexableSQLFunction {

  public static final String NAME = "search_fields";

  public static final ODocument EMPTY_METADATA = new ODocument();

  public OLuceneSearchOnFieldsFunction() {
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

    OElement element = result.toElement();

    String className = element.getSchemaType().get().getName();
    String[] fieldNames = ((String) params[0]).split(",");

    OLuceneFullTextIndex index = searchForIndex(className, ctx, fieldNames);

    if (index == null)
      return false;

    String query = (String) params[1];

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

    String[] fieldNames = ((String) args[0].execute((OIdentifiable) null, ctx)).split(",");

    OFromItem item = target.getItem();
    String className = item.getIdentifier().getStringValue();

    OLuceneFullTextIndex index = searchForIndex(className, ctx, fieldNames);

    OExpression query = args[1];
    if (index != null) {

      if (args.length == 3) {
        ODocument metadata = new ODocument().fromJSON(args[2].toString());

        //TODO handle metadata
        System.out.println("metadata.toJSON() = " + metadata.toJSON());
        Set<OIdentifiable> luceneResultSet = index.get(query.toString());
      }

      Set<OIdentifiable> luceneResultSet = index.get(query.toString());

      return luceneResultSet;
    }
    return Collections.emptySet();

  }

  private OLuceneFullTextIndex searchForIndex(String className, OCommandContext ctx, String[] fieldNames) {
    OMetadata dbMetadata = ctx.getDatabase().activateOnCurrentThread().getMetadata();

    List<OLuceneFullTextIndex> indices = dbMetadata
        .getIndexManager()
        .getClassIndexes(className)
        .stream()
        .filter(idx -> idx instanceof OLuceneFullTextIndex)
        .map(idx -> (OLuceneFullTextIndex) idx)
        .filter(idx -> intersect(idx.getDefinition().getFields(), Arrays.asList(fieldNames)))
        .collect(Collectors.toList());

    if (indices.size() > 1) {
      throw new IllegalArgumentException("too many indices matching given field name: " + String.join(",", fieldNames));
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

  public <T> List<T> intersection(List<T> list1, List<T> list2) {
    List<T> list = new ArrayList<T>();

    for (T t : list1) {
      if (list2.contains(t)) {
        list.add(t);
      }
    }

    return list;
  }

  public <T> boolean intersect(List<T> list1, List<T> list2) {

    for (T t : list1) {
      if (list2.contains(t)) {
        return true;
      }
    }

    return false;
  }

}
