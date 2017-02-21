package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import org.apache.lucene.index.memory.MemoryIndex;

/**
 * Created by frank on 13/02/2017.
 */
public class OLuceneFunctionsUtils {

  public static final String MEMORY_INDEX = "_memoryIndex";

  protected static OLuceneFullTextIndex searchForIndex(OExpression[] args, OCommandContext ctx) {
    String indexName = (String) args[0].execute((OIdentifiable) null, ctx);

    return getLuceneFullTextIndex(ctx, indexName);
  }

  protected static OLuceneFullTextIndex getLuceneFullTextIndex(OCommandContext ctx, String indexName) {
    OMetadata metadata = ctx.getDatabase().activateOnCurrentThread().getMetadata();

    OLuceneFullTextIndex index = (OLuceneFullTextIndex) metadata.getIndexManager().getIndex(indexName);
    if (!(index instanceof OLuceneFullTextIndex)) {
      throw new IllegalArgumentException("Not a valid Lucene index:: " + indexName);
    }

    return index;
  }

  public static MemoryIndex getOrCreateMemoryIndex(OCommandContext ctx) {
    MemoryIndex memoryIndex = (MemoryIndex) ctx.getVariable(MEMORY_INDEX);
    if (memoryIndex == null) {
      memoryIndex = new MemoryIndex();
      ctx.setVariable(MEMORY_INDEX, memoryIndex);
    }

    memoryIndex.reset();
    return memoryIndex;
  }

}
