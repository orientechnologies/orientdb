package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import org.apache.lucene.index.memory.MemoryIndex;

/** Created by frank on 13/02/2017. */
public class OLuceneFunctionsUtils {
  public static final String MEMORY_INDEX = "_memoryIndex";

  protected static OLuceneFullTextIndex searchForIndex(OExpression[] args, OCommandContext ctx) {
    final String indexName = (String) args[0].execute((OIdentifiable) null, ctx);
    return getLuceneFullTextIndex(ctx, indexName);
  }

  protected static OLuceneFullTextIndex getLuceneFullTextIndex(
      final OCommandContext ctx, final String indexName) {
    final ODatabaseDocumentInternal documentDatabase =
        (ODatabaseDocumentInternal) ctx.getDatabase();
    documentDatabase.activateOnCurrentThread();
    final OMetadataInternal metadata = documentDatabase.getMetadata();

    final OLuceneFullTextIndex index =
        (OLuceneFullTextIndex)
            metadata.getIndexManagerInternal().getIndex(documentDatabase, indexName);
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

  public static String doubleEscape(final String s) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); ++i) {
      final char c = s.charAt(i);
      if (c == 92 || c == 43 || c == 45 || c == 33 || c == 40 || c == 41 || c == 58 || c == 94
          || c == 91 || c == 93 || c == 34 || c == 123 || c == 125 || c == 126 || c == 42 || c == 63
          || c == 124 || c == 38 || c == 47) {
        sb.append('\\');
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.toString();
  }
}
