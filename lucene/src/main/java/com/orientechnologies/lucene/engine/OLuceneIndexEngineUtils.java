package com.orientechnologies.lucene.engine;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.builder.OLuceneQueryBuilder;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by frank on 04/05/2017.
 */
public class OLuceneIndexEngineUtils {

  public static void sendTotalHits(String indexName, OCommandContext context, int totalHits) {
    if (context != null) {

      if (context.getVariable("totalHits") == null) {
        context.setVariable("totalHits", totalHits);
      } else {
        context.setVariable("totalHits", null);
      }
      context.setVariable((indexName + ".totalHits").replace(".", "_"), totalHits);
    }

  }

  public static void sendLookupTime(String indexName, OCommandContext context, final TopDocs docs, final Integer limit,
      long startFetching) {
    if (context != null) {

      final long finalTime = System.currentTimeMillis() - startFetching;
      context.setVariable((indexName + ".lookupTime").replace(".", "_"), new HashMap<String, Object>() {
        {
          put("limit", limit);
          put("totalTime", finalTime);
          put("totalHits", docs.totalHits);
          put("returnedHits", docs.scoreDocs.length);
          if (!Float.isNaN(docs.getMaxScore())) {
            put("maxScore", docs.getMaxScore());
          }

        }
      });
    }
  }

  public static ODocument getMetadataFromIndex(IndexWriter writer) {

    IndexReader reader = null;
    IndexSearcher searcher = null;
    try {
      reader = DirectoryReader.open(writer);

      searcher = new IndexSearcher(reader);

      final TopDocs topDocs = searcher.search(new TermQuery(new Term("_CLASS", "JSON_METADATA")), 1);

      final Document metaDoc = searcher.doc(topDocs.scoreDocs[0].doc);

      return new ODocument().fromJSON(metaDoc.get("_JSON"));
    } catch (IOException e) {
      OLogManager.instance().error(OLuceneIndexEngineAbstract.class, "Error while retrieving index metadata", e);
    } finally {
      if (reader != null)
        try {
          reader.close();
        } catch (IOException e) {
          OLogManager.instance().error(OLuceneIndexEngineAbstract.class, "Error while retrieving index metadata", e);

        }

    }

    return OLuceneQueryBuilder.EMPTY_METADATA;

  }
}
