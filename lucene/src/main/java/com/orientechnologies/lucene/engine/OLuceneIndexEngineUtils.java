package com.orientechnologies.lucene.engine;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.exception.OLuceneIndexException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

/** Created by frank on 04/05/2017. */
public class OLuceneIndexEngineUtils {

  public static void sendTotalHits(String indexName, OCommandContext context, long totalHits) {
    if (context != null) {

      if (context.getVariable("totalHits") == null) {
        context.setVariable("totalHits", totalHits);
      } else {
        context.setVariable("totalHits", null);
      }
      context.setVariable((indexName + ".totalHits").replace(".", "_"), totalHits);
    }
  }

  public static void sendLookupTime(
      String indexName,
      OCommandContext context,
      final TopDocs docs,
      final Integer limit,
      long startFetching) {
    if (context != null) {

      final long finalTime = System.currentTimeMillis() - startFetching;
      context.setVariable(
          (indexName + ".lookupTime").replace(".", "_"),
          new HashMap<String, Object>() {
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

  public static Document retrieveIndexMetadata(IndexWriter writer) {

    IndexReader reader = null;
    IndexSearcher searcher = null;
    try {
      reader = DirectoryReader.open(writer);

      searcher = new IndexSearcher(reader);

      final TopDocs topDocs =
          searcher.search(new TermQuery(new Term("_CLASS", "JSON_METADATA")), 1);

      final Document metaDoc = searcher.doc(topDocs.scoreDocs[0].doc);
      return metaDoc;
    } catch (IOException e) {
      //      OLogManager.instance().error(OLuceneIndexEngineAbstract.class, "Error while retrieving
      // index metadata", e);
      throw OException.wrapException(
          new OLuceneIndexException("unable to retrieve metadata document from index"), e);
    } finally {
      if (reader != null)
        try {
          reader.close();
        } catch (IOException e) {
          OLogManager.instance()
              .error(OLuceneIndexEngineAbstract.class, "Error while retrieving index metadata", e);
        }
    }
  }

  public static List<SortField> buildSortFields(ODocument metadata) {
    List<Map<String, Object>> sortConf =
        Optional.ofNullable(metadata.<List<Map<String, Object>>>getProperty("sort"))
            .orElse(Collections.emptyList());

    final List<SortField> fields =
        sortConf.stream().map(d -> buildSortField(d)).collect(Collectors.toList());

    return fields;
  }

  /**
   * Builds {@link SortField} from a configuration {@link ODocument}
   *
   * @param conf
   * @return
   */
  public static SortField buildSortField(ODocument conf) {

    return buildSortField(conf.toMap());
  }

  /**
   * Builds a {@link SortField} from a configuration map. The map can contains up to three fields:
   * field (name), reverse (true/false) and type {@link SortField.Type}.
   *
   * @param conf
   * @return
   */
  public static SortField buildSortField(Map<String, Object> conf) {

    final String field = Optional.ofNullable((String) conf.get("field")).orElse(null);
    final String type =
        Optional.ofNullable(((String) conf.get("type")).toUpperCase())
            .orElse(SortField.Type.STRING.name());
    final Boolean reverse = Optional.ofNullable((Boolean) conf.get("reverse")).orElse(false);

    SortField sortField = new SortField(field, SortField.Type.valueOf(type), reverse);

    return sortField;
  }

  public static ODocument getMetadataFromIndex(IndexWriter writer) {

    final Document metaDoc = retrieveIndexMetadata(writer);

    return new ODocument().fromJSON(metaDoc.get("_META_JSON"));
  }

  public static OIndexDefinition getIndexDefinitionFromIndex(IndexWriter writer) {

    final Document metaDoc = retrieveIndexMetadata(writer);

    final ODocument defAsJson = new ODocument().fromJSON(metaDoc.get("_DEF_JSON"));

    final String defClassName = metaDoc.get("_DEF_CLASS_NAME");

    try {
      final Class<?> indexDefClass = Class.forName(defClassName);
      OIndexDefinition indexDefinition =
          (OIndexDefinition) indexDefClass.getDeclaredConstructor().newInstance();
      indexDefinition.fromStream(defAsJson);
      return indexDefinition;

    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InvocationTargetException
        | InstantiationException
        | IllegalAccessException e) {
      throw OException.wrapException(
          new OLuceneIndexException("Error during deserialization of index definition"), e);
    }
  }
}
