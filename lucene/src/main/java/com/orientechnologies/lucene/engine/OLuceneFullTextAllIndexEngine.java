package com.orientechnologies.lucene.engine;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.lucene.OLuceneIndexFactory;
import com.orientechnologies.lucene.analyzer.OLucenePerFieldAnalyzerWrapper;
import com.orientechnologies.lucene.collections.OLuceneResultSet;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.lucene.query.QueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.*;

/**
 * Created by frank on 03/11/2016.
 */
public class OLuceneFullTextAllIndexEngine implements OLuceneIndexEngine {

  private final OStorage storage;
  private final String   indexName;

  public OLuceneFullTextAllIndexEngine(OStorage storage, String indexName) {

    this.storage = storage;
    this.indexName = indexName;

    OAbstractPaginatedStorage s = (OAbstractPaginatedStorage) storage;

  }

  @Override
  public void init(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata) {

  }

  @Override
  public void flush() {

  }

  @Override
  public void create(OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes, boolean nullPointerSupport,
      OBinarySerializer keySerializer, int keySize, Set<String> clustersToIndex, Map<String, String> engineProperties,
      ODocument metadata) {

  }

  @Override
  public void delete() {

  }

  @Override
  public void deleteWithoutLoad(String indexName) {

  }

  @Override
  public void load(String indexName, OBinarySerializer valueSerializer, boolean isAutomatic, OBinarySerializer keySerializer,
      OType[] keyTypes, boolean nullPointerSupport, int keySize, Map<String, String> engineProperties) {

  }

  @Override
  public boolean contains(Object key) {
    return false;
  }

  @Override
  public boolean remove(Object key) {
    return false;
  }

  @Override
  public void clear() {

  }

  @Override
  public void close() {

  }

  @Override
  public Object get(Object key) {

    Collection<? extends OIndex<?>> indexes = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getIndexManager().getIndexes();

    OLucenePerFieldAnalyzerWrapper globalAnalyzer = new OLucenePerFieldAnalyzerWrapper(new StandardAnalyzer());
    try {
      List<IndexReader> fullTextsIndexes = indexes.stream()
          .filter(index -> index.getAlgorithm().equalsIgnoreCase(OLuceneIndexFactory.LUCENE_ALGORITHM))
          .map(index -> (OLuceneFullTextIndex) index.getInternal())
          .peek(index -> globalAnalyzer.add((OLucenePerFieldAnalyzerWrapper) index.queryAnalyzer()))
          .map(index -> {
            try {
              return index.searcher().getIndexReader();
            } catch (IOException e) {
              e.printStackTrace();
            }
            return null;
          })
          .collect(toList());

      IndexReader indexReader = new MultiReader(fullTextsIndexes.toArray(new IndexReader[] {}));
      IndexSearcher searcher = new IndexSearcher(indexReader);

      Query query = new QueryParser("UNDEFINED", globalAnalyzer).parse(key.toString());

      QueryContext ctx = new QueryContext(null, searcher, query);
      return new OLuceneResultSet(this, ctx);

    } catch (IOException e) {
      e.printStackTrace();
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void put(Object key, Object value) {

  }

  @Override
  public boolean validatedPut(Object key, OIdentifiable value, Validator<Object, OIdentifiable> validator) {
    return false;
  }

  @Override
  public Object getFirstKey() {
    return null;
  }

  @Override
  public Object getLastKey() {
    return null;
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer) {
    return null;
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer transformer) {
    return null;
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer transformer) {
    return null;
  }

  @Override
  public OIndexCursor cursor(ValuesTransformer valuesTransformer) {
    return null;
  }

  @Override
  public OIndexCursor descCursor(ValuesTransformer valuesTransformer) {
    return null;
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    return null;
  }

  @Override
  public long size(ValuesTransformer transformer) {
    return 0;
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }

  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public String getName() {
    return indexName;
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    return false;
  }

  @Override
  public String getIndexNameByKey(Object key) {
    return null;
  }

  @Override
  public String indexName() {
    return null;
  }

  @Override
  public void onRecordAddedToResultSet(QueryContext queryContext, OContextualRecordId recordId, Document ret, ScoreDoc score) {

    recordId.setContext(new HashMap<String, Object>() {
      {
        put("score", score.score);
      }
    });

  }

  @Override
  public Document buildDocument(Object key, OIdentifiable value) {
    return null;
  }

  @Override
  public Query buildQuery(Object query) {
    return null;
  }

  @Override
  public Analyzer indexAnalyzer() {
    return null;
  }

  @Override
  public Analyzer queryAnalyzer() {
    return null;
  }

  @Override
  public boolean remove(Object key, OIdentifiable value) {
    return false;
  }

  @Override
  public IndexSearcher searcher() throws IOException {
    return null;
  }

  @Override
  public Object getInTx(Object key, OLuceneTxChanges changes) {
    return null;
  }

  @Override
  public long sizeInTx(OLuceneTxChanges changes) {
    return 0;
  }

  @Override
  public OLuceneTxChanges buildTxChanges() throws IOException {
    return null;
  }

  @Override
  public Query deleteQuery(Object key, OIdentifiable value) {
    return null;
  }

  @Override
  public void freeze(boolean throwException) {

  }

  @Override
  public void release() {

  }
}
