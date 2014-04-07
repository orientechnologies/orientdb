package com.orientechnologies.lucene.manager;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import com.orientechnologies.lucene.OLuceneIndexType;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;

/**
 * Created by enricorisa on 24/03/14.
 */
public class OLuceneUniqueIndexManager extends OLuceneIndexManagerAbstract {

  @Override
  public IndexWriter createIndexWriter(Directory directory, ODocument metadata) throws IOException {
    Analyzer analyzer = new ClassicAnalyzer(Version.LUCENE_47);
    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
    return new IndexWriter(directory, iwc);
  }

  @Override
  public IndexWriter openIndexWriter(Directory directory) throws IOException {
    Analyzer analyzer = new ClassicAnalyzer(Version.LUCENE_47);
    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
    return new IndexWriter(directory, iwc);
  }

  @Override
  public void init() {

  }

  @Override
  public void create(String indexName, OIndexDefinition indexDefinition, String clusterIndexName,
      OStreamSerializer valueSerializer, boolean isAutomatic) {

  }

  @Override
  public void deleteWithoutLoad(String indexName) {

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
  public ORID getIdentity() {
    return null;
  }

  @Override
  public Iterator<Map.Entry> inverseIterator() {
    return null;
  }

  @Override
  public Iterator valuesIterator() {
    return null;
  }

  @Override
  public Iterator inverseValuesIterator() {
    return null;
  }

  @Override
  public Iterable<Object> keys() {
    return null;
  }

  @Override
  public Object get(Object key) {
    Query query = OLuceneIndexType.createExactQuery(index, key);
    Set<OIdentifiable> res = getResults(query, 1, null);
    return res.iterator().hasNext() ? res.iterator().next() : null;
  }

  private Set<OIdentifiable> getResults(Query query, int limit, Sort sort) {
    Set<OIdentifiable> result = new HashSet<OIdentifiable>();
    try {
      IndexSearcher searcher = getSearcher();

      TopDocs top = null;
      if (sort == null) {
        top = searcher.search(query, limit);
      } else {
        top = searcher.search(query, limit, sort);
      }
      ScoreDoc[] hits = top.scoreDocs;
      for (ScoreDoc score : hits) {
        Document ret = searcher.doc(score.doc);
        result.add(new ORecordId(ret.get(RID)));
      }
    } catch (IOException e) {

    } finally {
    }
    return result;
  }

  @Override
  public void put(Object key, Object value) {

    OIdentifiable oIdentifiable = (OIdentifiable) value;
    Document doc = new Document();
    doc.add(OLuceneIndexType.createField(RID, oIdentifiable, oIdentifiable.getIdentity().toString(), Field.Store.YES,
        Field.Index.NOT_ANALYZED_NO_NORMS));
    if (index.getFields().size() > 0) {
      for (String f : index.getFields()) {
        doc.add(OLuceneIndexType.createField(f, oIdentifiable, key, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
      }
    } else {
      doc.add(OLuceneIndexType.createField(KEY, oIdentifiable, key, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
    }

    try {
      addDocument(doc);
    } catch (Exception e) {
      e.printStackTrace();
    }
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
  public void getValuesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive, boolean ascSortOrder,
      ValuesTransformer transformer, ValuesResultListener valuesResultListener) {

    Query query = OLuceneIndexType.createRangeQuery(index, rangeFrom, rangeTo, fromInclusive, toInclusive);
    Sort sort = OLuceneIndexType.sort(query, index, ascSortOrder);

    Set<OIdentifiable> results = getResults(query, 100, sort);

    for (OIdentifiable i : results) {
      valuesResultListener.addResult(i);
    }
  }

  @Override
  public void getValuesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer,
      ValuesResultListener valuesResultListener) {

    Query query = OLuceneIndexType.createRangeQuery(index, fromKey, null, isInclusive, isInclusive);
    Sort sort = OLuceneIndexType.sort(query, index, ascSortOrder);
    Set<OIdentifiable> results = getResults(query, 100, sort);

    for (OIdentifiable i : results) {
      valuesResultListener.addResult(i);
    }
  }

  @Override
  public void getValuesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer,
      ValuesResultListener valuesResultListener) {

    Query query = OLuceneIndexType.createRangeQuery(index, null, toKey, isInclusive, isInclusive);
    Sort sort = OLuceneIndexType.sort(query, index, ascSortOrder);
    Set<OIdentifiable> results = getResults(query, 100, sort);

    for (OIdentifiable i : results) {
      valuesResultListener.addResult(i);
    }
  }

  @Override
  public void getEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer,
      EntriesResultListener entriesResultListener) {

  }

  @Override
  public void getEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer,
      EntriesResultListener entriesResultListener) {

  }

  @Override
  public void getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, boolean ascSortOrder,
      ValuesTransformer transformer, EntriesResultListener entriesResultListener) {

  }

  @Override
  public boolean hasRangeQuerySupport() {
    return true;
  }
}
