package com.orientechnologies.lucene.manager;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import com.orientechnologies.lucene.OLuceneIndexType;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by enricorisa on 21/03/14.
 */
public class OLuceneNotUniqueIndexManager extends OLuceneIndexManagerAbstract<Set<OIdentifiable>> {

  public OLuceneNotUniqueIndexManager() {
  }

  @Override
  public IndexWriter createIndexWriter(Directory directory, ODocument metadata) throws IOException {
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    return new IndexWriter(directory, iwc);
  }

  @Override
  public IndexWriter openIndexWriter(Directory directory) throws IOException {
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);

    iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
    return new IndexWriter(directory, iwc);
  }

  @Override
  public void init() {

  }

  @Override
  public void flush() {

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
  public Iterator<Map.Entry<Object, Set<OIdentifiable>>> inverseIterator() {
    return null;
  }

  @Override
  public Iterator<Set<OIdentifiable>> valuesIterator() {
    return null;
  }

  @Override
  public Iterator<Set<OIdentifiable>> inverseValuesIterator() {
    return null;
  }

  @Override
  public Iterable<Object> keys() {
    return null;
  }

  @Override
  public Set<OIdentifiable> get(Object key) {
    Query query = OLuceneIndexType.createExactQuery(index, key);
    Set<OIdentifiable> results = getResults(query);
    return results;
  }

  private Set<OIdentifiable> getResults(Query query) {
    Set<OIdentifiable> results = new HashSet<OIdentifiable>();
    try {
      IndexSearcher searcher = getSearcher();

      TopDocs docs = searcher.search(query, 100);
      ScoreDoc[] hits = docs.scoreDocs;
      for (ScoreDoc score : hits) {
        Document ret = searcher.doc(score.doc);
        results.add(new ORecordId(ret.get(RID)));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return results;
  }

  private void addToResultEntries(Query query, String fieldName, EntriesResultListener entriesResultListener) {

    try {

      IndexSearcher searcher = getSearcher();
      TopDocs docs = searcher.search(query, 100);
      ScoreDoc[] hits = docs.scoreDocs;
      for (ScoreDoc score : hits) {
        Document ret = searcher.doc(score.doc);
        final ODocument document = new ODocument();
        document.field(KEY, ret.get(fieldName));
        document.field(RID, ret.get(RID));
        document.unsetDirty();
        entriesResultListener.addResult(document);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  @Override
  public void put(Object key, Set<OIdentifiable> value) {
    Set<OIdentifiable> container = (Set<OIdentifiable>) value;
    for (OIdentifiable oIdentifiable : container) {
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
      ValuesTransformer<Set<OIdentifiable>> transformer, ValuesResultListener valuesResultListener) {
    Query query = OLuceneIndexType.createRangeQuery(index, rangeFrom, rangeTo, fromInclusive, toInclusive);
    Set<OIdentifiable> results = getResults(query);

    for (OIdentifiable i : results) {
      valuesResultListener.addResult(i);
    }

  }

  @Override
  public void getValuesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer<Set<OIdentifiable>> transformer, ValuesResultListener valuesResultListener) {

  }

  @Override
  public void getValuesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer<Set<OIdentifiable>> transformer, ValuesResultListener valuesResultListener) {

  }

  @Override
  public void getEntriesMajor(Object fromKey, boolean isInclusive, ValuesTransformer<Set<OIdentifiable>> transformer,
      EntriesResultListener entriesResultListener) {

  }

  @Override
  public void getEntriesMinor(Object toKey, boolean isInclusive, ValuesTransformer<Set<OIdentifiable>> transformer,
      EntriesResultListener entriesResultListener) {

  }

  @Override
  public void getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive,
      ValuesTransformer<Set<OIdentifiable>> transformer, EntriesResultListener entriesResultListener) {

    Query query = OLuceneIndexType.createRangeQuery(index, iRangeFrom, iRangeTo, iInclusive, iInclusive);
    addToResultEntries(query, index.getFields().iterator().next(), entriesResultListener);

  }

  @Override
  public boolean hasRangeQuerySupport() {
    return true;
  }
}
