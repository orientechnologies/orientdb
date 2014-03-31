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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import com.orientechnologies.lucene.OLuceneIndexType;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by enricorisa on 22/03/14.
 */
public class OLuceneFullTextIndexManager extends OLuceneIndexManagerAbstract {

  public OLuceneFullTextIndexManager() {
  }

  @Override
  public IndexWriter createIndexWriter(Directory directory, ODocument metadata) throws IOException {

    Analyzer analyzer = getAnalyzer(metadata);
    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
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
    Set<OIdentifiable> results = new HashSet<OIdentifiable>();
    Query q = null;
    try {
      q = OLuceneIndexType.createFullQuery(index, key, indexWriter.getAnalyzer());
      return getResults(q);
    } catch (ParseException e) {
      throw new RuntimeException("Error parsing query ", e);
    }

  }

  @Override
  public void put(Object key, Object value) {
    Set<OIdentifiable> container = (Set<OIdentifiable>) value;
    for (OIdentifiable oIdentifiable : container) {
      Document doc = new Document();
      doc.add(OLuceneIndexType.createField(RID, oIdentifiable, oIdentifiable.getIdentity().toString(), Field.Store.YES,
          Field.Index.NOT_ANALYZED_NO_NORMS));
      for (String f : index.getFields()) {
        doc.add(OLuceneIndexType.createField(f, oIdentifiable, key, Field.Store.NO, Field.Index.ANALYZED));
      }
      addDocument(doc);

    }
  }

  private Set<OIdentifiable> getResults(Query query) {
    Set<OIdentifiable> results = new HashSet<OIdentifiable>();
    try {
      IndexSearcher searcher = getSearcher();

      TopDocs docs = searcher.search(query, 20);
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
      OIndexEngine.ValuesTransformer transformer, OIndexEngine.ValuesResultListener valuesResultListener) {

  }

  @Override
  public void getValuesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder, OIndexEngine.ValuesTransformer transformer,
      OIndexEngine.ValuesResultListener valuesResultListener) {

  }

  @Override
  public void getValuesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, OIndexEngine.ValuesTransformer transformer,
      OIndexEngine.ValuesResultListener valuesResultListener) {

  }

  @Override
  public void getEntriesMajor(Object fromKey, boolean isInclusive, OIndexEngine.ValuesTransformer transformer,
      OIndexEngine.EntriesResultListener entriesResultListener) {

  }

  @Override
  public void getEntriesMinor(Object toKey, boolean isInclusive, OIndexEngine.ValuesTransformer transformer,
      OIndexEngine.EntriesResultListener entriesResultListener) {

  }

  @Override
  public void getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, OIndexEngine.ValuesTransformer transformer,
      OIndexEngine.EntriesResultListener entriesResultListener) {

  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }
}
