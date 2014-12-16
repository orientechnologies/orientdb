/*
 * Copyright 2014 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.lucene.manager;

import com.orientechnologies.lucene.OLuceneIndexType;
import com.orientechnologies.lucene.collections.OFullTextCompositeKey;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.lucene.analysis.Analyzer;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;

public class OLuceneFullTextIndexManager extends OLuceneIndexManagerAbstract {

  public OLuceneFullTextIndexManager() {
  }

  @Override
  public IndexWriter createIndexWriter(Directory directory, ODocument metadata) throws IOException {

    Analyzer analyzer = getAnalyzer(metadata);
    Version version = getVersion(metadata);
    IndexWriterConfig iwc = new IndexWriterConfig(version, analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
    return new IndexWriter(directory, iwc);
  }

  @Override
  public IndexWriter openIndexWriter(Directory directory, ODocument metadata) throws IOException {
    Analyzer analyzer = getAnalyzer(metadata);
    Version version = getVersion(metadata);
    IndexWriterConfig iwc = new IndexWriterConfig(version, analyzer);
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
  public Object get(Object key) {
    Query q = null;

    try {
      q = OLuceneIndexType.createFullQuery(index, key, mgrWriter.getIndexWriter().getAnalyzer(), getVersion(metadata));
      OCommandContext context = null;
      if (key instanceof OFullTextCompositeKey) {
        context = ((OFullTextCompositeKey) key).getContext();
      }
      return getResults(q, context);
    } catch (ParseException e) {
      throw new OIndexEngineException("Error parsing lucene query ", e);
    }
  }

  @Override
  public void put(Object key, Object value) {
    Set<OIdentifiable> container = (Set<OIdentifiable>) value;
    for (OIdentifiable oIdentifiable : container) {
      Document doc = new Document();
      doc.add(OLuceneIndexType.createField(RID, oIdentifiable, oIdentifiable.getIdentity().toString(), Field.Store.YES,
          Field.Index.NOT_ANALYZED_NO_NORMS));
      int i = 0;
      for (String f : index.getFields()) {
        Object val = null;
        if (key instanceof OCompositeKey) {
          val = ((OCompositeKey) key).getKeys().get(i);
          i++;
        } else {
          val = key;
        }
        if (val != null)
          doc.add(OLuceneIndexType.createField(f, oIdentifiable, val, Field.Store.NO, Field.Index.ANALYZED));
      }
      addDocument(doc);

    }
  }

  private Set<OIdentifiable> getResults(Query query, OCommandContext context) {

    try {
      IndexSearcher searcher = getSearcher();

      Integer limit = null;
      if (context != null) {
        limit = (Integer) context.getVariable("$limit");
      }

      final long startFetching = System.currentTimeMillis();
      final TopDocs docs = searcher.search(query, (limit != null && limit > 0) ? limit : Integer.MAX_VALUE);
      ScoreDoc[] hits = docs.scoreDocs;
      sendTotalHits(context, docs);
      Set<OIdentifiable> results = new LinkedHashSet<OIdentifiable>(hits.length);
      for (final ScoreDoc score : hits) {
        Document ret = searcher.doc(score.doc);
        String rId = ret.get(RID);
        results.add(new OContextualRecordId(rId).setContext(new HashMap<String, Object>() {
          {
            put("score", score.score);
          }
        }));

      }
      if (context != null) {
        sendLookupTime(context, docs, limit, startFetching);
      }
//      return new LuceneResultSet(searcher,docs);
      return results;
    } catch (IOException e) {
      throw new OIndexException("Error reading from Lucene index", e);
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
  public OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer) {
    return null;
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return null;
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return null;
  }

  @Override
  public OIndexCursor cursor(ValuesTransformer valuesTransformer) {
    return null;
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    return new OIndexKeyCursor() {
      @Override
      public Object next(int prefetchSize) {
        return null;
      }
    };
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }
}
