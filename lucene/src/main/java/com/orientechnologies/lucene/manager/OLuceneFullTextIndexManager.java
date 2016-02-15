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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.OLuceneIndexType;
import com.orientechnologies.lucene.collections.LuceneResultSet;
import com.orientechnologies.lucene.collections.OFullTextCompositeKey;
import com.orientechnologies.lucene.query.QueryContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
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
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.*;

public class OLuceneFullTextIndexManager extends OLuceneIndexManagerAbstract {

  protected OLuceneFacetManager facetManager;

  public OLuceneFullTextIndexManager() {
  }

  @Override
  public IndexWriter createIndexWriter(Directory directory, ODocument metadata) throws IOException {

    Analyzer analyzer = getAnalyzer(metadata);
    Version version = getLuceneVersion(metadata);
    IndexWriterConfig iwc = new IndexWriterConfig(version, analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

    facetManager = new OLuceneFacetManager(this, metadata);

    OLogManager.instance().debug(this, "Creating Lucene index in '%s'...", directory);

    return new IndexWriter(directory, iwc);
  }

  @Override
  public IndexWriter openIndexWriter(Directory directory, ODocument metadata) throws IOException {
    Analyzer analyzer = getAnalyzer(metadata);
    Version version = getLuceneVersion(metadata);
    IndexWriterConfig iwc = new IndexWriterConfig(version, analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

    OLogManager.instance().debug(this, "Opening Lucene index in '%s'...", directory);

    return new IndexWriter(directory, iwc);
  }

  @Override
  public void init() {

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
  public Object get(Object key) {
    Query q = null;
    try {
      q = OLuceneIndexType.createFullQuery(index, key, mgrWriter.getIndexWriter().getAnalyzer(), getLuceneVersion(metadata));
      OCommandContext context = null;
      if (key instanceof OFullTextCompositeKey) {
        context = ((OFullTextCompositeKey) key).getContext();
      }
      return getResults(q, context, key);
    } catch (ParseException e) {
      throw new OIndexEngineException("Error parsing lucene query ", e);
    }
  }

  @Override
  public void put(Object key, Object value) {
    Set<OIdentifiable> container = (Set<OIdentifiable>) value;
    for (OIdentifiable oIdentifiable : container) {
      Document doc = new Document();
      doc.add(OLuceneIndexType.createField(RID, oIdentifiable.getIdentity().toString(), Field.Store.YES,
          Field.Index.NOT_ANALYZED_NO_NORMS));
      int i = 0;
      if (index.isAutomatic()) {
        for (String f : index.getFields()) {

          Object val = null;
          if (key instanceof OCompositeKey) {
            val = ((OCompositeKey) key).getKeys().get(i);
            i++;
          } else {
            val = key;
          }
          if (val != null) {
            if (facetManager.supportsFacets() && facetManager.isFacetField(f)) {
              doc.add(facetManager.buildFacetField(f, val));
            } else {

              if (isToStore(f).equals(Field.Store.YES)) {
                doc.add(OLuceneIndexType.createField(f + STORED, val, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
              }
              doc.add(OLuceneIndexType.createField(f, val, Field.Store.NO, Field.Index.ANALYZED));
            }
          }

        }
      } else {

        Object val = null;
        if (key instanceof OCompositeKey) {
          List<Object> keys = ((OCompositeKey) key).getKeys();
          int k = 0;
          for (Object o : keys) {
            doc.add(OLuceneIndexType.createField("k" + k, o, Field.Store.NO, Field.Index.ANALYZED));
            k++;
          }
        } else if (key instanceof Collection) {
          Collection<Object> keys = (Collection<Object>) key;
          int k = 0;
          for (Object o : keys) {
            doc.add(OLuceneIndexType.createField("k" + k, o, Field.Store.NO, Field.Index.ANALYZED));
            k++;
          }
        } else {
          val = key;
          doc.add(OLuceneIndexType.createField("k0", val, Field.Store.NO, Field.Index.ANALYZED));
        }
      }
      if (facetManager.supportsFacets()) {
        try {
          addDocument(facetManager.buildDocument(doc));
        } catch (IOException e) {
          e.printStackTrace();
        }
      } else {
        addDocument(doc);
      }

      facetManager.commit();

      if (!index.isAutomatic()) {
        commit();
      }
    }
  }

  private Set<OIdentifiable> getResults(Query query, OCommandContext context, Object key) {

    try {
      IndexSearcher searcher = getSearcher();
      QueryContext queryContext = new QueryContext(context, searcher, query);
      if (facetManager.supportsFacets()) {
        facetManager.addFacetContext(queryContext, key);
      }
      return new LuceneResultSet(this, queryContext);
    } catch (IOException e) {
      throw new OIndexException("Error reading from Lucene index", e);
    }

  }

  @Override
  public void onRecordAddedToResultSet(QueryContext queryContext, OContextualRecordId recordId, Document ret,
      final ScoreDoc score) {
    recordId.setContext(new HashMap<String, Object>() {
      {
        put("score", score.score);
      }
    });
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
    return new LuceneIndexCursor((LuceneResultSet) get(rangeFrom), rangeFrom);
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer transformer) {
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

  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public Document buildDocument(Object key, OIdentifiable value) {
    return build(index, key, value, metadata);
  }

  @Override
  public Query buildQuery(Object query) throws ParseException {
    return OLuceneIndexType.createFullQuery(index, query, mgrWriter.getIndexWriter().getAnalyzer(), getLuceneVersion(metadata));

  }

  @Override
  public Analyzer analyzer(String field) {
    return getAnalyzer(metadata);
  }

  protected Document build(OIndexDefinition definition, Object key, OIdentifiable value, ODocument metadata) {
    Document doc = new Document();
    int i = 0;

    if (value != null) {
      doc.add(OLuceneIndexType.createField(OLuceneIndexManagerAbstract.RID, value.getIdentity().toString(), Field.Store.YES,
          Field.Index.NOT_ANALYZED_NO_NORMS));
    }
    List<Object> formattedKey = formatKeys(definition, key);
    for (String f : definition.getFields()) {
      Object val = formattedKey.get(i);
      i++;
      if (val != null)
        doc.add(OLuceneIndexType.createField(f, val, Field.Store.NO, Field.Index.ANALYZED));
    }
    return doc;
  }

  private List<Object> formatKeys(OIndexDefinition definition, Object key) {
    List<Object> keys;

    if (key instanceof OCompositeKey) {
      keys = ((OCompositeKey) key).getKeys();

    } else if (key instanceof List) {
      keys = ((List) key);
    } else {
      keys = new ArrayList<Object>();
      keys.add(key);
    }

    for (int i = keys.size(); i < definition.getFields().size(); i++) {
      keys.add("");
    }
    return keys;
  }

  @Override
  public void delete() {
    super.delete();
    facetManager.delete();
  }

  public class LuceneIndexCursor implements OIndexCursor {

    private final Object            key;
    private LuceneResultSet         resultSet;

    private Iterator<OIdentifiable> iterator;

    public LuceneIndexCursor(LuceneResultSet resultSet, Object key) {
      this.resultSet = resultSet;
      this.iterator = resultSet.iterator();
      this.key = key;
    }

    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {

      if (iterator.hasNext()) {
        final OIdentifiable next = iterator.next();
        return new Map.Entry<Object, OIdentifiable>() {
          @Override
          public Object getKey() {
            return key;
          }

          @Override
          public OIdentifiable getValue() {
            return next;
          }

          @Override
          public OIdentifiable setValue(OIdentifiable value) {
            return null;
          }
        };
      }
      return null;
    }

    @Override
    public Set<OIdentifiable> toValues() {
      return null;
    }

    @Override
    public Set<Map.Entry<Object, OIdentifiable>> toEntries() {
      return null;
    }

    @Override
    public Set<Object> toKeys() {
      return null;
    }

    @Override
    public void setPrefetchSize(int prefetchSize) {

    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public OIdentifiable next() {
      return null;
    }

    @Override
    public void remove() {

    }

  }

}
