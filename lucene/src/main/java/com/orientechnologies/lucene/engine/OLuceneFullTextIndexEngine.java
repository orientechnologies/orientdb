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

package com.orientechnologies.lucene.engine;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.OLuceneIndexType;
import com.orientechnologies.lucene.builder.DocBuilder;
import com.orientechnologies.lucene.builder.OQueryBuilder;
import com.orientechnologies.lucene.collections.LuceneResultSet;
import com.orientechnologies.lucene.collections.LuceneResultSetFactory;
import com.orientechnologies.lucene.collections.OFullTextCompositeKey;
import com.orientechnologies.lucene.query.QueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.sql.parser.ParseException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.*;

public class OLuceneFullTextIndexEngine extends OLuceneIndexEngineAbstract {

  protected OLuceneFacetManager facetManager;
  private   DocBuilder          builder;
  private   OQueryBuilder       queryBuilder;

  public OLuceneFullTextIndexEngine(String idxName, DocBuilder builder, OQueryBuilder queryBuilder) {
    super(idxName);
    this.builder = builder;
    this.queryBuilder = queryBuilder;
  }

  @Override
  public IndexWriter createIndexWriter(Directory directory) throws IOException {

    Analyzer analyzer = getAnalyzer(metadata);
    IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

    facetManager = new OLuceneFacetManager(this, metadata);

    OLogManager.instance().debug(this, "Creating Lucene index in '%s'...", directory);

    return new IndexWriter(directory, iwc);
  }

  @Override
  public IndexWriter openIndexWriter(Directory directory) throws IOException {
    Analyzer analyzer = getAnalyzer(metadata);
    IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

    // TODO: use writer config params to tune writer behaviour

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
    return getInTx(key, null);
  }

  @Override
  public Object getInTx(Object key, OLuceneTxChanges changes) {
    Query q = null;
    try {
      q = queryBuilder.query(index, key, mgrWriter.getIndexWriter().getAnalyzer());
      OCommandContext context = null;
      if (key instanceof OFullTextCompositeKey) {
        context = ((OFullTextCompositeKey) key).getContext();
      }
      return getResults(q, context, key, changes);
    } catch (ParseException e) {
      throw OException.wrapException(new OIndexEngineException("Error parsing lucene query"), e);
    }
  }

  @Override
  public void put(Object key, Object value) {
    Collection<OIdentifiable> container = (Collection<OIdentifiable>) value;
    for (OIdentifiable oIdentifiable : container) {
      Document doc = new Document();
      doc.add(OLuceneIndexType
                  .createField(RID, oIdentifiable.getIdentity().toString(), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
      int i = 0;
      if (index.isAutomatic()) {
        putInAutomaticIndex(key, doc, i);
      } else {
        putInManualindex(key, doc);
      }

      if (facetManager.supportsFacets()) {
        try {
          addDocument(facetManager.buildDocument(doc));
          // FIXME: writer is commited by timer task, why commit this separately?
          facetManager.commit();
        } catch (IOException e) {
          OLogManager.instance().error(this, "Error while updating facets", e);
        }
      } else {
        addDocument(doc);
      }

      if (!index.isAutomatic()) {
        commit();
      }
    }
  }

  private void putInManualindex(Object key, Document doc) {
    Object val = null;
    if (key instanceof OCompositeKey) {
      List<Object> keys = ((OCompositeKey) key).getKeys();

      int k = 0;
      for (Object o : keys) {
        doc.add(OLuceneIndexType.createField("k" + k, val, Field.Store.NO, Field.Index.ANALYZED));
      }
    } else if (key instanceof Collection) {
      Collection<Object> keys = (Collection<Object>) key;
      int k = 0;
      for (Object o : keys) {
        doc.add(OLuceneIndexType.createField("k" + k, o, Field.Store.NO, Field.Index.ANALYZED));
      }
    } else {
      val = key;
      doc.add(OLuceneIndexType.createField("k0", val, Field.Store.NO, Field.Index.ANALYZED));
    }
  }

  private void putInAutomaticIndex(Object key, Document doc, int i) {
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
  }

  private Set<OIdentifiable> getResults(Query query, OCommandContext context, Object key, OLuceneTxChanges changes) {

    try {
      IndexSearcher searcher = searcher();
      QueryContext queryContext = new QueryContext(context, searcher, query).setChanges(changes);
      if (facetManager.supportsFacets()) {
        facetManager.addFacetContext(queryContext, key);
      }
      return LuceneResultSetFactory.INSTANCE.create(this, queryContext);
    } catch (IOException e) {
      throw OIOException.wrapException(new OIndexException("Error reading from Lucene index"), e);
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
    return builder.build(index, key, value, collectionFields, metadata);
  }

  @Override
  public Query buildQuery(Object query) {

    try {
      return queryBuilder.query(index, query, queryAnalyzer());
    } catch (ParseException e) {

      throw OException.wrapException(new OIndexEngineException("Error parsing query"), e);
    }
  }

  @Override
  public void delete() {
    super.delete();
    if (facetManager != null) {
      facetManager.delete();
    }

  }

  public class LuceneIndexCursor implements OIndexCursor {

    private final Object          key;
    private       LuceneResultSet resultSet;

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
