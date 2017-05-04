/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
import com.orientechnologies.lucene.builder.OLuceneDocumentBuilder;
import com.orientechnologies.lucene.builder.OLuceneIndexType;
import com.orientechnologies.lucene.builder.OLuceneQueryBuilder;
import com.orientechnologies.lucene.collections.OLuceneCompositeKey;
import com.orientechnologies.lucene.collections.OLuceneIndexCursor;
import com.orientechnologies.lucene.collections.OLuceneResultSet;
import com.orientechnologies.lucene.collections.OLuceneResultSetFactory;
import com.orientechnologies.lucene.query.OLuceneQueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.ParseException;
import com.orientechnologies.orient.core.storage.OStorage;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class OLuceneFullTextIndexEngine extends OLuceneIndexEngineAbstract {

  protected OLuceneFacetManager    facetManager;
  private   OLuceneDocumentBuilder builder;
  private   OLuceneQueryBuilder    queryBuilder;

  public OLuceneFullTextIndexEngine(OStorage storage, String idxName) {
    super(storage, idxName);

  }

  @Override
  public IndexWriter openIndexWriter(Directory directory) throws IOException {
    return createIndexWriter(directory);
  }

  @Override
  public void init(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata) {
    super.init(indexName, indexType, indexDefinition, isAutomatic, metadata);
    queryBuilder = new OLuceneQueryBuilder(metadata);
    builder = new OLuceneDocumentBuilder();
  }

  @Override
  public IndexWriter createIndexWriter(Directory directory) throws IOException {

    OLuceneIndexWriterFactory fc = new OLuceneIndexWriterFactory();

    facetManager = new OLuceneFacetManager(storage, this, metadata);

    OLogManager.instance().debug(this, "Creating Lucene index in '%s'...", directory);

    return fc.createIndexWriter(directory, metadata, indexAnalyzer());
  }

  @Override
  public void delete() {
    super.delete();
    if (facetManager != null) {
      facetManager.delete();
    }

  }

  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public void onRecordAddedToResultSet(OLuceneQueryContext queryContext, OContextualRecordId recordId, Document ret,
      final ScoreDoc score) {
    recordId.setContext(new HashMap<String, Object>() {
      {
        put("$score", score.score);
      }
    });
  }

  @Override
  public boolean contains(Object key) {
    return false;
  }

  @Override
  public boolean remove(Object key) {

    try {
      Query query = new QueryParser("", queryAnalyzer()).parse((String) key);
      deleteDocument(query);
      return true;
    } catch (org.apache.lucene.queryparser.classic.ParseException e) {
      e.printStackTrace();

    }
    return false;
  }

  @Override
  public Object get(Object key) {
    return getInTx(key, null);
  }

  @Override
  public void put(Object key, Object value) {

    Collection<OIdentifiable> container = (Collection<OIdentifiable>) value;

    for (OIdentifiable oIdentifiable : container) {

      Document doc = buildDocument(key, oIdentifiable);

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
        flush();
      }
    }
  }

  @Override
  public boolean validatedPut(Object key, OIdentifiable value, Validator<Object, OIdentifiable> validator) {
    throw new UnsupportedOperationException("Validated put is not supported by OLuceneFullTextIndexEngine");
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
    return new OLuceneIndexCursor((OLuceneResultSet) get(rangeFrom), rangeFrom);
  }

  private Set<OIdentifiable> getResults(Query query, OCommandContext context, Object key, OLuceneTxChanges changes) {

    try {
      IndexSearcher searcher = searcher();
      OLuceneQueryContext queryContext = new OLuceneQueryContext(context, searcher, query).withChanges(changes);
      if (facetManager.supportsFacets()) {
        facetManager.addFacetContext(queryContext, key);
      }

      return OLuceneResultSetFactory.INSTANCE.create(this, queryContext);
    } catch (IOException e) {
      throw OIOException.wrapException(new OIndexException("Error reading from Lucene index"), e);
    }

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
  public boolean hasRangeQuerySupport() {
    return false;
  }

  @Override
  public Document buildDocument(Object key, OIdentifiable value) {
    if (index.isAutomatic()) {
//      builder.newBuild(index, key, value);

      return builder.build(index, key, value, collectionFields, metadata);
    } else {
      return putInManualindex(key, value);
    }
  }

  private Document putInManualindex(Object key, OIdentifiable oIdentifiable) {
    Document doc = new Document();
    doc.add(OLuceneIndexType.createField(RID, oIdentifiable.getIdentity().toString(), Field.Store.YES));

    if (key instanceof OCompositeKey) {

      List<Object> keys = ((OCompositeKey) key).getKeys();

      int k = 0;
      for (Object o : keys) {
        doc.add(OLuceneIndexType.createField("k" + k, o, Field.Store.YES));
        k++;
      }
    } else if (key instanceof Collection) {
      Collection<Object> keys = (Collection<Object>) key;

      int k = 0;
      for (Object o : keys) {
        doc.add(OLuceneIndexType.createField("k" + k, o, Field.Store.YES));
        k++;
      }
    } else {
      doc.add(OLuceneIndexType.createField("k0", key, Field.Store.NO));
    }
    return doc;
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
  public Set<OIdentifiable> getInTx(Object key, OLuceneTxChanges changes) {
    try {
      Query q = queryBuilder.query(index, key, queryAnalyzer());
      OCommandContext context = null;
      if (key instanceof OLuceneCompositeKey) {
        context = ((OLuceneCompositeKey) key).getContext();
      }
      return getResults(q, context, key, changes);
    } catch (ParseException e) {
      throw OException.wrapException(new OIndexEngineException("Error parsing lucene query"), e);
    }
  }

}
