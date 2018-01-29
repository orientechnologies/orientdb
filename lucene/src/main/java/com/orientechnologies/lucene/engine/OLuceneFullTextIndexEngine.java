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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.builder.OLuceneDocumentBuilder;
import com.orientechnologies.lucene.builder.OLuceneIndexType;
import com.orientechnologies.lucene.builder.OLuceneQueryBuilder;
import com.orientechnologies.lucene.collections.OLuceneCompositeKey;
import com.orientechnologies.lucene.collections.OLuceneIndexCursor;
import com.orientechnologies.lucene.collections.OLuceneResultSet;
import com.orientechnologies.lucene.query.OLuceneKeyAndMetadata;
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
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static com.orientechnologies.lucene.builder.OLuceneQueryBuilder.EMPTY_METADATA;

public class OLuceneFullTextIndexEngine extends OLuceneIndexEngineAbstract {

  private OLuceneDocumentBuilder builder;
  private OLuceneQueryBuilder    queryBuilder;

  public OLuceneFullTextIndexEngine(OStorage storage, String idxName) {
    super(storage, idxName);
    builder = new OLuceneDocumentBuilder();

  }

  @Override
  public void init(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata) {
    super.init(indexName, indexType, indexDefinition, isAutomatic, metadata);
    queryBuilder = new OLuceneQueryBuilder(metadata);
  }

  @Override
  public IndexWriter createIndexWriter(Directory directory) throws IOException {

    OLuceneIndexWriterFactory fc = new OLuceneIndexWriterFactory();

    OLogManager.instance().debug(this, "Creating Lucene index in '%s'...", directory);

    return fc.createIndexWriter(directory, metadata, indexAnalyzer());
  }

  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public void onRecordAddedToResultSet(OLuceneQueryContext queryContext, OContextualRecordId recordId, Document ret,
      final ScoreDoc score) {

    recordId.setContext(new HashMap<String, Object>() {{

      HashMap<String, TextFragment[]> frag = queryContext.getFragments();

      frag.entrySet().stream().forEach(f -> {
        TextFragment[] fragments = f.getValue();
        StringBuilder hlField = new StringBuilder();
        for (int j = 0; j < fragments.length; j++) {
          if ((fragments[j] != null) && (fragments[j].getScore() > 0)) {
            hlField.append(fragments[j].toString());
          }
        }
        put("$" + f.getKey() + "_hl", hlField.toString());
      });

      put("$score", score.score);
    }});
  }

  @Override
  public boolean contains(Object key) {
    return false;
  }

  @Override
  public boolean remove(Object key) {
    updateLastAccess();
    openIfClosed();
    try {
      Query query = new QueryParser("", queryAnalyzer()).parse((String) key);
      deleteDocument(query);
      return true;
    } catch (org.apache.lucene.queryparser.classic.ParseException e) {
      OLogManager.instance().error(this, "Lucene parsing exception", e);

    }
    return false;
  }

  @Override
  public Object get(Object key) {
    return getInTx(key, null);
  }

  @Override
  public void update(Object key, OIndexKeyUpdater<Object> updater) {
    put(key, updater.update(null).getValue());
  }

  @Override
  public void put(Object key, Object value) {

    updateLastAccess();
    openIfClosed();
    Collection<OIdentifiable> container = (Collection<OIdentifiable>) value;

    for (OIdentifiable oIdentifiable : container) {

      Document doc = buildDocument(key, oIdentifiable);

      addDocument(doc);

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

  private Set<OIdentifiable> getResults(Query query, OCommandContext context, OLuceneTxChanges changes, ODocument metadata) {

    //sort

    final List<SortField> fields = OLuceneIndexEngineUtils.buildSortFields(metadata);

    IndexSearcher searcher = searcher();

    OLuceneQueryContext queryContext = new OLuceneQueryContext(context, searcher, query, fields).withChanges(changes);

    return new OLuceneResultSet(this, queryContext, metadata);

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
    if (indexDefinition.isAutomatic()) {
//      builder.newBuild(index, key, value);

      return builder.build(indexDefinition, key, value, collectionFields, metadata);
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
  public Query buildQuery(Object maybeQuery) {
    try {
      if (maybeQuery instanceof String) {
        return queryBuilder.query(indexDefinition, maybeQuery, EMPTY_METADATA, queryAnalyzer());
      } else {
        OLuceneKeyAndMetadata q = (OLuceneKeyAndMetadata) maybeQuery;
        Query query = queryBuilder.query(indexDefinition, q.key, q.metadata, queryAnalyzer());
        return query;

      }
    } catch (ParseException e) {

      throw OException.wrapException(new OIndexEngineException("Error parsing query"), e);
    }
  }

  @Override
  public Set<OIdentifiable> getInTx(Object key, OLuceneTxChanges changes) {
    updateLastAccess();
    openIfClosed();
    try {
      if (key instanceof OLuceneKeyAndMetadata) {
        OLuceneKeyAndMetadata q = (OLuceneKeyAndMetadata) key;
        Query query = queryBuilder.query(indexDefinition, q.key, q.metadata, queryAnalyzer());

        OCommandContext commandContext = q.key.getContext();
        return getResults(query, commandContext, changes, q.metadata);

      } else {
        Query query = queryBuilder.query(indexDefinition, key, EMPTY_METADATA, queryAnalyzer());

        OCommandContext commandContext = null;
        if (key instanceof OLuceneCompositeKey) {
          commandContext = ((OLuceneCompositeKey) key).getContext();
        }
        return getResults(query, commandContext, changes, EMPTY_METADATA);
      }
    } catch (ParseException e) {
      throw OException.wrapException(new OIndexEngineException("Error parsing lucene query"), e);
    }
  }

}
